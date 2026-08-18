// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

const (
	recordingQueue   = "webhook-ingest:recording-jobs"
	recordingWorkers = 4
)

// Service ingests webhook deliveries.
type Service struct {
	store  *store.Store
	cache  *stats.Cache
	rdb    *redis.Client
	log    *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Start hydrates the stats cache, re-queues unfinished recording work, and
// runs background workers. It must be called before serving traffic.
func (s *Service) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	if err := s.hydrateCache(ctx); err != nil {
		s.log.Error("hydrate stats cache", "err", err)
	}
	if err := s.recoverRecordings(ctx); err != nil {
		s.log.Error("recover recordings", "err", err)
	}
	for i := 0; i < recordingWorkers; i++ {
		s.wg.Add(1)
		go s.runRecordingWorker(ctx)
	}
}

// Shutdown stops recording workers and waits for them to exit.
func (s *Service) Shutdown(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	inserted, err := s.store.IngestCallEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	if rec.RecordingURL != "" {
		if err := s.rdb.LPush(ctx, recordingQueue, rec.CallID).Err(); err != nil {
			s.log.Error("enqueue recording", "call_id", rec.CallID, "event_id", rec.EventID, "err", err)
		}
	}

	return nil
}

func (s *Service) hydrateCache(ctx context.Context) error {
	all, err := s.store.AllAccountStats(ctx)
	if err != nil {
		return err
	}
	snap := make(map[string]stats.AccountStats, len(all))
	for id, st := range all {
		snap[id] = stats.AccountStats{
			CallCount:        st.CallCount,
			TotalDurationSec: st.TotalDurationSec,
		}
	}
	s.cache.ReplaceAll(snap)
	return nil
}

func (s *Service) recoverRecordings(ctx context.Context) error {
	ids, err := s.store.UnprocessedCallIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.rdb.LPush(ctx, recordingQueue, id).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) runRecordingWorker(ctx context.Context) {
	defer s.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		res, err := s.rdb.BRPop(ctx, time.Second, recordingQueue).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			s.log.Error("recording queue", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		callID := res[1]
		workCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := s.processRecording(workCtx, callID); err != nil {
			s.log.Error("process recording", "call_id", callID, "err", err)
		}
		cancel()
	}
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, callID string) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, callID)
}
