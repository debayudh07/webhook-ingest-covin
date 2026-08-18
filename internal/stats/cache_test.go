package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

func TestCacheRecordIsSafeConcurrently(t *testing.T) {
	c := stats.NewCache()
	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			c.Record("acc", 1)
		}()
	}
	wg.Wait()

	got := c.Get("acc")
	if got.CallCount != n || got.TotalDurationSec != n {
		t.Fatalf("got %+v, want CallCount=%d TotalDurationSec=%d", got, n, n)
	}
}

func TestCacheReplaceAllHydratesFromSnapshot(t *testing.T) {
	c := stats.NewCache()
	c.ReplaceAll(map[string]stats.AccountStats{
		"acc": {CallCount: 7, TotalDurationSec: 99},
	})
	got := c.Get("acc")
	if got.CallCount != 7 || got.TotalDurationSec != 99 {
		t.Fatalf("got %+v, want CallCount=7 TotalDurationSec=99", got)
	}
}
