package mpio

import (
	"context"
	"sync"
	"testing"
	"time"

	"southpole/mpio/internal/sim"
)

func TestCloseConcurrentWithResume(t *testing.T) {
	for i := 0; i < 100; i++ {
		cfg := sim.DefaultConfig()
		cfg.DiskCount = 1
		arr := sim.New(cfg)
		m := NewManager(arr, DefaultConfig())
		m.Start()

		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = m.Resume(ctx)
			}()
		}
		if i%2 == 0 {
			time.Sleep(time.Millisecond)
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
	}
}

func TestPlanChunksAlignsCompleteChunksToOptimalUnit(t *testing.T) {
	const opt = 64 << 10
	chunks := PlanChunks(4<<10, 256<<10, opt)
	if len(chunks) < 4 {
		t.Fatalf("expected multiple chunks, got %#v", chunks)
	}

	var cursor int64 = 4 << 10
	for i, c := range chunks {
		if c.Offset != cursor {
			t.Fatalf("chunk %d offset=%d, want %d", i, c.Offset, cursor)
		}
		if c.Aligned && (c.Offset%opt != 0 || c.Length != opt) {
			t.Fatalf("aligned chunk violates geometry: %+v", c)
		}
		if c.Length > opt {
			t.Fatalf("chunk %d too large: %d", i, c.Length)
		}
		cursor += int64(c.Length)
	}
	if cursor != 4<<10+256<<10 {
		t.Fatalf("chunks cover %d bytes, want %d", cursor-(4<<10), 256<<10)
	}

	full := PlanChunks(0, 4*opt, opt)
	if len(full) != 4 {
		t.Fatalf("aligned request should split into four optimal chunks, got %d", len(full))
	}
	for _, c := range full {
		if c.Length != opt || !c.Aligned {
			t.Fatalf("chunk is not optimal/aligned: %+v", c)
		}
	}
}

func TestBusyAndMediaErrorsDoNotTakePathDown(t *testing.T) {
	cfg := sim.DefaultConfig()
	cfg.DiskCount = 1
	cfg.DiskSize = 1 << 20
	arr := sim.New(cfg)

	mcfg := DefaultConfig()
	mcfg.ProbeInterval = 10 * time.Millisecond
	mcfg.ProbeTimeout = 30 * time.Millisecond
	mcfg.IOTimeout = 80 * time.Millisecond
	mcfg.BusyWait = time.Millisecond
	mcfg.MaxBusyWait = 4 * time.Millisecond
	m := NewManager(arr, mcfg)
	m.Start()
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}

	if err := arr.InjectBusy("disk-000", 4, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := arr.InjectMediaError("disk-000", 1); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	if err := m.executeChunk(context.Background(), 1, "disk-000", sim.Write, buf, 0, 0); err != nil {
		t.Fatalf("temporary array errors should be retried: %v", err)
	}

	snap := m.Snapshot()
	if snap.Summary.PathsDown != 0 {
		t.Fatalf("busy/media error marked a path down: %+v", snap.Summary)
	}
	if snap.Summary.TotalPathErrors != 0 {
		t.Fatalf("busy/media errors counted as path errors: %+v", snap.Summary)
	}
	if snap.Summary.TotalBusyRetries < 4 || snap.Summary.TotalMediaErrors < 1 {
		t.Fatalf("temporary errors were not recorded: busy=%d media=%d", snap.Summary.TotalBusyRetries, snap.Summary.TotalMediaErrors)
	}
}

func TestRapidOwnerChangesDoNotLeaveActiveSwitch(t *testing.T) {
	cfg := sim.DefaultConfig()
	cfg.DiskCount = 1
	arr := sim.New(cfg)
	mcfg := DefaultConfig()
	mcfg.ProbeInterval = time.Hour
	m := NewManager(arr, mcfg)
	m.Start()
	defer m.Close()

	for i := 0; i < 5; i++ {
		owner := i % 2
		if err := arr.SetOwner("disk-000", owner, 80*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.Snapshot().Summary.ActiveSwitches == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	snap := m.Snapshot()
	t.Fatalf("active switches remain: %+v", snap.Switches)
}

func TestArrayPathNotificationUsesNotificationReason(t *testing.T) {
	cfg := sim.DefaultConfig()
	cfg.DiskCount = 1
	arr := sim.New(cfg)
	mcfg := DefaultConfig()
	mcfg.ProbeInterval = time.Hour
	m := NewManager(arr, mcfg)
	m.Start()
	defer m.Close()

	pathID := arr.PathInfos()[0].ID
	if err := arr.NotifyPathState(pathID, StateDown, "test array notification"); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snap := m.Snapshot()
		for _, p := range snap.Paths {
			if p.ID == pathID && p.State == StateDown && p.LastReason == ReasonArrayNotify {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("array path notification was not applied with array_notification reason")
}
