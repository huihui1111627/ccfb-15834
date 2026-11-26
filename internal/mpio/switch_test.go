package mpio

import (
	"bytes"
	"context"
	"testing"
	"time"

	"southpole/mpio/internal/sim"
)

func TestOneFailedSwitchLeavesWorkingPaths(t *testing.T) {
	for _, sw := range []string{"switch-A", "switch-B"} {
		t.Run(sw, func(t *testing.T) {
			cfg := sim.DefaultConfig()
			cfg.DiskCount = 1
			cfg.DiskSize = 1 << 20
			cfg.PathRTT = 20 * time.Microsecond
			cfg.NonOwnerPenalty = 40 * time.Microsecond
			cfg.Jitter = 20 * time.Microsecond
			arr := sim.New(cfg)

			mcfg := DefaultConfig()
			mcfg.ProbeInterval = 5 * time.Millisecond
			mcfg.ProbeTimeout = 20 * time.Millisecond
			mcfg.IOTimeout = 60 * time.Millisecond
			mcfg.RequestTimeout = 5 * time.Second
			m := NewManager(arr, mcfg)
			m.Start()
			defer m.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := m.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			cancel()

			dev, err := m.Device("disk-000")
			if err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("south-pole-multipath-0123456789"), 4096)[:64<<10]

			if _, err := dev.WriteAt(payload, 0); err != nil {
				t.Fatalf("initial write: %v", err)
			}
			if _, err := arr.SetSwitchFault(sw, "link_down"); err != nil {
				t.Fatal(err)
			}

			deadline := time.Now().Add(500 * time.Millisecond)
			for i := 0; time.Now().Before(deadline); i++ {
				off := int64((i % 8) * (64 << 10))
				writeCtx, writeCancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := dev.WriteAtContext(writeCtx, payload, off)
				writeCancel()
				if err != nil {
					t.Fatalf("IO %d should fail over after %s outage: %v", i, sw, err)
				}

				got := make([]byte, len(payload))
				readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err = dev.ReadAtContext(readCtx, got, off)
				readCancel()
				if err != nil {
					t.Fatalf("read %d after failover: %v", i, err)
				}
				if !bytes.Equal(payload, got) {
					t.Fatalf("read %d payload mismatch", i)
				}
			}

			snap := m.Snapshot()
			if snap.Summary.PathsUp < 2 {
				t.Fatalf("expected at least two surviving paths, summary=%+v", snap.Summary)
			}

			_, _ = arr.SetSwitchFault(sw, "up")
			recoverDeadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(recoverDeadline) {
				s := m.Snapshot().Summary
				if s.PathsUp == 4 && s.PathsDown == 0 {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatalf("paths did not recover after restoring %s", sw)
		})
	}
}
