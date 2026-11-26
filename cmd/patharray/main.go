package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	mpio "southpole/mpio/internal/mpio"
	"southpole/mpio/internal/sim"
	"southpole/mpio/internal/web"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "web/API listen address")
	disks := flag.Int("disks", 48, "number of simulated disks")
	seed := flag.Int64("seed", 20261127, "simulation random seed")
	autoFaults := flag.Bool("auto-faults", true, "periodically inject demo path/controller/array faults")
	flag.Parse()

	simCfg := sim.DefaultConfig()
	simCfg.Seed = *seed
	simCfg.DiskCount = *disks
	simCfg.DiskSize = 4 << 20
	arr := sim.New(simCfg)

	mgrCfg := mpio.DefaultConfig()
	mgrCfg.ProbeInterval = 200 * time.Millisecond
	mgrCfg.ProbeTimeout = 80 * time.Millisecond
	mgrCfg.IOTimeout = 300 * time.Millisecond
	mgrCfg.RequestTimeout = 15 * time.Second
	mgr := mpio.NewManager(arr, mgrCfg)
	mgr.Start()

	alerts, unsubscribeAlerts := mgr.SubscribeAlerts(32)
	defer unsubscribeAlerts()
	go func() {
		for alert := range alerts {
			fmt.Printf("[上层状态] %s: %s\n", alert.DiskID, alert.Message)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	devices := mgr.Devices()
	for i, dev := range devices {
		go runDiskTraffic(ctx, rand.New(rand.NewSource(*seed+int64(i)*97+3)), dev)
	}
	if *autoFaults {
		go runDemoFaults(ctx, rand.New(rand.NewSource(*seed^0xd3a0)), arr, devices)
	}

	server := &http.Server{
		Addr:              *addr,
		Handler:           web.New(mgr, arr).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		fmt.Printf("多路径阵列仪表盘: http://%s\n", *addr)
		fmt.Println("按 Ctrl+C 退出")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("HTTP server error: %v\n", err)
			stop()
		}
	}()

	<-ctx.Done()
	fmt.Println("\n正在关闭...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	_ = mgr.Close()
}

func runDiskTraffic(ctx context.Context, r *rand.Rand, dev *mpio.Device) {
	for ctx.Err() == nil {
		size := dev.Capacity()
		if size <= 64<<10 {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		var n int64
		switch r.Intn(10) {
		case 0, 1, 2, 3:
			n = int64(4 << (12 + r.Intn(4))) // 4K-32K
		case 4, 5, 6:
			n = 64 << 10
		default:
			n = int64((128 + r.Intn(384)) << 10)
		}
		if n > size {
			n = 64 << 10
		}
		off := r.Int63n((size-n)/(4<<10)+1) * (4 << 10)
		buf := make([]byte, n)
		_, _ = r.Read(buf)

		opCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		var err error
		if r.Intn(100) < 45 {
			_, err = dev.WriteAtContext(opCtx, buf, off)
		} else {
			_, err = dev.ReadAtContext(opCtx, buf, off)
		}
		cancel()
		if err != nil && ctx.Err() == nil {
			// The dashboard reports failover/error events; demo traffic keeps going.
			time.Sleep(20 * time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(2+r.Intn(18)) * time.Millisecond):
		}
	}
}

func runDemoFaults(ctx context.Context, r *rand.Rand, arr *sim.Array, devices []*mpio.Device) {
	paths := arr.PathInfos()
	ticker := time.NewTicker(4 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		switch r.Intn(100) {
		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19:
			p := paths[r.Intn(len(paths))]
			fault := "link_down"
			if r.Intn(3) == 0 {
				fault = "timeout"
			}
			_ = arr.SetPathFault(p.ID, fault)
			go func(id string, d time.Duration) {
				t := time.NewTimer(d)
				defer t.Stop()
				select {
				case <-ctx.Done():
				case <-t.C:
					_ = arr.RecoverPath(id)
				}
			}(p.ID, time.Duration(1200+r.Intn(2200))*time.Millisecond)
		case 20, 21, 22, 23, 24:
			sw := []string{"switch-A", "switch-B"}[r.Intn(2)]
			_, _ = arr.SetSwitchFault(sw, "link_down")
			go func() {
				t := time.NewTimer(2500 * time.Millisecond)
				defer t.Stop()
				select {
				case <-ctx.Done():
				case <-t.C:
					_, _ = arr.SetSwitchFault(sw, "up")
				}
			}()
		case 25, 26, 27, 28, 29, 30, 31, 32, 33, 34:
			dev := devices[r.Intn(len(devices))]
			_ = arr.SetOwner(dev.ID(), 1-dev.OwnerController(), 60*time.Millisecond)
		case 35, 36, 37, 38, 39:
			dev := devices[r.Intn(len(devices))]
			if r.Intn(2) == 0 {
				_ = arr.InjectBusy(dev.ID(), 2+r.Intn(3), 8*time.Millisecond)
			} else {
				_ = arr.InjectMediaError(dev.ID(), 1)
			}
		}
	}
}
