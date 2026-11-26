package selftest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	mpio "southpole/mpio/internal/mpio"
	"southpole/mpio/internal/sim"
)

// Config controls the deterministic self-test.
type Config struct {
	Seed      int64
	Disks     int
	Duration  time.Duration
	RequestTO time.Duration
}

// DefaultConfig returns the CI/demo self-test parameters.
func DefaultConfig() Config {
	return Config{
		Seed:      20261127,
		Disks:     48,
		Duration:  7 * time.Second,
		RequestTO: 20 * time.Second,
	}
}

// Report is the machine- and human-readable self-test result.
type Report struct {
	Pass bool `json:"pass"`

	Seed       int64         `json:"seed"`
	DiskCount  int           `json:"disk_count"`
	Duration   time.Duration `json:"duration"`
	ElapsedMS  int64         `json:"elapsed_ms"`
	Operations int64         `json:"operations"`
	ReadOps    int64         `json:"read_ops"`
	WriteOps   int64         `json:"write_ops"`
	Bytes      int64         `json:"bytes"`

	VerifiedBlocks int64 `json:"verified_4k_blocks"`
	Mismatches     int64 `json:"mismatches"`
	IOErrors       int64 `json:"io_errors"`

	MaxRequestWaitMS   int64 `json:"max_request_wait_ms"`
	TotalRequestWaitMS int64 `json:"total_request_wait_ms"`
	RequestSamples     int64 `json:"request_samples"`

	Manager mpio.Snapshot `json:"manager"`

	EventReasons map[string]int `json:"event_reasons"`

	ExpandedDisks []ExpansionCheck `json:"expanded_disks"`
	SpaceDisk     string           `json:"space_disk"`
	SpacePushOK   bool             `json:"space_alert_push_ok"`
	ResumeOK      bool             `json:"resume_ok"`
	HourlyRows    int              `json:"hourly_rows"`
	CSVBytes      int              `json:"csv_bytes"`

	Failures []string `json:"failures"`

	reportMutexes
}

// ExpansionCheck records an online expansion/new-region write/read check.
type ExpansionCheck struct {
	DiskID   string `json:"disk_id"`
	OldSize  int64  `json:"old_size"`
	NewSize  int64  `json:"new_size"`
	WroteNew bool   `json:"wrote_new_region"`
	ReadNew  bool   `json:"read_new_region"`
}

type diskRef struct {
	mu   sync.RWMutex
	data []byte
}

// Run executes the deterministic fault and consistency scenario.
func Run(cfg Config) (*Report, *mpio.Manager, *sim.Array) {
	def := DefaultConfig()
	if cfg.Seed == 0 {
		cfg.Seed = def.Seed
	}
	if cfg.Disks <= 0 {
		cfg.Disks = def.Disks
	}
	if cfg.Duration <= 0 {
		cfg.Duration = def.Duration
	}
	if cfg.RequestTO <= 0 {
		cfg.RequestTO = def.RequestTO
	}

	started := time.Now()
	report := &Report{Seed: cfg.Seed, DiskCount: cfg.Disks, Duration: cfg.Duration, EventReasons: map[string]int{}}

	simCfg := sim.DefaultConfig()
	simCfg.Seed = cfg.Seed
	simCfg.DiskCount = cfg.Disks
	simCfg.DiskSize = 1 << 20
	simCfg.PathRTT = 80 * time.Microsecond
	simCfg.NonOwnerPenalty = 250 * time.Microsecond
	simCfg.Jitter = 180 * time.Microsecond
	simCfg.BlackholeHold = 2 * time.Second

	arr := sim.New(simCfg)
	mgrCfg := mpio.DefaultConfig()
	mgrCfg.ProbeInterval = 15 * time.Millisecond
	mgrCfg.ProbeTimeout = 40 * time.Millisecond
	mgrCfg.IOTimeout = 100 * time.Millisecond
	mgrCfg.RequestTimeout = cfg.RequestTO
	mgrCfg.BusyWait = 2 * time.Millisecond
	mgrCfg.MaxBusyWait = 20 * time.Millisecond
	mgrCfg.HistoryLimit = 5000
	mgrCfg.SwitchLimit = 1000
	mgrCfg.SuspensionLimit = 300
	mgr := mpio.NewManager(arr, mgrCfg)
	mgr.Start()

	readyCtx, cancelReady := context.WithTimeout(context.Background(), 2*time.Second)
	if err := mgr.WaitReady(readyCtx); err != nil {
		report.fail("initial path confirmation failed: %v", err)
	}
	cancelReady()

	alertCh, unsubscribeAlerts := mgr.SubscribeAlerts(16)
	defer unsubscribeAlerts()

	devices := mgr.Devices()
	if len(devices) != cfg.Disks {
		report.fail("device count = %d, want %d", len(devices), cfg.Disks)
	}

	refs := make([]*diskRef, len(devices))
	for i, dev := range devices {
		info, ok := arr.DiskInfo(dev.ID())
		if !ok {
			report.fail("missing sim disk %s", dev.ID())
			continue
		}
		data := make([]byte, info.Size)
		if err := arr.ReadDirect(dev.ID(), 0, data); err != nil {
			report.fail("read initial reference for %s: %v", dev.ID(), err)
		}
		refs[i] = &diskRef{data: data}
	}

	rootCtx, cancelRoot := context.WithTimeout(context.Background(), cfg.Duration+45*time.Second)
	defer cancelRoot()

	var abortOnce sync.Once
	abort := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		abortOnce.Do(func() {
			report.failuresMu.Lock()
			report.Failures = append(report.Failures, msg)
			report.failuresMu.Unlock()
			cancelRoot()
		})
	}

	var ops, readOps, writeOps, totalBytes, verifiedBlocks, mismatchCount, ioErrorCount atomic.Int64
	var maxWaitNS atomic.Int64
	var totalWaitNS atomic.Int64
	var samples atomic.Int64

	workerWG := sync.WaitGroup{}
	workDeadline := time.Now().Add(cfg.Duration)
	for i, dev := range devices {
		i, dev := i, dev
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			r := rand.New(rand.NewSource(cfg.Seed + int64(i)*104729 + 17))
			for time.Now().Before(workDeadline) && rootCtx.Err() == nil {
				size := int64(dev.Capacity())
				if size <= 0 {
					abort("device %s has zero capacity", dev.ID())
					return
				}
				n := chooseLength(r)
				if n > size {
					n = int64(simCfg.OptimalIOSize)
				}
				// 4 KiB alignment keeps the block-by-block oracle simple.
				blockSize := int64(simCfg.BlockSize)
				maxBlock := (size - n) / blockSize
				if maxBlock < 0 {
					n = blockSize
					maxBlock = (size - n) / blockSize
				}
				off := r.Int63n(maxBlock+1) * blockSize
				isWrite := r.Intn(100) < 45
				buf := make([]byte, n)

				refs[i].mu.Lock()
				if isWrite {
					_, _ = r.Read(buf)
				}
				start := time.Now()
				var err error
				if isWrite {
					_, err = dev.WriteAtContext(rootCtx, buf, off)
				} else {
					_, err = dev.ReadAtContext(rootCtx, buf, off)
				}
				wait := time.Since(start)
				refs[i].mu.Unlock()

				samples.Add(1)
				totalWaitNS.Add(int64(wait))
				updateMax(&maxWaitNS, int64(wait))

				if err != nil {
					if rootCtx.Err() != nil {
						return
					}
					ioErrorCount.Add(1)
					abort("%s IO at off=%d len=%d failed: %v", dev.ID(), off, n, err)
					return
				}

				ops.Add(1)
				totalBytes.Add(n)
				if isWrite {
					writeOps.Add(1)
					refs[i].mu.Lock()
					copy(refs[i].data[off:off+n], buf)
					refs[i].mu.Unlock()
				} else {
					refs[i].mu.RLock()
					want := append([]byte(nil), refs[i].data[off:off+n]...)
					refs[i].mu.RUnlock()
					if !bytes.Equal(want, buf) {
						block := firstDifferent4KBlock(want, buf, off, simCfg.BlockSize)
						mismatchCount.Add(1)
						abort("consistency mismatch on %s at disk offset block %d (io off=%d len=%d)", dev.ID(), block, off, n)
						return
					}
					verifiedBlocks.Add((n + int64(simCfg.BlockSize) - 1) / int64(simCfg.BlockSize))
					readOps.Add(1)
				}
			}
		}()
	}

	// Deterministically chosen online-expansion disks.
	expandIndexes := uniqueIndexes(cfg.Disks, 3)
	expChecks := make([]ExpansionCheck, 0, len(expandIndexes))
	for _, idx := range expandIndexes {
		oldSize := refs[idx].dataLen()
		expChecks = append(expChecks, ExpansionCheck{DiskID: devices[idx].ID(), OldSize: oldSize})
	}

	spaceIdx := cfg.Disks - 1
	spaceDisk := devices[spaceIdx].ID()

	faultWG := sync.WaitGroup{}
	faultWG.Add(1)
	go func() {
		defer faultWG.Done()
		r := rand.New(rand.NewSource(cfg.Seed ^ 0x51f15e))
		deadline := time.Now().Add(cfg.Duration)
		expanded := map[int]bool{}
		resumed := false

		// Give the workload a brief clean start.
		sleepCtx(rootCtx, 120*time.Millisecond)

		for time.Now().Before(deadline) && rootCtx.Err() == nil {
			sleepCtx(rootCtx, time.Duration(8+r.Intn(35))*time.Millisecond)
			action := r.Intn(100)
			switch {
			case action < 28:
				pid := arr.PathInfos()[r.Intn(4)].ID
				_ = arr.SetPathFault(pid, "link_down")
				sleepCtx(rootCtx, time.Duration(25+r.Intn(85))*time.Millisecond)
				_ = arr.RecoverPath(pid)
			case action < 43:
				pid := arr.PathInfos()[r.Intn(4)].ID
				_ = arr.SetPathFault(pid, "timeout")
				sleepCtx(rootCtx, time.Duration(70+r.Intn(130))*time.Millisecond)
				_ = arr.RecoverPath(pid)
			case action < 62:
				sw := []string{"switch-A", "switch-B"}[r.Intn(2)]
				_, _ = arr.SetSwitchFault(sw, "link_down")
				sleepCtx(rootCtx, time.Duration(45+r.Intn(100))*time.Millisecond)
				_, _ = arr.SetSwitchFault(sw, "up")
			case action < 70:
				pid := arr.PathInfos()[r.Intn(4)].ID
				_ = arr.InjectLinkError(pid, 1+r.Intn(3))
			case action < 82:
				di := arr.DiskInfos()[r.Intn(cfg.Disks)]
				newOwner := 1 - di.OwnerController
				_ = arr.SetOwner(di.ID, newOwner, time.Duration(15+r.Intn(45))*time.Millisecond)
			case action < 94:
				di := arr.DiskInfos()[r.Intn(cfg.Disks)]
				if r.Intn(2) == 0 {
					_ = arr.InjectBusy(di.ID, 1+r.Intn(4), time.Duration(2+r.Intn(9))*time.Millisecond)
				} else {
					_ = arr.InjectMediaError(di.ID, 1+r.Intn(2))
				}
			default:
				idx := expandIndexes[r.Intn(len(expandIndexes))]
				if !expanded[idx] {
					expanded[idx] = true
					extra := int64(512 << 10)
					info, err := arr.ExpandDisk(devices[idx].ID(), extra)
					if err == nil {
						refs[idx].mu.Lock()
						refs[idx].data = append(refs[idx].data, make([]byte, extra)...)
						refs[idx].mu.Unlock()
						waitCapacity(devices[idx], info.Size, time.Second)
					}
				}
			}

			if !resumed && time.Now().After(started.Add(cfg.Duration/3)) {
				resumed = true
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				err := mgr.Resume(ctx)
				cancel()
				report.resumeMu.Lock()
				report.ResumeOK = err == nil
				report.resumeMu.Unlock()
			}

			// Ensure the designated space warning is active for the assertion window.
			if action == 999 {
				_, _ = arr.SetSpacePressure(spaceDisk, 96)
			}
		}

		// Make expansion coverage deterministic even if the random branch did not fire.
		for j, idx := range expandIndexes {
			if !expanded[idx] {
				expanded[idx] = true
				extra := int64(512 << 10)
				info, err := arr.ExpandDisk(devices[idx].ID(), extra)
				if err == nil {
					refs[idx].mu.Lock()
					refs[idx].data = append(refs[idx].data, make([]byte, extra)...)
					refs[idx].mu.Unlock()
					waitCapacity(devices[idx], info.Size, 2*time.Second)
				}
				_ = j
			}
		}
		_, _ = arr.SetSpacePressure(spaceDisk, 96)
	}()

	workerWG.Wait()
	faultWG.Wait()

	select {
	case alert := <-alertCh:
		report.SpacePushOK = alert.DiskID == spaceDisk && alert.Kind == "space_low"
	case <-time.After(time.Second):
		report.SpacePushOK = false
	}

	// Restore every simulated path and wait for the manager to converge.
	for _, p := range arr.PathInfos() {
		_ = arr.RecoverPath(p.ID)
	}
	_, _ = arr.SetSwitchFault("switch-A", "up")
	_, _ = arr.SetSwitchFault("switch-B", "up")
	waitAllPathsUp(mgr, 4, 4*time.Second)

	// Explicitly write and read the newly expanded region without recreating the device.
	for j := range expChecks {
		check := &expChecks[j]
		idx := expandIndexes[j]
		dev := devices[idx]
		info, ok := arr.DiskInfo(check.DiskID)
		if !ok {
			abort("expanded disk missing %s", check.DiskID)
			continue
		}
		check.NewSize = info.Size
		if !waitCapacity(dev, info.Size, 2*time.Second) {
			abort("online expansion not visible for %s: manager=%d array=%d", check.DiskID, dev.Capacity(), info.Size)
			continue
		}
		newOff := check.OldSize
		if newOff+128<<10 > info.Size {
			newOff = info.Size - 128<<10
		}
		pattern := bytes.Repeat([]byte{byte(0x40 + j), 0xA5, 0x5A, 0xC3}, (128<<10)/4)
		ctx, cancel := context.WithTimeout(context.Background(), cfg.RequestTO)
		_, werr := dev.WriteAtContext(ctx, pattern, newOff)
		cancel()
		if werr != nil {
			abort("write expanded region %s: %v", check.DiskID, werr)
			continue
		}
		refs[idx].mu.Lock()
		copy(refs[idx].data[newOff:newOff+int64(len(pattern))], pattern)
		refs[idx].mu.Unlock()
		check.WroteNew = true

		got := make([]byte, len(pattern))
		ctx2, cancel2 := context.WithTimeout(context.Background(), cfg.RequestTO)
		_, rerr := dev.ReadAtContext(ctx2, got, newOff)
		cancel2()
		if rerr != nil {
			abort("read expanded region %s: %v", check.DiskID, rerr)
			continue
		}
		if !bytes.Equal(pattern, got) {
			abort("expanded region mismatch on %s", check.DiskID)
			continue
		}
		check.ReadNew = true
	}
	report.ExpandedDisks = expChecks

	// Full block-by-block scan against the memory oracle.
	for i, dev := range devices {
		size := dev.Capacity()
		buf := make([]byte, size)
		ctx, cancel := context.WithTimeout(context.Background(), cfg.RequestTO)
		_, err := dev.ReadAtContext(ctx, buf, 0)
		cancel()
		if err != nil {
			abort("final scan %s: %v", dev.ID(), err)
			continue
		}
		refs[i].mu.RLock()
		want := append([]byte(nil), refs[i].data...)
		refs[i].mu.RUnlock()
		if !bytes.Equal(want, buf) {
			block := firstDifferent4KBlock(want, buf, 0, simCfg.BlockSize)
			abort("final full scan mismatch on %s at 4k block %d", dev.ID(), block)
			continue
		}
		verifiedBlocks.Add(size / int64(simCfg.BlockSize))
	}

	snap := mgr.Snapshot()
	report.Manager = snap
	report.Operations = ops.Load()
	report.ReadOps = readOps.Load()
	report.WriteOps = writeOps.Load()
	report.Bytes = totalBytes.Load()
	report.VerifiedBlocks = verifiedBlocks.Load()
	report.Mismatches = mismatchCount.Load()
	report.MaxRequestWaitMS = maxWaitNS.Load() / int64(time.Millisecond)
	report.TotalRequestWaitMS = totalWaitNS.Load() / int64(time.Millisecond)
	report.RequestSamples = samples.Load()
	report.IOErrors = ioErrorCount.Load()
	report.ElapsedMS = time.Since(started).Milliseconds()
	report.SpaceDisk = spaceDisk

	for _, ev := range snap.Events {
		if ev.Reason != "" {
			report.EventReasons[ev.Reason]++
		}
	}
	report.HourlyRows = len(snap.Hourly)
	csvData, _ := mgr.ExportHourlyCSV("")
	report.CSVBytes = len(csvData)

	validateReport(report, arr, mgr, spaceDisk)
	report.Pass = len(report.Failures) == 0
	return report, mgr, arr
}

type reportMutexes struct {
	failuresMu sync.Mutex
	resumeMu   sync.Mutex
}

// Embedded named mutex groups keep Report JSON clean-ish while allowing synchronized writes.
// They are unexported but promoted methods below are used in this package.

func (r *Report) fail(format string, args ...any) {
	r.failuresMu.Lock()
	defer r.failuresMu.Unlock()
	r.Failures = append(r.Failures, fmt.Sprintf(format, args...))
}

func (d *diskRef) dataLen() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return int64(len(d.data))
}

func chooseLength(r *rand.Rand) int64 {
	switch r.Intn(100) {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19:
		return int64(4 << 10)
	case 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34:
		return int64((8 + r.Intn(24)) << 10) // 8-32 KiB
	case 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54:
		return 64 << 10
	default:
		return int64((96 + r.Intn(416)) << 10) // 96-512 KiB
	}
}

func updateMax(max *atomic.Int64, v int64) {
	for {
		old := max.Load()
		if v <= old || max.CompareAndSwap(old, v) {
			return
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func waitCapacity(dev *mpio.Device, want int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dev.Capacity() == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return dev.Capacity() == want
}

func waitAllPathsUp(mgr *mpio.Manager, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s := mgr.Snapshot().Summary
		if s.PathsUp == want && s.PathsDown == 0 && s.PathsChecking == 0 {
			return true
		}
		time.Sleep(15 * time.Millisecond)
	}
	return false
}

func uniqueIndexes(n, count int) []int {
	if count > n {
		count = n
	}
	base := []int{0, n / 3, 2 * n / 3}
	out := make([]int, 0, count)
	seen := map[int]bool{}
	for _, v := range base {
		if v >= 0 && v < n && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	for i := 0; len(out) < count; i++ {
		v := (n - 1 - i) % n
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func firstDifferent4KBlock(want, got []byte, baseOffset int64, blockSize int) int64 {
	n := len(want)
	if len(got) < n {
		n = len(got)
	}
	for off := 0; off < n; off += blockSize {
		end := off + blockSize
		if end > n {
			end = n
		}
		if !bytes.Equal(want[off:end], got[off:end]) {
			return (baseOffset + int64(off)) / int64(blockSize)
		}
	}
	return -1
}

func validateReport(report *Report, arr *sim.Array, mgr *mpio.Manager, spaceDisk string) {
	snap := mgr.Snapshot()
	if report.Operations < 100 {
		report.fail("too few operations: %d", report.Operations)
	}
	if report.Mismatches != 0 {
		report.fail("had %d block mismatches", report.Mismatches)
	}
	if snap.Summary.PathsUp != 4 {
		report.fail("not all four paths recovered: up=%d down=%d checking=%d", snap.Summary.PathsUp, snap.Summary.PathsDown, snap.Summary.PathsChecking)
	}
	if report.EventReasons[mpio.ReasonLinkError] == 0 {
		report.fail("did not observe a link_error state transition")
	}
	if report.EventReasons[mpio.ReasonProbeTimeout] == 0 && report.EventReasons[mpio.ReasonIOTimeout] == 0 {
		report.fail("did not observe a timeout/probe-timeout transition")
	}
	if report.EventReasons[mpio.ReasonArrayNotify] == 0 {
		report.fail("did not observe array_notification events")
	}
	if report.EventReasons[mpio.ReasonHostResume] == 0 || !report.ResumeOK {
		report.fail("host resume reconfirmation was not observed/ok")
	}
	if snap.Summary.TotalSuspensions == 0 {
		report.fail("no requests were suspended during failover")
	}
	if snap.Summary.MaxSuspensionWaitMS > 10_000 {
		report.fail("suspension wait too high: %dms", snap.Summary.MaxSuspensionWaitMS)
	}
	if report.MaxRequestWaitMS > 20_000 {
		report.fail("end-to-end request wait too high: %dms", report.MaxRequestWaitMS)
	}
	if report.HourlyRows == 0 || report.CSVBytes == 0 {
		report.fail("hourly statistics/CSV export are empty")
	}

	alertOK := false
	for _, d := range snap.Disks {
		info, _ := arr.DiskInfo(d.ID)
		if d.Owner != info.OwnerController {
			report.fail("owner not followed for %s: manager=%d array=%d", d.ID, d.Owner, info.OwnerController)
		}
		if d.Size != info.Size {
			report.fail("size not followed for %s: manager=%d array=%d", d.ID, d.Size, info.Size)
		}
		if d.ID == spaceDisk && d.SpaceAlert {
			alertOK = true
		}
		for pid, pri := range d.PathPriorities {
			want := 50
			for _, p := range snap.Paths {
				if p.ID == pid && p.Controller == d.Owner {
					want = 100
				}
			}
			if pri != want {
				report.fail("bad priority for %s/%s: got %d want %d", d.ID, pid, pri, want)
			}
		}
	}
	if !alertOK {
		report.fail("space warning was not propagated for %s", spaceDisk)
	}
	if !report.SpacePushOK {
		report.fail("upper-layer alert subscription did not receive the space warning")
	}
	for _, ec := range report.ExpandedDisks {
		if !ec.WroteNew || !ec.ReadNew {
			report.fail("expansion check incomplete for %s", ec.DiskID)
		}
	}
}

// MarshalJSON renders the report as indented JSON.
func (r *Report) MarshalJSONPretty() ([]byte, error) {
	type alias Report
	return json.MarshalIndent((*alias)(r), "", "  ")
}
