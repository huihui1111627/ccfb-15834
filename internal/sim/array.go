package sim

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// IOp is the kind of a simulated data operation.
type IOp string

const (
	Read  IOp = "read"
	Write IOp = "write"
)

// FaultKind classifies a simulated array/path error.
type FaultKind string

const (
	FaultLink    FaultKind = "link_error"
	FaultTimeout FaultKind = "probe_timeout"
	FaultBusy    FaultKind = "busy"
	FaultMedia   FaultKind = "media_error"
)

// Error is an error reported by the simulated array or path.
type Error struct {
	Kind       FaultKind     `json:"kind"`
	PathID     string        `json:"path_id,omitempty"`
	DiskID     string        `json:"disk_id,omitempty"`
	Message    string        `json:"message"`
	RetryAfter time.Duration `json:"retry_after_ms,omitempty"`
	Temporary  bool          `json:"temporary"`
}

func (e *Error) Error() string {
	if e.DiskID != "" && e.PathID != "" {
		return fmt.Sprintf("%s on disk %s via %s: %s", e.Kind, e.DiskID, e.PathID, e.Message)
	}
	if e.DiskID != "" {
		return fmt.Sprintf("%s on disk %s: %s", e.Kind, e.DiskID, e.Message)
	}
	if e.PathID != "" {
		return fmt.Sprintf("%s on path %s: %s", e.Kind, e.PathID, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Message)
}

// IsPathFault reports whether the error indicates path/link failure.
func IsPathFault(err error) bool {
	var se *Error
	if errors.As(err, &se) {
		return se.Kind == FaultLink || se.Kind == FaultTimeout
	}
	return false
}

// IsTemporaryArrayError reports busy or correctable media errors.
func IsTemporaryArrayError(err error) bool {
	var se *Error
	if errors.As(err, &se) {
		return se.Temporary || se.Kind == FaultBusy
	}
	return false
}

// PathInfo describes one fixed server-to-controller path.
type PathInfo struct {
	ID         string `json:"id"`
	NIC        string `json:"nic"`
	Switch     string `json:"switch"`
	Controller int    `json:"controller"`
}

// DiskInfo is the array-reported state of a simulated disk/LUN.
type DiskInfo struct {
	ID               string    `json:"id"`
	Index            int       `json:"index"`
	Size             int64     `json:"size"`
	BlockSize        int       `json:"block_size"`
	OptimalIOSize    int       `json:"optimal_io_size"`
	OwnerController  int       `json:"owner_controller"`
	OwnerStable      bool      `json:"owner_stable"`
	UsedBytes        int64     `json:"used_bytes"`
	FreeBytes        int64     `json:"free_bytes"`
	UsedPercent      int       `json:"used_percent"`
	SpaceAlert       bool      `json:"space_alert"`
	SpaceThreshold   int       `json:"space_threshold_percent"`
	OwnerStableUntil time.Time `json:"-"`
}

// Event is an active notification emitted by the simulated array.
type Event struct {
	Time         time.Time     `json:"time"`
	Type         string        `json:"type"`
	DiskID       string        `json:"disk_id,omitempty"`
	PathID       string        `json:"path_id,omitempty"`
	State        string        `json:"state,omitempty"`
	Controller   int           `json:"controller,omitempty"`
	Size         int64         `json:"size,omitempty"`
	UsedPercent  int           `json:"used_percent,omitempty"`
	Transition   time.Duration `json:"transition_ms,omitempty"`
	Reason       string        `json:"reason"`
	Message      string        `json:"message"`
	Notification bool          `json:"notification"`
}

// Config controls the simulated array.
type Config struct {
	Seed            int64
	DiskCount       int
	DiskSize        int64
	BlockSize       int
	OptimalIOSize   int
	PathRTT         time.Duration
	NonOwnerPenalty time.Duration
	Jitter          time.Duration
	BlackholeHold   time.Duration
	SpaceThreshold  int
}

// DefaultConfig returns small, fast, deterministic-friendly defaults.
func DefaultConfig() Config {
	return Config{
		Seed:            20261127,
		DiskCount:       48,
		DiskSize:        2 << 20, // 2 MiB
		BlockSize:       4 << 10,
		OptimalIOSize:   64 << 10,
		PathRTT:         300 * time.Microsecond,
		NonOwnerPenalty: 1200 * time.Microsecond,
		Jitter:          400 * time.Microsecond,
		BlackholeHold:   2 * time.Second,
		SpaceThreshold:  90,
	}
}

// IORequest is one chunk sent through one path.
type IORequest struct {
	PathID string
	DiskID string
	Op     IOp
	Offset int64
	Data   []byte
}

type disk struct {
	info DiskInfo
	data []byte

	busyRemaining  int
	busyRetryAfter time.Duration
	mediaRemaining int
}

type pathState struct {
	info PathInfo
	up   bool
	// blackhole keeps the link looking present but drops probes/IO until timeout.
	blackhole bool

	linkErrorsRemaining int
}

// Array simulates two controllers, their disks and four network paths.
type Array struct {
	cfg Config

	mu    sync.RWMutex
	disks map[string]*disk
	paths map[string]*pathState
	order []string

	subMu sync.Mutex
	subs  map[int]chan Event
	next  int

	rngMu sync.Mutex
	rng   *rand.Rand
}

// New creates and initializes the simulated array.
func New(cfg Config) *Array {
	if cfg.DiskCount <= 0 {
		cfg.DiskCount = 48
	}
	if cfg.DiskSize <= 0 {
		cfg.DiskSize = 2 << 20
	}
	if cfg.BlockSize <= 0 {
		cfg.BlockSize = 4 << 10
	}
	if cfg.OptimalIOSize <= 0 {
		cfg.OptimalIOSize = 64 << 10
	}
	if cfg.PathRTT <= 0 {
		cfg.PathRTT = 300 * time.Microsecond
	}
	if cfg.NonOwnerPenalty <= 0 {
		cfg.NonOwnerPenalty = time.Millisecond
	}
	if cfg.BlackholeHold <= 0 {
		cfg.BlackholeHold = 2 * time.Second
	}
	if cfg.SpaceThreshold <= 0 {
		cfg.SpaceThreshold = 90
	}

	a := &Array{
		cfg:   cfg,
		disks: make(map[string]*disk, cfg.DiskCount),
		paths: make(map[string]*pathState, 4),
		subs:  make(map[int]chan Event),
		rng:   rand.New(rand.NewSource(cfg.Seed)),
	}

	pathDefs := []PathInfo{
		{ID: "p0-nic0-swA-ctl0", NIC: "nic-0", Switch: "switch-A", Controller: 0},
		{ID: "p1-nic0-swA-ctl1", NIC: "nic-0", Switch: "switch-A", Controller: 1},
		{ID: "p2-nic1-swB-ctl0", NIC: "nic-1", Switch: "switch-B", Controller: 0},
		{ID: "p3-nic1-swB-ctl1", NIC: "nic-1", Switch: "switch-B", Controller: 1},
	}
	for _, p := range pathDefs {
		a.paths[p.ID] = &pathState{info: p, up: true}
		a.order = append(a.order, p.ID)
	}

	for i := 0; i < cfg.DiskCount; i++ {
		id := fmt.Sprintf("disk-%03d", i)
		data := make([]byte, cfg.DiskSize)
		// A deterministic nonzero pattern makes direct manual demos readable.
		r := rand.New(rand.NewSource(cfg.Seed + int64(i)*7919))
		_, _ = r.Read(data)
		used := cfg.DiskSize * 35 / 100
		a.disks[id] = &disk{
			data: data,
			info: DiskInfo{
				ID:              id,
				Index:           i,
				Size:            cfg.DiskSize,
				BlockSize:       cfg.BlockSize,
				OptimalIOSize:   cfg.OptimalIOSize,
				OwnerController: i % 2,
				OwnerStable:     true,
				UsedBytes:       used,
				FreeBytes:       cfg.DiskSize - used,
				UsedPercent:     35,
				SpaceThreshold:  cfg.SpaceThreshold,
			},
		}
	}
	return a
}

// Subscribe receives array notifications.
func (a *Array) Subscribe(buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 256
	}
	ch := make(chan Event, buffer)
	a.subMu.Lock()
	id := a.next
	a.next++
	a.subs[id] = ch
	a.subMu.Unlock()
	return ch, func() {
		a.subMu.Lock()
		delete(a.subs, id)
		a.subMu.Unlock()
	}
}

func (a *Array) emit(e Event) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	e.Notification = true
	a.subMu.Lock()
	for _, ch := range a.subs {
		select {
		case ch <- e:
		default:
			// Slow observers must never change simulation behavior.
		}
	}
	a.subMu.Unlock()
}

// PathInfos returns the four simulated paths.
func (a *Array) PathInfos() []PathInfo {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]PathInfo, 0, len(a.order))
	for _, id := range a.order {
		out = append(out, a.paths[id].info)
	}
	return out
}

// DiskInfos returns all disk information.
func (a *Array) DiskInfos() []DiskInfo {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]DiskInfo, 0, len(a.disks))
	for _, d := range a.disks {
		info := d.info
		info.OwnerStable = time.Now().After(info.OwnerStableUntil)
		out = append(out, info)
	}
	return out
}

// DiskInfo returns one disk snapshot.
func (a *Array) DiskInfo(id string) (DiskInfo, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	d, ok := a.disks[id]
	if !ok {
		return DiskInfo{}, false
	}
	info := d.info
	info.OwnerStable = time.Now().After(info.OwnerStableUntil)
	return info, true
}

// OptimalIOSize reports the array's preferred split unit.
func (a *Array) OptimalIOSize() int { return a.cfg.OptimalIOSize }

// BlockSize reports the simulated disk logical block size.
func (a *Array) BlockSize() int { return a.cfg.BlockSize }

func (a *Array) jitter() time.Duration {
	if a.cfg.Jitter <= 0 {
		return 0
	}
	a.rngMu.Lock()
	n := a.rng.Int63n(int64(a.cfg.Jitter) + 1)
	a.rngMu.Unlock()
	return time.Duration(n)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Probe checks one path. A black-holed path returns a timeout when ctx expires.
func (a *Array) Probe(ctx context.Context, pathID string) error {
	a.mu.RLock()
	p, ok := a.paths[pathID]
	if !ok {
		a.mu.RUnlock()
		return &Error{Kind: FaultLink, PathID: pathID, Message: "unknown path"}
	}
	info := p.info
	up := p.up
	blackhole := p.blackhole
	rtt := a.cfg.PathRTT + a.jitter()
	a.mu.RUnlock()

	if !up {
		return &Error{Kind: FaultLink, PathID: pathID, Message: "link down"}
	}
	if blackhole {
		if err := sleepContext(ctx, a.cfg.BlackholeHold); err != nil {
			return &Error{Kind: FaultTimeout, PathID: pathID, Message: "probe timed out", Temporary: true}
		}
		return &Error{Kind: FaultTimeout, PathID: pathID, Message: "probe timed out"}
	}
	if err := sleepContext(ctx, rtt); err != nil {
		return &Error{Kind: FaultTimeout, PathID: pathID, Message: "probe timed out", Temporary: true}
	}
	_ = info
	return nil
}

// DoIO executes one already-split chunk through one path.
func (a *Array) DoIO(ctx context.Context, req IORequest) error {
	if req.PathID == "" || req.DiskID == "" {
		return errors.New("sim: path and disk are required")
	}
	if req.Offset < 0 || len(req.Data) == 0 {
		return errors.New("sim: invalid IO range")
	}

	// Consume one-shot injected faults with a writer lock.
	a.mu.Lock()
	p, ok := a.paths[req.PathID]
	if !ok {
		a.mu.Unlock()
		return &Error{Kind: FaultLink, PathID: req.PathID, DiskID: req.DiskID, Message: "unknown path"}
	}
	d, ok := a.disks[req.DiskID]
	if !ok {
		a.mu.Unlock()
		return fmt.Errorf("sim: unknown disk %s", req.DiskID)
	}
	if p.linkErrorsRemaining > 0 {
		p.linkErrorsRemaining--
		a.mu.Unlock()
		return &Error{Kind: FaultLink, PathID: req.PathID, DiskID: req.DiskID, Message: "injected link error"}
	}
	if d.busyRemaining > 0 {
		d.busyRemaining--
		wait := d.busyRetryAfter
		a.mu.Unlock()
		if wait <= 0 {
			wait = 5 * time.Millisecond
		}
		return &Error{Kind: FaultBusy, PathID: req.PathID, DiskID: req.DiskID, Message: "disk temporarily busy", RetryAfter: wait, Temporary: true}
	}
	if d.mediaRemaining > 0 {
		d.mediaRemaining--
		a.mu.Unlock()
		return &Error{Kind: FaultMedia, PathID: req.PathID, DiskID: req.DiskID, Message: "injected correctable media error", Temporary: true, RetryAfter: 2 * time.Millisecond}
	}
	a.mu.Unlock()

	a.mu.RLock()
	defer a.mu.RUnlock()
	p = a.paths[req.PathID]
	d = a.disks[req.DiskID]
	if !p.up {
		return &Error{Kind: FaultLink, PathID: req.PathID, DiskID: req.DiskID, Message: "link down"}
	}
	if req.Offset+int64(len(req.Data)) > d.info.Size {
		return fmt.Errorf("sim: IO beyond disk size: %d+%d > %d", req.Offset, len(req.Data), d.info.Size)
	}

	latency := a.cfg.PathRTT + a.jitter()
	if p.info.Controller != d.info.OwnerController {
		latency += a.cfg.NonOwnerPenalty
	}
	if p.blackhole {
		if err := sleepContext(ctx, a.cfg.BlackholeHold); err != nil {
			return &Error{Kind: FaultTimeout, PathID: req.PathID, DiskID: req.DiskID, Message: "IO timed out", Temporary: true}
		}
	} else if err := sleepContext(ctx, latency); err != nil {
		return &Error{Kind: FaultTimeout, PathID: req.PathID, DiskID: req.DiskID, Message: "IO timed out", Temporary: true}
	}

	start := int(req.Offset)
	end := start + len(req.Data)
	switch req.Op {
	case Write:
		copy(d.data[start:end], req.Data)
	case Read:
		copy(req.Data, d.data[start:end])
	default:
		return fmt.Errorf("sim: unknown op %q", req.Op)
	}
	return nil
}

// SetPathFault sets one path to up, hard link-down, or black-holed timeout.
func (a *Array) SetPathFault(pathID, fault string) error {
	a.mu.Lock()
	p, ok := a.paths[pathID]
	if !ok {
		a.mu.Unlock()
		return fmt.Errorf("unknown path %s", pathID)
	}
	switch fault {
	case "up", "recover":
		p.up = true
		p.blackhole = false
	case "link_down", "down":
		p.up = false
		p.blackhole = false
	case "timeout", "blackhole":
		p.up = true
		p.blackhole = true
	default:
		a.mu.Unlock()
		return fmt.Errorf("unknown fault %q", fault)
	}
	a.mu.Unlock()
	return nil
}

// NotifyPathState emits an unsolicited array/controller path-state notification.
// Unlike SetPathFault, this does not change the simulated wire fault itself.
func (a *Array) NotifyPathState(pathID, state, message string) error {
	a.mu.RLock()
	_, ok := a.paths[pathID]
	a.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown path %s", pathID)
	}
	switch state {
	case "up", "down", "checking":
	default:
		return fmt.Errorf("invalid path state %q", state)
	}
	if message == "" {
		message = "array reported path state " + state
	}
	a.emit(Event{
		Type:    "path_state",
		PathID:  pathID,
		State:   state,
		Reason:  "array_notification",
		Message: message,
	})
	return nil
}

// RecoverPath restores a path.
func (a *Array) RecoverPath(pathID string) error { return a.SetPathFault(pathID, "up") }

// SetSwitchFault applies a fault to both paths through one switch.
func (a *Array) SetSwitchFault(switchID, fault string) (int, error) {
	a.mu.Lock()
	changed := 0
	for _, id := range a.order {
		p := a.paths[id]
		if p.info.Switch != switchID {
			continue
		}
		switch fault {
		case "up", "recover":
			p.up = true
			p.blackhole = false
		case "link_down", "down":
			p.up = false
			p.blackhole = false
		case "timeout", "blackhole":
			p.up = true
			p.blackhole = true
		default:
			a.mu.Unlock()
			return 0, fmt.Errorf("unknown fault %q", fault)
		}
		changed++
	}
	a.mu.Unlock()
	if changed == 0 {
		return 0, fmt.Errorf("unknown switch %s", switchID)
	}
	return changed, nil
}

// InjectLinkError makes the next n IOs on a path fail as link errors.
func (a *Array) InjectLinkError(pathID string, n int) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	p, ok := a.paths[pathID]
	if !ok {
		return fmt.Errorf("unknown path %s", pathID)
	}
	if n < 1 {
		n = 1
	}
	p.linkErrorsRemaining += n
	return nil
}

// InjectBusy makes the next n operations for a disk return busy.
func (a *Array) InjectBusy(diskID string, n int, retryAfter time.Duration) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	d, ok := a.disks[diskID]
	if !ok {
		return fmt.Errorf("unknown disk %s", diskID)
	}
	if n < 1 {
		n = 1
	}
	d.busyRemaining += n
	if retryAfter > 0 {
		d.busyRetryAfter = retryAfter
	}
	return nil
}

// InjectMediaError makes the next n disk operations return a correctable media error.
func (a *Array) InjectMediaError(diskID string, n int) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	d, ok := a.disks[diskID]
	if !ok {
		return fmt.Errorf("unknown disk %s", diskID)
	}
	if n < 1 {
		n = 1
	}
	d.mediaRemaining += n
	return nil
}

// SetOwner changes the preferred controller and emits an array notification.
func (a *Array) SetOwner(diskID string, controller int, transition time.Duration) error {
	if controller != 0 && controller != 1 {
		return fmt.Errorf("controller must be 0 or 1")
	}
	a.mu.Lock()
	d, ok := a.disks[diskID]
	if !ok {
		a.mu.Unlock()
		return fmt.Errorf("unknown disk %s", diskID)
	}
	old := d.info.OwnerController
	d.info.OwnerController = controller
	if transition > 0 {
		d.info.OwnerStable = false
		d.info.OwnerStableUntil = time.Now().Add(transition)
	} else {
		d.info.OwnerStable = true
		d.info.OwnerStableUntil = time.Time{}
	}
	a.mu.Unlock()

	a.emit(Event{
		Type:         "owner_changed",
		DiskID:       diskID,
		Controller:   controller,
		Transition:   transition,
		Reason:       "array_notification",
		Message:      fmt.Sprintf("primary controller changed ctl%d -> ctl%d", old, controller),
		Notification: true,
	})
	return nil
}

// ExpandDisk grows a disk online and emits a capacity notification.
func (a *Array) ExpandDisk(diskID string, extra int64) (DiskInfo, error) {
	if extra <= 0 {
		return DiskInfo{}, fmt.Errorf("extra size must be positive")
	}
	a.mu.Lock()
	d, ok := a.disks[diskID]
	if !ok {
		a.mu.Unlock()
		return DiskInfo{}, fmt.Errorf("unknown disk %s", diskID)
	}
	grown := make([]byte, d.info.Size+extra)
	copy(grown, d.data)
	d.data = grown
	d.info.Size += extra
	if d.info.UsedBytes > d.info.Size {
		d.info.UsedBytes = d.info.Size
	}
	d.info.FreeBytes = d.info.Size - d.info.UsedBytes
	size := d.info.Size
	info := d.info
	a.mu.Unlock()

	a.emit(Event{
		Type:         "capacity_changed",
		DiskID:       diskID,
		Size:         size,
		Reason:       "array_notification",
		Message:      fmt.Sprintf("disk expanded online to %d bytes", size),
		Notification: true,
	})
	return info, nil
}

// SetSpacePressure updates used capacity and emits a threshold notification.
func (a *Array) SetSpacePressure(diskID string, usedPercent int) (DiskInfo, error) {
	if usedPercent < 0 || usedPercent > 100 {
		return DiskInfo{}, fmt.Errorf("used percent must be 0..100")
	}
	a.mu.Lock()
	d, ok := a.disks[diskID]
	if !ok {
		a.mu.Unlock()
		return DiskInfo{}, fmt.Errorf("unknown disk %s", diskID)
	}
	d.info.UsedPercent = usedPercent
	d.info.UsedBytes = d.info.Size * int64(usedPercent) / 100
	d.info.FreeBytes = d.info.Size - d.info.UsedBytes
	wasAlert := d.info.SpaceAlert
	d.info.SpaceAlert = usedPercent >= d.info.SpaceThreshold
	info := d.info
	a.mu.Unlock()

	if info.SpaceAlert && !wasAlert {
		a.emit(Event{
			Type:         "space_alert",
			DiskID:       diskID,
			UsedPercent:  usedPercent,
			Reason:       "array_notification",
			Message:      fmt.Sprintf("disk space %d%% used; threshold %d%%", usedPercent, info.SpaceThreshold),
			Notification: true,
		})
	}
	return info, nil
}

// ReadDirect reads the simulated backend directly. It is intended for tests/tools.
func (a *Array) ReadDirect(diskID string, offset int64, p []byte) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	d, ok := a.disks[diskID]
	if !ok {
		return fmt.Errorf("unknown disk %s", diskID)
	}
	if offset < 0 || offset+int64(len(p)) > d.info.Size {
		return fmt.Errorf("read beyond disk")
	}
	copy(p, d.data[offset:offset+int64(len(p))])
	return nil
}
