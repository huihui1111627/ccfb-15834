package mpio

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"southpole/mpio/internal/sim"
)

type pathState struct {
	info  sim.PathInfo
	state string

	active int

	completed    int64
	readBytes    int64
	writeBytes   int64
	pathErrors   int64
	busyRetries  int64
	mediaErrors  int64
	latencySum   time.Duration
	latencyCount int64
	ewmaLatency  time.Duration
	lastLatency  time.Duration

	lastProbeAt     time.Time
	lastStateChange time.Time
	lastReason      string
	lastDetail      string
	lastError       string
	probeOK         bool
}

type diskState struct {
	info      sim.DiskInfo
	switching bool
	switchID  uint64
	ready     chan struct{}
}

// Manager owns paths, failover, events, statistics and simulated devices.
type Manager struct {
	cfg     Config
	backend Backend
	optimal int
	block   int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	lifeMu sync.RWMutex
	closed bool

	reqSeq atomic.Uint64

	mu               sync.RWMutex
	paths            map[string]*pathState
	pathOrder        []string
	disks            map[string]*diskState
	events           []Event
	eventSeq         uint64
	switchSeq        uint64
	switches         map[uint64]*SwitchEvent
	switchOrder      []uint64
	activePathSwitch map[string]uint64
	suspensions      map[uint64]*Suspension
	recentSusp       []Suspension
	suspSeq          uint64
	hourly           map[string]map[string]*HourBucket
	alertSubs        map[int]chan Alert
	alertSeq         int

	wake        chan struct{}
	rechecking  bool
	recheckID   uint64
	recheckDone chan struct{}
	recheckSwID uint64

	totalSuspensions      int64
	totalSuspensionWaitMS int64
	maxSuspensionWaitMS   int64
}

// NewManager creates a multipath manager over a simulated array.
func NewManager(backend Backend, cfg Config) *Manager {
	def := DefaultConfig()
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = def.ProbeInterval
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = def.ProbeTimeout
	}
	if cfg.IOTimeout <= 0 {
		cfg.IOTimeout = def.IOTimeout
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = def.RequestTimeout
	}
	if cfg.BusyWait <= 0 {
		cfg.BusyWait = def.BusyWait
	}
	if cfg.MaxBusyWait <= 0 {
		cfg.MaxBusyWait = def.MaxBusyWait
	}
	if cfg.HistoryLimit <= 0 {
		cfg.HistoryLimit = def.HistoryLimit
	}
	if cfg.SuspensionLimit <= 0 {
		cfg.SuspensionLimit = def.SuspensionLimit
	}
	if cfg.SwitchLimit <= 0 {
		cfg.SwitchLimit = def.SwitchLimit
	}
	if cfg.HourRetention <= 0 {
		cfg.HourRetention = def.HourRetention
	}
	if cfg.OwnerSettleWait <= 0 {
		cfg.OwnerSettleWait = def.OwnerSettleWait
	}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg:              cfg,
		backend:          backend,
		optimal:          backend.OptimalIOSize(),
		block:            backend.BlockSize(),
		ctx:              ctx,
		cancel:           cancel,
		paths:            make(map[string]*pathState),
		disks:            make(map[string]*diskState),
		switches:         make(map[uint64]*SwitchEvent),
		activePathSwitch: make(map[string]uint64),
		suspensions:      make(map[uint64]*Suspension),
		hourly:           make(map[string]map[string]*HourBucket),
		alertSubs:        make(map[int]chan Alert),
		wake:             make(chan struct{}),
	}
	if m.optimal <= 0 {
		m.optimal = 64 << 10
	}
	if m.block <= 0 {
		m.block = 4 << 10
	}

	for _, info := range backend.PathInfos() {
		m.pathOrder = append(m.pathOrder, info.ID)
		m.paths[info.ID] = &pathState{
			info:            info,
			state:           StateChecking,
			ewmaLatency:     time.Millisecond,
			lastStateChange: time.Now(),
			lastReason:      ReasonStartupCheck,
			lastDetail:      "waiting for initial probe",
		}
	}
	for _, info := range backend.DiskInfos() {
		m.disks[info.ID] = &diskState{info: info}
	}
	return m
}

// Start begins array-event processing and health probing, then confirms paths.
func (m *Manager) Start() {
	eventCh, unsub := m.backend.Subscribe(1024)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer unsub()
		m.watchEvents(eventCh)
	}()

	m.wg.Add(1)
	go m.probeLoop()

	m.wg.Add(1)
	go m.watchHostSleep()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = m.RecheckAll(ctx, ReasonStartupCheck)
}

// Close stops background goroutines.
func (m *Manager) Close() error {
	m.cancel()

	// Wait for any RecheckAll that entered before shutdown to finish adding
	// WaitGroup work, then mark closed so no external caller can race Wait.
	m.lifeMu.Lock()
	m.closed = true
	m.lifeMu.Unlock()

	m.wg.Wait()
	return nil
}

func (m *Manager) watchEvents(ch <-chan sim.Event) {
	for {
		select {
		case <-m.ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			m.handleArrayEvent(ev)
		}
	}
}

func (m *Manager) handleArrayEvent(ev sim.Event) {
	switch ev.Type {
	case "path_state":
		m.mu.Lock()
		if p := m.paths[ev.PathID]; p != nil {
			reason := ev.Reason
			if reason == "" {
				reason = ReasonArrayNotify
			}
			m.transitionPathLocked(p, ev.State, reason, ev.Message)
			m.broadcastLocked()
		}
		m.mu.Unlock()
	case "owner_changed":
		m.applyOwnerChange(ev)
	case "capacity_changed":
		info, ok := m.backend.DiskInfo(ev.DiskID)
		m.mu.Lock()
		ds := m.disks[ev.DiskID]
		if ds != nil {
			if ok {
				ds.info = info
			} else {
				ds.info.Size = ev.Size
			}
			m.logEventLocked(Event{
				Kind:    "capacity_change",
				Level:   "success",
				DiskID:  ev.DiskID,
				Reason:  ReasonArrayNotify,
				Message: ev.Message,
			})
			m.broadcastLocked()
		}
		m.mu.Unlock()
	case "space_alert":
		info, ok := m.backend.DiskInfo(ev.DiskID)
		m.mu.Lock()
		ds := m.disks[ev.DiskID]
		if ds != nil {
			if ok {
				ds.info = info
			} else {
				ds.info.SpaceAlert = true
				ds.info.UsedPercent = ev.UsedPercent
			}
			m.logEventLocked(Event{
				Kind:    "space_alert",
				Level:   "warning",
				DiskID:  ev.DiskID,
				Reason:  ReasonArrayNotify,
				Message: ev.Message,
			})
			m.publishAlertLocked(Alert{
				DiskID:      ev.DiskID,
				Kind:        "space_low",
				Level:       "warning",
				UsedPercent: ev.UsedPercent,
				Message:     ev.Message,
			})
			m.broadcastLocked()
		}
		m.mu.Unlock()
	}
}

func (m *Manager) applyOwnerChange(ev sim.Event) {
	info, hasInfo := m.backend.DiskInfo(ev.DiskID)

	m.mu.Lock()
	ds := m.disks[ev.DiskID]
	if ds == nil {
		m.mu.Unlock()
		return
	}
	old := ds.info.OwnerController
	if ds.switching && ds.switchID != 0 {
		// A newer owner event supersedes this window; don't leave it "active".
		m.finishSwitchLocked(ds.switchID)
	}
	if hasInfo {
		ds.info = info
	} else {
		ds.info.OwnerController = ev.Controller
	}

	id := m.beginSwitchLocked(SwitchEvent{
		Kind:   ReasonControllerSwitch,
		Status: "active",
		Reason: ReasonArrayNotify,
		DiskID: ev.DiskID,
		Detail: ev.Message,
	})
	ready := make(chan struct{})
	ds.switching = true
	ds.switchID = id
	ds.ready = ready

	m.logEventLocked(Event{
		Kind:    "owner_change",
		Level:   "notice",
		DiskID:  ev.DiskID,
		Reason:  ReasonArrayNotify,
		Message: fmt.Sprintf("primary controller ctl%d -> ctl%d; priorities updated", old, ev.Controller),
	})
	m.broadcastLocked()
	m.mu.Unlock()

	wait := ev.Transition
	if wait <= 0 {
		wait = 20 * time.Millisecond
	}
	m.wg.Add(1)
	go m.waitOwnerStable(ev.DiskID, id, ready, wait+m.cfg.OwnerSettleWait)
}

func (m *Manager) waitOwnerStable(diskID string, switchID uint64, ready chan struct{}, maxWait time.Duration) {
	defer m.wg.Done()
	defer close(ready)
	deadline := time.Now().Add(maxWait)
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		m.mu.RLock()
		currentSwitchID := uint64(0)
		if ds := m.disks[diskID]; ds != nil {
			currentSwitchID = ds.switchID
		}
		m.mu.RUnlock()
		if currentSwitchID != switchID {
			return
		}

		info, ok := m.backend.DiskInfo(diskID)
		if ok && info.OwnerStable {
			break
		}
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
		}
	}
	m.mu.Lock()
	if ds := m.disks[diskID]; ds != nil && ds.switchID == switchID {
		ds.switching = false
		ds.switchID = 0
		ds.ready = nil
		m.finishSwitchLocked(switchID)
		m.broadcastLocked()
	}
	m.mu.Unlock()
}

func (m *Manager) pathIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := append([]string(nil), m.pathOrder...)
	return out
}

func (m *Manager) probeLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.ProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			ids := m.pathIDs()
			for _, id := range ids {
				m.wg.Add(1)
				go func(pathID string) {
					defer m.wg.Done()
					m.probeOnce(pathID)
				}(id)
			}
		}
	}
}

func (m *Manager) watchHostSleep() {
	defer m.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastWall := time.Now().UnixNano()
	for {
		select {
		case <-m.ctx.Done():
			return
		case now := <-ticker.C:
			wall := now.UnixNano()
			// A normal one-second tick cannot jump by this much unless the host
			// slept/hibernated or was stopped. Treat that as a resume event.
			if wall-lastWall > int64(5*time.Second) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_ = m.RecheckAll(ctx, ReasonHostResume)
				cancel()
			}
			lastWall = wall
		}
	}
}

// RecheckAll probes every path once. It is used at startup and after host resume.
func (m *Manager) RecheckAll(waitCtx context.Context, reason string) error {
	m.lifeMu.RLock()
	if m.closed {
		m.lifeMu.RUnlock()
		return context.Canceled
	}
	defer m.lifeMu.RUnlock()

	ids, recheckID, done, started := m.beginRecheck(reason)
	if started {
		var probeWG sync.WaitGroup
		for _, id := range ids {
			id := id
			probeWG.Add(1)
			m.wg.Add(1)
			go func() {
				defer probeWG.Done()
				defer m.wg.Done()
				m.probeOnce(id)
			}()
		}
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			probeWG.Wait()
			m.finishRecheck(recheckID)
		}()
	}

	select {
	case <-done:
		return nil
	case <-waitCtx.Done():
		return waitCtx.Err()
	}
}

func (m *Manager) beginRecheck(reason string) ([]string, uint64, <-chan struct{}, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rechecking {
		return nil, m.recheckID, m.recheckDone, false
	}
	m.recheckID++
	id := m.recheckID
	done := make(chan struct{})
	m.recheckDone = done
	m.rechecking = true

	kind := ReasonStartupCheck
	level := "notice"
	if reason == ReasonHostResume {
		kind = ReasonHostResume
		level = "warning"
	}
	swID := m.beginSwitchLocked(SwitchEvent{
		Kind:   kind,
		Status: "active",
		Reason: reason,
		Detail: "all paths are being reconfirmed before IO is released",
	})
	m.recheckSwID = swID

	now := time.Now()
	for _, pid := range m.pathOrder {
		p := m.paths[pid]
		old := p.state
		p.state = StateChecking
		p.lastReason = reason
		p.lastDetail = "recheck requested"
		p.lastStateChange = now
		if old != StateChecking {
			m.logEventLocked(Event{
				Kind:    "path_state",
				Level:   level,
				PathID:  pid,
				Reason:  reason,
				Message: fmt.Sprintf("path set to checking for %s", reason),
				From:    old,
				To:      StateChecking,
			})
		}
	}
	m.logEventLocked(Event{
		Kind:    "recheck_started",
		Level:   level,
		Reason:  reason,
		Message: "host state changed; reconfirming every path",
	})
	m.broadcastLocked()
	return append([]string(nil), m.pathOrder...), id, done, true
}

func (m *Manager) currentRecheckID() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.recheckID
}

func (m *Manager) currentRecheckDone() <-chan struct{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.recheckDone != nil {
		return m.recheckDone
	}
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (m *Manager) finishRecheck(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.rechecking || m.recheckID != id {
		return
	}
	m.rechecking = false
	done := m.recheckDone
	swID := m.recheckSwID
	m.recheckDone = nil
	m.recheckSwID = 0
	if swID != 0 {
		m.finishSwitchLocked(swID)
	}
	up, down := 0, 0
	for _, p := range m.paths {
		switch p.state {
		case StateUp:
			up++
		case StateDown:
			down++
		}
	}
	m.logEventLocked(Event{
		Kind:    "recheck_completed",
		Level:   "success",
		Reason:  ReasonProbeRecovered,
		Message: fmt.Sprintf("path reconfirmation complete: %d up, %d down", up, down),
	})
	m.broadcastLocked()
	if done != nil {
		close(done)
	}
}

func (m *Manager) probeOnce(pathID string) {
	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.ProbeTimeout)
	defer cancel()
	err := m.backend.Probe(ctx, pathID)

	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.paths[pathID]
	if p == nil {
		return
	}
	now := time.Now()
	p.lastProbeAt = now
	if err == nil {
		p.probeOK = true
		p.lastError = ""
		if p.state != StateUp {
			m.transitionPathLocked(p, StateUp, ReasonProbeRecovered, "probe succeeded")
			if swID := m.activePathSwitch[pathID]; swID != 0 {
				if sw := m.switches[swID]; sw != nil && sw.ActiveSuspensions == 0 {
					m.finishSwitchLocked(swID)
					delete(m.activePathSwitch, pathID)
				}
			}
		}
		m.broadcastLocked()
		return
	}

	reason := ReasonProbeError
	var se *sim.Error
	if errors.As(err, &se) {
		switch se.Kind {
		case sim.FaultTimeout:
			reason = ReasonProbeTimeout
		case sim.FaultLink:
			reason = ReasonLinkError
		}
	}
	p.probeOK = false
	p.lastError = err.Error()
	m.addStatErrorLocked(pathID, "path", now)
	if p.state != StateDown {
		m.transitionPathLocked(p, StateDown, reason, err.Error())
	} else {
		p.lastReason = reason
		p.lastDetail = err.Error()
	}
	m.broadcastLocked()
}

func (m *Manager) transitionPathLocked(p *pathState, to, reason, detail string) {
	from := p.state
	now := time.Now()
	p.state = to
	p.lastStateChange = now
	p.lastReason = reason
	p.lastDetail = detail
	if to == StateDown {
		p.lastError = detail
	}
	if from == to {
		return
	}
	level := "info"
	switch to {
	case StateDown:
		level = "error"
	case StateUp:
		level = "success"
	case StateChecking:
		level = "notice"
	}
	title := map[string]string{
		StateUp:       "path is up",
		StateDown:     "path is down",
		StateChecking: "path is being rechecked",
	}[to]
	m.logEventLocked(Event{
		Kind:    "path_state",
		Level:   level,
		PathID:  p.info.ID,
		Reason:  reason,
		Message: fmt.Sprintf("%s: %s", title, detail),
		From:    from,
		To:      to,
	})
}

func (m *Manager) logEventLocked(e Event) {
	m.eventSeq++
	e.ID = m.eventSeq
	if e.At.IsZero() {
		e.At = time.Now()
	}
	m.events = append(m.events, e)
	if len(m.events) > m.cfg.HistoryLimit {
		m.events = append([]Event(nil), m.events[len(m.events)-m.cfg.HistoryLimit:]...)
	}
}

func (m *Manager) broadcastLocked() {
	close(m.wake)
	m.wake = make(chan struct{})
}

func (m *Manager) wakeChannel() <-chan struct{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.wake
}

// Resume is the hook called after a host suspend/hibernate/wake event.
func (m *Manager) Resume(ctx context.Context) error {
	return m.RecheckAll(ctx, ReasonHostResume)
}

// NextRequestID exposes the monotonically increasing request sequence.
func (m *Manager) NextRequestID() uint64 {
	return m.reqSeq.Add(1)
}

// SubscribeAlerts receives upper-layer warnings such as array space pressure.
func (m *Manager) SubscribeAlerts(buffer int) (<-chan Alert, func()) {
	if buffer <= 0 {
		buffer = 32
	}
	ch := make(chan Alert, buffer)
	m.mu.Lock()
	id := m.alertSeq
	m.alertSeq++
	m.alertSubs[id] = ch
	m.mu.Unlock()
	return ch, func() {
		m.mu.Lock()
		delete(m.alertSubs, id)
		m.mu.Unlock()
	}
}

func (m *Manager) publishAlertLocked(alert Alert) {
	if alert.At.IsZero() {
		alert.At = time.Now()
	}
	for _, ch := range m.alertSubs {
		select {
		case ch <- alert:
		default:
			// A slow upper-layer consumer must not block IO/event processing.
		}
	}
}
