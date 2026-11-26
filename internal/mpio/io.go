package mpio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"southpole/mpio/internal/sim"
)

// Chunk is one optimal-unit-aligned piece of an upper-layer request.
type Chunk struct {
	Index   int   `json:"index"`
	Offset  int64 `json:"offset"`
	Length  int   `json:"length"`
	Aligned bool  `json:"aligned_to_optimal"`
}

// PlanChunks splits a request. Complete chunks are exactly the array's optimal
// transfer unit; only the request's head/tail can be shorter.
func PlanChunks(offset, length, optimal int64) []Chunk {
	if length <= 0 {
		return nil
	}
	if optimal <= 0 {
		optimal = 64 << 10
	}
	var chunks []Chunk
	end := offset + length
	pos := offset
	idx := 0

	alignedNext := ((pos + optimal - 1) / optimal) * optimal
	if pos%optimal != 0 && alignedNext > pos {
		n := alignedNext - pos
		if n > length {
			n = length
		}
		chunks = append(chunks, Chunk{Index: idx, Offset: pos, Length: int(n), Aligned: false})
		pos += n
		idx++
	}
	for pos < end {
		n := optimal
		if end-pos < n {
			n = end - pos
		}
		chunks = append(chunks, Chunk{Index: idx, Offset: pos, Length: int(n), Aligned: n == optimal && pos%optimal == 0})
		pos += n
		idx++
	}
	return chunks
}

// Device is one online multipath block disk.
type Device struct {
	mgr *Manager
	id  string
}

// ID returns the array disk identifier.
func (d *Device) ID() string { return d.id }

// Manager returns the owning multipath manager.
func (d *Device) Manager() *Manager { return d.mgr }

// Capacity returns the current capacity; online expansion changes this without remount.
func (d *Device) Capacity() int64 {
	d.mgr.mu.RLock()
	defer d.mgr.mu.RUnlock()
	if ds := d.mgr.disks[d.id]; ds != nil {
		return ds.info.Size
	}
	return 0
}

// OwnerController returns the array-reported preferred controller.
func (d *Device) OwnerController() int {
	d.mgr.mu.RLock()
	defer d.mgr.mu.RUnlock()
	if ds := d.mgr.disks[d.id]; ds != nil {
		return ds.info.OwnerController
	}
	return -1
}

// SpaceAlert reports the array's early capacity warning.
func (d *Device) SpaceAlert() bool {
	d.mgr.mu.RLock()
	defer d.mgr.mu.RUnlock()
	if ds := d.mgr.disks[d.id]; ds != nil {
		return ds.info.SpaceAlert
	}
	return false
}

// ReadAtContext reads len(p) bytes at offset.
func (d *Device) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	return d.do(ctx, sim.Read, p, off)
}

// WriteAtContext writes len(p) bytes at offset.
func (d *Device) WriteAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	return d.do(ctx, sim.Write, p, off)
}

// ReadAt implements io.ReaderAt.
func (d *Device) ReadAt(p []byte, off int64) (int, error) {
	return d.ReadAtContext(nil, p, off)
}

// WriteAt implements io.WriterAt.
func (d *Device) WriteAt(p []byte, off int64) (int, error) {
	return d.WriteAtContext(nil, p, off)
}

func (d *Device) do(parent context.Context, op sim.IOp, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("mpio: negative offset %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	capacity := d.Capacity()
	if off+int64(len(p)) > capacity {
		return 0, fmt.Errorf("mpio: %s range [%d,%d) exceeds capacity %d", op, off, off+int64(len(p)), capacity)
	}

	ctx := parent
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && d.mgr.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.mgr.cfg.RequestTimeout)
		defer cancel()
	}

	reqID := d.mgr.NextRequestID()
	chunks := PlanChunks(off, int64(len(p)), int64(d.mgr.optimal))
	chunkCtx, cancelChunks := context.WithCancel(ctx)
	defer cancelChunks()

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancelChunks()
		}
		errMu.Unlock()
	}

	for _, chunk := range chunks {
		chunk := chunk
		rel := chunk.Offset - off
		if rel < 0 || rel+int64(chunk.Length) > int64(len(p)) {
			setErr(fmt.Errorf("mpio: internal chunk %+v outside request", chunk))
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := d.mgr.executeChunk(chunkCtx, reqID, d.id, op, p[rel:rel+int64(chunk.Length)], chunk.Offset, chunk.Index)
			setErr(err)
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return 0, firstErr
	}
	return len(p), nil
}

// Device returns one disk handle.
func (m *Manager) Device(id string) (*Device, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.disks[id]; !ok {
		return nil, fmt.Errorf("mpio: unknown device %s", id)
	}
	return &Device{mgr: m, id: id}, nil
}

// Devices returns all managed disk handles in identifier order.
func (m *Manager) Devices() []*Device {
	m.mu.RLock()
	ids := make([]string, 0, len(m.disks))
	for id := range m.disks {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	sort.Strings(ids)
	out := make([]*Device, 0, len(ids))
	for _, id := range ids {
		out = append(out, &Device{mgr: m, id: id})
	}
	return out
}

func (m *Manager) acquirePath(diskID string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ds := m.disks[diskID]
	if ds == nil {
		return "", false
	}
	owner := ds.info.OwnerController

	var best *pathState
	bestCost := math.MaxFloat64
	for _, id := range m.pathOrder {
		p := m.paths[id]
		if p == nil || p.state != StateUp {
			continue
		}
		priority := 50
		if p.info.Controller == owner {
			priority = 100
		}
		latencyMS := float64(p.ewmaLatency.Microseconds()) / 1000.0
		if latencyMS <= 0 {
			latencyMS = 1
		}
		// Prefer optimized controller, then less outstanding work and lower latency.
		cost := float64(p.active+1) * latencyMS * (100.0 / float64(priority))
		if cost < bestCost {
			bestCost = cost
			best = p
		}
	}
	if best == nil {
		return "", false
	}
	best.active++
	return best.info.ID, true
}

func (m *Manager) waitGlobalReady(ctx context.Context, meta suspensionInput) error {
	for {
		m.mu.RLock()
		checking := m.rechecking
		done := m.recheckDone
		switchID := m.recheckSwID
		m.mu.RUnlock()
		if !checking {
			return nil
		}
		waitMeta := meta
		waitMeta.Reason = ReasonHostResume
		waitMeta.SwitchID = switchID
		id := m.beginSuspension(waitMeta)
		select {
		case <-done:
			m.endSuspension(id, false)
			return nil
		case <-ctx.Done():
			m.endSuspension(id, true)
			return ctx.Err()
		}
	}
}

func (m *Manager) waitDiskReady(ctx context.Context, meta suspensionInput) error {
	for {
		m.mu.RLock()
		ds := m.disks[meta.DiskID]
		var ready <-chan struct{}
		switchID := uint64(0)
		switching := false
		if ds != nil {
			switching = ds.switching
			ready = ds.ready
			switchID = ds.switchID
		}
		m.mu.RUnlock()
		if !switching || ready == nil {
			return nil
		}
		waitMeta := meta
		waitMeta.Reason = ReasonControllerSwitch
		waitMeta.SwitchID = switchID
		id := m.beginSuspension(waitMeta)
		select {
		case <-ready:
			m.endSuspension(id, false)
		case <-ctx.Done():
			m.endSuspension(id, true)
			return ctx.Err()
		}
	}
}

func (m *Manager) waitAndAcquirePath(ctx context.Context, diskID string, meta suspensionInput) (string, error) {
	for {
		if err := m.waitGlobalReady(ctx, meta); err != nil {
			return "", err
		}
		if err := m.waitDiskReady(ctx, meta); err != nil {
			return "", err
		}
		if pathID, ok := m.acquirePath(diskID); ok {
			return pathID, nil
		}
		wake := m.wakeChannel()
		select {
		case <-wake:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

func (m *Manager) startPathSwitch(reason, pathID, diskID, detail string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pathID != "" {
		if id := m.activePathSwitch[pathID]; id != 0 {
			return id
		}
	}
	return m.beginSwitchLocked(SwitchEvent{
		Kind:   "path_failover",
		Reason: reason,
		PathID: pathID,
		DiskID: diskID,
		Detail: detail,
	})
}

func (m *Manager) markPathIOFault(pathID, reason, detail string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.paths[pathID]
	if p == nil {
		return
	}
	if p.state != StateDown {
		m.transitionPathLocked(p, StateDown, reason, detail)
	} else {
		p.lastReason = reason
		p.lastDetail = detail
		p.lastError = detail
	}
	m.broadcastLocked()
}

func (m *Manager) executeChunk(parent context.Context, reqID uint64, diskID string, op sim.IOp, buf []byte, offset int64, chunkIndex int) error {
	meta := suspensionInput{
		RequestID:  reqID,
		ChunkIndex: chunkIndex,
		DiskID:     diskID,
		Op:         string(op),
		Offset:     offset,
		Length:     len(buf),
	}

	var presetPath string
	for {
		var pathID string
		if presetPath != "" {
			pathID = presetPath
			presetPath = ""
		} else {
			if err := m.waitGlobalReady(parent, meta); err != nil {
				return err
			}
			if err := m.waitDiskReady(parent, meta); err != nil {
				return err
			}
			acquired, ok := m.acquirePath(diskID)
			if !ok {
				switchID := m.startPathSwitch(ReasonNoHealthyPath, "", diskID, "all paths are unavailable; request queued")
				waitMeta := meta
				waitMeta.Reason = ReasonNoHealthyPath
				waitMeta.SwitchID = switchID
				suspID := m.beginSuspension(waitMeta)
				next, err := m.waitAndAcquirePath(parent, diskID, meta)
				m.endSuspension(suspID, err != nil)
				if err != nil {
					return err
				}
				presetPath = next
				continue
			}
			pathID = acquired
		}

		attemptCtx, cancelAttempt := context.WithTimeout(parent, m.cfg.IOTimeout)
		start := time.Now()
		err := m.backend.DoIO(attemptCtx, sim.IORequest{
			PathID: pathID,
			DiskID: diskID,
			Op:     op,
			Offset: offset,
			Data:   buf,
		})
		latency := time.Since(start)
		cancelAttempt()

		if err == nil {
			m.finishAttempt(pathID, op, len(buf), latency, attemptOK, "")
			return nil
		}

		var se *sim.Error
		_ = errors.As(err, &se)
		pathFault := sim.IsPathFault(err)
		temporaryArray := sim.IsTemporaryArrayError(err)

		switch {
		case errors.Is(err, context.Canceled) || errors.Is(parent.Err(), context.Canceled):
			m.finishAttempt(pathID, op, len(buf), latency, attemptCanceled, err.Error())
			return err
		case pathFault:
			reason := ReasonLinkError
			if se != nil && se.Kind == sim.FaultTimeout {
				reason = ReasonIOTimeout
			}
			m.finishAttempt(pathID, op, len(buf), latency, attemptPathError, err.Error())
			m.markPathIOFault(pathID, reason, err.Error())
			switchID := m.startPathSwitch(reason, pathID, diskID, err.Error())

			failMeta := meta
			failMeta.Reason = reason
			failMeta.FailedPath = pathID
			failMeta.SwitchID = switchID
			suspID := m.beginSuspension(failMeta)
			next, waitErr := m.waitAndAcquirePath(parent, diskID, meta)
			m.endSuspension(suspID, waitErr != nil)
			if waitErr != nil {
				return waitErr
			}
			presetPath = next
			continue
		case temporaryArray:
			result := attemptBusy
			wait := m.cfg.BusyWait
			if se != nil {
				if se.Kind == sim.FaultMedia {
					result = attemptMedia
					wait = se.RetryAfter
					if wait <= 0 {
						wait = 2 * time.Millisecond
					}
				} else if se.RetryAfter > 0 {
					wait = se.RetryAfter
				}
			}
			m.finishAttempt(pathID, op, len(buf), latency, result, err.Error())
			if wait > m.cfg.MaxBusyWait {
				wait = m.cfg.MaxBusyWait
			}
			timer := time.NewTimer(wait)
			select {
			case <-parent.Done():
				timer.Stop()
				return parent.Err()
			case <-timer.C:
			}
			continue
		default:
			// Unexpected backend/accounting error: balance active count and surface it.
			m.finishAttempt(pathID, op, len(buf), latency, attemptCanceled, err.Error())
			return fmt.Errorf("mpio: unclassified array error on %s: %w", pathID, err)
		}
	}
}
