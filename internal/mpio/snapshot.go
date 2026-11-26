package mpio

import (
	"context"
	"sort"
	"time"
)

// Snapshot returns a point-in-time copy suitable for JSON rendering.
func (m *Manager) Snapshot() Snapshot {
	hourly := m.HourlyRows("")

	m.mu.RLock()
	defer m.mu.RUnlock()

	snap := Snapshot{
		GeneratedAt:   time.Now(),
		Ready:         !m.rechecking,
		Rechecking:    m.rechecking,
		RecheckID:     m.recheckID,
		OptimalIOSize: m.optimal,
		BlockSize:     m.block,
		Hourly:        hourly,
	}

	pathIDs := append([]string(nil), m.pathOrder...)
	for _, id := range pathIDs {
		p := m.paths[id]
		if p == nil {
			continue
		}
		ps := PathSnapshot{
			ID:              p.info.ID,
			NIC:             p.info.NIC,
			Switch:          p.info.Switch,
			Controller:      p.info.Controller,
			State:           p.state,
			BasePriority:    100,
			Active:          p.active,
			Completed:       p.completed,
			ReadBytes:       p.readBytes,
			WriteBytes:      p.writeBytes,
			BytesTotal:      p.readBytes + p.writeBytes,
			PathErrors:      p.pathErrors,
			BusyRetries:     p.busyRetries,
			MediaErrors:     p.mediaErrors,
			LastLatencyMS:   float64(p.lastLatency.Microseconds()) / 1000.0,
			LastProbeAt:     p.lastProbeAt,
			LastStateChange: p.lastStateChange,
			LastReason:      p.lastReason,
			LastDetail:      p.lastDetail,
			LastError:       p.lastError,
		}
		if p.ewmaLatency > 0 {
			ps.EWMALatencyMS = float64(p.ewmaLatency.Microseconds()) / 1000.0
		}
		if p.latencyCount > 0 {
			ps.AverageLatencyMS = float64(p.latencySum.Microseconds()) / float64(p.latencyCount) / 1000.0
		}
		if byPath, ok := m.hourly[hourKey(snap.GeneratedAt)]; ok {
			if b, ok := byPath[id]; ok {
				bc := *b
				ps.CurrentHour = &bc
			}
		}
		snap.Paths = append(snap.Paths, ps)

		snap.Summary.ActiveRequests += p.active
		snap.Summary.TotalCompleted += p.completed
		snap.Summary.TotalBytes += ps.BytesTotal
		snap.Summary.TotalPathErrors += p.pathErrors
		snap.Summary.TotalBusyRetries += p.busyRetries
		snap.Summary.TotalMediaErrors += p.mediaErrors
		switch p.state {
		case StateUp:
			snap.Summary.PathsUp++
		case StateChecking:
			snap.Summary.PathsChecking++
		case StateDown:
			snap.Summary.PathsDown++
		}
	}
	snap.Summary.PathCount = len(pathIDs)

	diskIDs := make([]string, 0, len(m.disks))
	for id := range m.disks {
		diskIDs = append(diskIDs, id)
	}
	sort.Strings(diskIDs)
	for _, id := range diskIDs {
		ds := m.disks[id]
		di := DiskSnapshot{
			ID:             ds.info.ID,
			Index:          ds.info.Index,
			Size:           ds.info.Size,
			BlockSize:      ds.info.BlockSize,
			OptimalIOSize:  ds.info.OptimalIOSize,
			Owner:          ds.info.OwnerController,
			OwnerStable:    ds.info.OwnerStable,
			Switching:      ds.switching,
			SpaceAlert:     ds.info.SpaceAlert,
			UsedPercent:    ds.info.UsedPercent,
			UsedBytes:      ds.info.UsedBytes,
			FreeBytes:      ds.info.FreeBytes,
			PathPriorities: make(map[string]int, len(m.pathOrder)),
		}
		for _, pid := range m.pathOrder {
			p := m.paths[pid]
			if p != nil && p.info.Controller == ds.info.OwnerController {
				di.PathPriorities[pid] = 100
			} else {
				di.PathPriorities[pid] = 50
			}
		}
		snap.Disks = append(snap.Disks, di)
	}

	for i := len(m.events) - 1; i >= 0; i-- {
		snap.Events = append(snap.Events, m.events[i])
	}

	for i := len(m.switchOrder) - 1; i >= 0; i-- {
		id := m.switchOrder[i]
		if sw := m.switches[id]; sw != nil {
			cp := *sw
			snap.Switches = append(snap.Switches, cp)
			if cp.Status == "active" {
				snap.Summary.ActiveSwitches++
			}
		}
	}
	snap.Summary.SwitchCount = int64(m.switchSeq)

	active := make([]Suspension, 0, len(m.suspensions))
	for _, s := range m.suspensions {
		active = append(active, *s)
	}
	sort.Slice(active, func(i, j int) bool { return active[i].StartedAt.Before(active[j].StartedAt) })
	snap.ActiveSuspensions = active
	snap.Summary.ActiveSuspensions = len(active)

	for i := len(m.recentSusp) - 1; i >= 0; i-- {
		snap.RecentSuspensions = append(snap.RecentSuspensions, m.recentSusp[i])
	}

	snap.Summary.TotalSuspensions = m.totalSuspensions
	snap.Summary.TotalSuspensionWaitMS = m.totalSuspensionWaitMS
	snap.Summary.MaxSuspensionWaitMS = m.maxSuspensionWaitMS
	snap.Summary.RequestCounter = m.reqSeq.Load()

	return snap
}

// WaitReady blocks until an initial/resume recheck completes or ctx expires.
func (m *Manager) WaitReady(ctx context.Context) error {
	for {
		m.mu.RLock()
		checking := m.rechecking
		done := m.recheckDone
		m.mu.RUnlock()
		if !checking {
			return nil
		}
		if done == nil {
			return nil
		}
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
