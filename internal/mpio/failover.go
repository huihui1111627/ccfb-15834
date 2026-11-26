package mpio

import (
	"time"
)

type suspensionInput struct {
	RequestID  uint64
	ChunkIndex int
	DiskID     string
	Op         string
	Offset     int64
	Length     int
	Reason     string
	FailedPath string
	SwitchID   uint64
}

func (m *Manager) beginSwitchLocked(in SwitchEvent) uint64 {
	m.switchSeq++
	id := m.switchSeq
	now := time.Now()
	sw := &SwitchEvent{
		ID:        id,
		Kind:      in.Kind,
		Status:    "active",
		Reason:    in.Reason,
		DiskID:    in.DiskID,
		PathID:    in.PathID,
		Detail:    in.Detail,
		StartedAt: now,
	}
	m.switches[id] = sw
	m.switchOrder = append(m.switchOrder, id)
	if len(m.switchOrder) > m.cfg.SwitchLimit {
		drop := m.switchOrder[0]
		m.switchOrder = m.switchOrder[1:]
		delete(m.switches, drop)
	}
	if in.Kind == "path_failover" && in.PathID != "" {
		m.activePathSwitch[in.PathID] = id
	}
	return id
}

func (m *Manager) finishSwitchLocked(id uint64) {
	sw, ok := m.switches[id]
	if !ok || sw.Status != "active" {
		return
	}
	now := time.Now()
	sw.Status = "completed"
	sw.EndedAt = now
	sw.DurationMS = now.Sub(sw.StartedAt).Milliseconds()
	if sw.Kind == "path_failover" && sw.PathID != "" {
		if current := m.activePathSwitch[sw.PathID]; current == id {
			delete(m.activePathSwitch, sw.PathID)
		}
	}
}

func (m *Manager) pathFailoverSwitch(pathID string, reason, detail string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id := m.activePathSwitch[pathID]; id != 0 {
		return id
	}
	return m.beginSwitchLocked(SwitchEvent{
		Kind:   "path_failover",
		Reason: reason,
		PathID: pathID,
		Detail: detail,
	})
}

func (m *Manager) beginSuspension(in suspensionInput) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.suspSeq++
	id := m.suspSeq
	s := &Suspension{
		ID:         id,
		SwitchID:   in.SwitchID,
		RequestID:  in.RequestID,
		ChunkIndex: in.ChunkIndex,
		DiskID:     in.DiskID,
		Op:         in.Op,
		Offset:     in.Offset,
		Length:     in.Length,
		Reason:     in.Reason,
		FailedPath: in.FailedPath,
		Status:     "waiting",
		StartedAt:  time.Now(),
	}
	m.suspensions[id] = s
	if in.SwitchID != 0 {
		if sw := m.switches[in.SwitchID]; sw != nil {
			sw.SuspendedCount++
			sw.ActiveSuspensions++
		}
	}
	return id
}

func (m *Manager) endSuspension(id uint64, failed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.suspensions[id]
	if !ok {
		return
	}
	now := time.Now()
	s.EndedAt = now
	s.DurationMS = now.Sub(s.StartedAt).Milliseconds()
	if failed {
		s.Status = "failed"
	} else {
		s.Status = "resumed"
	}

	m.totalSuspensions++
	m.totalSuspensionWaitMS += s.DurationMS
	if s.DurationMS > m.maxSuspensionWaitMS {
		m.maxSuspensionWaitMS = s.DurationMS
	}

	m.recentSusp = append(m.recentSusp, *s)
	if len(m.recentSusp) > m.cfg.SuspensionLimit {
		m.recentSusp = append([]Suspension(nil), m.recentSusp[len(m.recentSusp)-m.cfg.SuspensionLimit:]...)
	}
	delete(m.suspensions, id)

	if s.SwitchID != 0 {
		if sw := m.switches[s.SwitchID]; sw != nil {
			if sw.ActiveSuspensions > 0 {
				sw.ActiveSuspensions--
			}
			if s.DurationMS > sw.MaxWaitMS {
				sw.MaxWaitMS = s.DurationMS
			}
			if sw.Kind == "path_failover" && sw.ActiveSuspensions == 0 && sw.Status == "active" {
				m.finishSwitchLocked(sw.ID)
			}
		}
	}
	m.broadcastLocked()
}
