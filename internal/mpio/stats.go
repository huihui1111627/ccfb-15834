package mpio

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"southpole/mpio/internal/sim"
)

const (
	attemptOK = iota
	attemptPathError
	attemptBusy
	attemptMedia
	attemptCanceled
)

func hourKey(t time.Time) string {
	return t.Truncate(time.Hour).Format("2006-01-02 15:04")
}

func (m *Manager) bucketLocked(t time.Time, pathID string) *HourBucket {
	key := hourKey(t)
	byPath, ok := m.hourly[key]
	if !ok {
		byPath = make(map[string]*HourBucket)
		m.hourly[key] = byPath
	}
	b, ok := byPath[pathID]
	if !ok {
		b = &HourBucket{Hour: key, PathID: pathID}
		byPath[pathID] = b
	}
	return b
}

func (m *Manager) addStatErrorLocked(pathID, kind string, at time.Time) {
	p := m.paths[pathID]
	b := m.bucketLocked(at, pathID)
	switch kind {
	case "busy":
		if p != nil {
			p.busyRetries++
		}
		b.BusyRetries++
	case "media":
		if p != nil {
			p.mediaErrors++
		}
		b.MediaErrors++
	default:
		if p != nil {
			p.pathErrors++
		}
		b.PathErrors++
	}
}

func (m *Manager) finishAttempt(pathID string, op sim.IOp, n int, latency time.Duration, result int, errText string) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.paths[pathID]
	if p == nil {
		return
	}
	if p.active > 0 {
		p.active--
	}
	p.lastLatency = latency
	if errText != "" && result != attemptOK {
		p.lastError = errText
	}

	b := m.bucketLocked(now, pathID)
	switch result {
	case attemptOK:
		p.completed++
		p.latencySum += latency
		p.latencyCount++
		if p.ewmaLatency <= 0 {
			p.ewmaLatency = latency
		} else {
			// Low-pass EWMA makes the dashboard latency responsive but not twitchy.
			p.ewmaLatency = time.Duration(0.75*float64(p.ewmaLatency) + 0.25*float64(latency))
		}
		b.IOs++
		b.BytesTotal += int64(n)
		switch op {
		case sim.Read:
			p.readBytes += int64(n)
			b.ReadBytes += int64(n)
		case sim.Write:
			p.writeBytes += int64(n)
			b.WriteBytes += int64(n)
		}
	case attemptPathError:
		m.addStatErrorLocked(pathID, "path", now)
	case attemptBusy:
		m.addStatErrorLocked(pathID, "busy", now)
	case attemptMedia:
		m.addStatErrorLocked(pathID, "media", now)
	case attemptCanceled:
		// Cancellation is a control-plane outcome, not a path error.
	}
	m.pruneHoursLocked(now)
}

func (m *Manager) pruneHoursLocked(now time.Time) {
	if m.cfg.HourRetention <= 0 {
		return
	}
	cutoff := now.Add(-time.Duration(m.cfg.HourRetention) * time.Hour)
	for key := range m.hourly {
		t, err := time.ParseInLocation("2006-01-02 15:04", key, time.Local)
		if err == nil && t.Before(cutoff) {
			delete(m.hourly, key)
		}
	}
}

// HourlyRows returns hourly statistics sorted by hour and path.
func (m *Manager) HourlyRows(pathIDFilter string) []HourBucket {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []HourBucket
	for _, byPath := range m.hourly {
		for _, b := range byPath {
			if pathIDFilter != "" && b.PathID != pathIDFilter {
				continue
			}
			out = append(out, *b)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hour != out[j].Hour {
			return out[i].Hour < out[j].Hour
		}
		return out[i].PathID < out[j].PathID
	})
	return out
}

// ExportHourlyJSON exports hourly statistics as JSON.
func (m *Manager) ExportHourlyJSON(pathID string) ([]byte, error) {
	rows := m.HourlyRows(pathID)
	return json.MarshalIndent(rows, "", "  ")
}

// ExportHourlyCSV exports hourly statistics as CSV.
func (m *Manager) ExportHourlyCSV(pathID string) ([]byte, error) {
	rows := m.HourlyRows(pathID)
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	header := []string{"hour", "path_id", "ios", "read_bytes", "write_bytes", "bytes_total", "path_errors", "busy_retries", "media_errors"}
	if err := w.Write(header); err != nil {
		return nil, err
	}
	for _, r := range rows {
		record := []string{
			r.Hour, r.PathID,
			fmt.Sprint(r.IOs),
			fmt.Sprint(r.ReadBytes),
			fmt.Sprint(r.WriteBytes),
			fmt.Sprint(r.BytesTotal),
			fmt.Sprint(r.PathErrors),
			fmt.Sprint(r.BusyRetries),
			fmt.Sprint(r.MediaErrors),
		}
		if err := w.Write(record); err != nil {
			return nil, err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
