package mpio

import (
	"context"
	"time"

	"southpole/mpio/internal/sim"
)

// Path states.
const (
	StateChecking = "checking"
	StateUp       = "up"
	StateDown     = "down"
)

// Reasons used in path/state and switch records.
const (
	ReasonLinkError        = "link_error"
	ReasonIOTimeout        = "io_timeout"
	ReasonProbeTimeout     = "probe_timeout"
	ReasonProbeError       = "probe_error"
	ReasonProbeRecovered   = "probe_recovered"
	ReasonArrayNotify      = "array_notification"
	ReasonHostResume       = "host_resume"
	ReasonStartupCheck     = "startup_check"
	ReasonNoHealthyPath    = "no_healthy_path"
	ReasonControllerSwitch = "controller_switch"
)

// Backend is the subset of the simulated array used by the multipath layer.
type Backend interface {
	PathInfos() []sim.PathInfo
	DiskInfos() []sim.DiskInfo
	DiskInfo(id string) (sim.DiskInfo, bool)
	Probe(ctx context.Context, pathID string) error
	DoIO(ctx context.Context, req sim.IORequest) error
	Subscribe(buffer int) (<-chan sim.Event, func())
	OptimalIOSize() int
	BlockSize() int
}

// Config tunes probing, retry and retained history.
type Config struct {
	ProbeInterval   time.Duration
	ProbeTimeout    time.Duration
	IOTimeout       time.Duration
	RequestTimeout  time.Duration
	BusyWait        time.Duration
	MaxBusyWait     time.Duration
	HistoryLimit    int
	SuspensionLimit int
	SwitchLimit     int
	HourRetention   int
	OwnerSettleWait time.Duration
}

// DefaultConfig is suitable for a fast local demo.
func DefaultConfig() Config {
	return Config{
		ProbeInterval:   250 * time.Millisecond,
		ProbeTimeout:    80 * time.Millisecond,
		IOTimeout:       250 * time.Millisecond,
		RequestTimeout:  20 * time.Second,
		BusyWait:        4 * time.Millisecond,
		MaxBusyWait:     40 * time.Millisecond,
		HistoryLimit:    500,
		SuspensionLimit: 500,
		SwitchLimit:     200,
		HourRetention:   24 * 14,
		OwnerSettleWait: 2 * time.Second,
	}
}

// PathSnapshot is the web/API view of one path.
type PathSnapshot struct {
	ID               string      `json:"id"`
	NIC              string      `json:"nic"`
	Switch           string      `json:"switch"`
	Controller       int         `json:"controller"`
	State            string      `json:"state"`
	BasePriority     int         `json:"base_priority"`
	Active           int         `json:"active"`
	Completed        int64       `json:"completed"`
	ReadBytes        int64       `json:"read_bytes"`
	WriteBytes       int64       `json:"write_bytes"`
	BytesTotal       int64       `json:"bytes_total"`
	PathErrors       int64       `json:"path_errors"`
	BusyRetries      int64       `json:"busy_retries"`
	MediaErrors      int64       `json:"media_errors"`
	EWMALatencyMS    float64     `json:"ewma_latency_ms"`
	AverageLatencyMS float64     `json:"average_latency_ms"`
	LastLatencyMS    float64     `json:"last_latency_ms"`
	LastProbeAt      time.Time   `json:"last_probe_at"`
	LastStateChange  time.Time   `json:"last_state_change"`
	LastReason       string      `json:"last_reason"`
	LastDetail       string      `json:"last_detail"`
	LastError        string      `json:"last_error"`
	CurrentHour      *HourBucket `json:"current_hour,omitempty"`
}

// DiskSnapshot exposes online capacity, ownership and space warnings.
type DiskSnapshot struct {
	ID             string         `json:"id"`
	Index          int            `json:"index"`
	Size           int64          `json:"size"`
	BlockSize      int            `json:"block_size"`
	OptimalIOSize  int            `json:"optimal_io_size"`
	Owner          int            `json:"owner_controller"`
	OwnerStable    bool           `json:"owner_stable"`
	Switching      bool           `json:"switching"`
	SpaceAlert     bool           `json:"space_alert"`
	UsedPercent    int            `json:"used_percent"`
	UsedBytes      int64          `json:"used_bytes"`
	FreeBytes      int64          `json:"free_bytes"`
	PathPriorities map[string]int `json:"path_priorities"`
}

// Event is a state change or array notification shown in the UI.
type Event struct {
	ID      uint64    `json:"id"`
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Level   string    `json:"level"`
	PathID  string    `json:"path_id,omitempty"`
	DiskID  string    `json:"disk_id,omitempty"`
	Reason  string    `json:"reason"`
	Message string    `json:"message"`
	From    string    `json:"from_state,omitempty"`
	To      string    `json:"to_state,omitempty"`
}

// SwitchEvent describes one failover/controller/resume switching window.
type SwitchEvent struct {
	ID                uint64    `json:"id"`
	Kind              string    `json:"kind"`
	Status            string    `json:"status"`
	Reason            string    `json:"reason"`
	DiskID            string    `json:"disk_id,omitempty"`
	PathID            string    `json:"path_id,omitempty"`
	Detail            string    `json:"detail,omitempty"`
	StartedAt         time.Time `json:"started_at"`
	EndedAt           time.Time `json:"ended_at,omitempty"`
	DurationMS        int64     `json:"duration_ms"`
	SuspendedCount    int64     `json:"suspended_count"`
	ActiveSuspensions int       `json:"active_suspensions"`
	MaxWaitMS         int64     `json:"max_wait_ms"`
}

// Suspension is one interval during which a chunk/request was held.
type Suspension struct {
	ID         uint64    `json:"id"`
	SwitchID   uint64    `json:"switch_id"`
	RequestID  uint64    `json:"request_id"`
	ChunkIndex int       `json:"chunk_index"`
	DiskID     string    `json:"disk_id"`
	Op         string    `json:"op"`
	Offset     int64     `json:"offset"`
	Length     int       `json:"length"`
	Reason     string    `json:"reason"`
	FailedPath string    `json:"failed_path,omitempty"`
	Status     string    `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	EndedAt    time.Time `json:"ended_at,omitempty"`
	DurationMS int64     `json:"duration_ms"`
}

// HourBucket is hourly per-path usage and error statistics.
type HourBucket struct {
	Hour        string `json:"hour"`
	PathID      string `json:"path_id"`
	IOs         int64  `json:"ios"`
	ReadBytes   int64  `json:"read_bytes"`
	WriteBytes  int64  `json:"write_bytes"`
	BytesTotal  int64  `json:"bytes_total"`
	PathErrors  int64  `json:"path_errors"`
	BusyRetries int64  `json:"busy_retries"`
	MediaErrors int64  `json:"media_errors"`
}

// Summary contains aggregate values for the dashboard header.
type Summary struct {
	PathCount             int    `json:"path_count"`
	PathsUp               int    `json:"paths_up"`
	PathsChecking         int    `json:"paths_checking"`
	PathsDown             int    `json:"paths_down"`
	ActiveRequests        int    `json:"active_requests"`
	TotalCompleted        int64  `json:"total_completed"`
	TotalBytes            int64  `json:"total_bytes"`
	TotalPathErrors       int64  `json:"total_path_errors"`
	TotalBusyRetries      int64  `json:"total_busy_retries"`
	TotalMediaErrors      int64  `json:"total_media_errors"`
	SwitchCount           int64  `json:"switch_count"`
	ActiveSwitches        int    `json:"active_switches"`
	ActiveSuspensions     int    `json:"active_suspensions"`
	TotalSuspensions      int64  `json:"total_suspensions"`
	TotalSuspensionWaitMS int64  `json:"total_suspension_wait_ms"`
	MaxSuspensionWaitMS   int64  `json:"max_suspension_wait_ms"`
	RequestCounter        uint64 `json:"request_counter"`
}

// Alert is an upper-layer warning pushed by the block layer.
type Alert struct {
	At          time.Time `json:"at"`
	DiskID      string    `json:"disk_id"`
	Kind        string    `json:"kind"`
	Level       string    `json:"level"`
	UsedPercent int       `json:"used_percent,omitempty"`
	Message     string    `json:"message"`
}

// Snapshot is the complete API/UI state.
type Snapshot struct {
	GeneratedAt       time.Time      `json:"generated_at"`
	Ready             bool           `json:"ready"`
	Rechecking        bool           `json:"rechecking"`
	RecheckID         uint64         `json:"recheck_id"`
	OptimalIOSize     int            `json:"optimal_io_size"`
	BlockSize         int            `json:"block_size"`
	Summary           Summary        `json:"summary"`
	Paths             []PathSnapshot `json:"paths"`
	Disks             []DiskSnapshot `json:"disks"`
	Events            []Event        `json:"events"`
	Switches          []SwitchEvent  `json:"switches"`
	ActiveSuspensions []Suspension   `json:"active_suspensions"`
	RecentSuspensions []Suspension   `json:"recent_suspensions"`
	Hourly            []HourBucket   `json:"hourly"`
}
