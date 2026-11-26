package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	mpio "southpole/mpio/internal/mpio"
	"southpole/mpio/internal/sim"
)

//go:embed static/*
var staticFS embed.FS

// Server exposes the dashboard and JSON/CSV APIs.
type Server struct {
	Mgr *mpio.Manager
	Arr *sim.Array
}

// New creates the web/API server.
func New(mgr *mpio.Manager, arr *sim.Array) *Server {
	return &Server{Mgr: mgr, Arr: arr}
}

// Handler builds all HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.Mgr.Snapshot())
	})

	mux.HandleFunc("GET /api/hourly", func(w http.ResponseWriter, r *http.Request) {
		pathID := r.URL.Query().Get("path_id")
		format := r.URL.Query().Get("format")
		if format == "json" {
			data, err := s.Mgr.ExportHourlyJSON(pathID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_, _ = w.Write(data)
			return
		}
		data, err := s.Mgr.ExportHourlyCSV(pathID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", "attachment; filename=\"hourly-path-stats.csv\"")
		_, _ = w.Write(data)
	})

	mux.HandleFunc("POST /api/host/resume", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := s.Mgr.Resume(ctx); err != nil {
			writeError(w, http.StatusGatewayTimeout, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	mux.HandleFunc("POST /api/control/path", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			PathID string `json:"path_id"`
			Fault  string `json:"fault"`
		}
		if !decode(w, r, &req) {
			return
		}
		if err := s.Arr.SetPathFault(req.PathID, req.Fault); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	mux.HandleFunc("POST /api/control/path-notify", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			PathID  string `json:"path_id"`
			State   string `json:"state"`
			Message string `json:"message"`
		}
		if !decode(w, r, &req) {
			return
		}
		if err := s.Arr.NotifyPathState(req.PathID, req.State, req.Message); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	mux.HandleFunc("POST /api/control/switch", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Switch string `json:"switch"`
			Fault  string `json:"fault"`
		}
		if !decode(w, r, &req) {
			return
		}
		n, err := s.Arr.SetSwitchFault(req.Switch, req.Fault)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "paths_affected": n})
	})

	mux.HandleFunc("POST /api/control/owner", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DiskID       string `json:"disk_id"`
			Controller   int    `json:"controller"`
			TransitionMS int    `json:"transition_ms"`
		}
		if !decode(w, r, &req) {
			return
		}
		transition := time.Duration(req.TransitionMS) * time.Millisecond
		if transition <= 0 {
			transition = 40 * time.Millisecond
		}
		if err := s.Arr.SetOwner(req.DiskID, req.Controller, transition); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	mux.HandleFunc("POST /api/control/array-error", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DiskID string `json:"disk_id"`
			Busy   int    `json:"busy"`
			Media  int    `json:"media"`
		}
		if !decode(w, r, &req) {
			return
		}
		if req.Busy > 0 {
			if err := s.Arr.InjectBusy(req.DiskID, req.Busy, 5*time.Millisecond); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
		}
		if req.Media > 0 {
			if err := s.Arr.InjectMediaError(req.DiskID, req.Media); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	mux.HandleFunc("POST /api/control/expand", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DiskID string `json:"disk_id"`
			MiB    int64  `json:"mib"`
		}
		if !decode(w, r, &req) {
			return
		}
		if req.MiB <= 0 {
			req.MiB = 1
		}
		info, err := s.Arr.ExpandDisk(req.DiskID, req.MiB<<20)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "size": info.Size})
	})

	mux.HandleFunc("POST /api/control/space", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DiskID      string `json:"disk_id"`
			UsedPercent int    `json:"used_percent"`
		}
		if !decode(w, r, &req) {
			return
		}
		info, err := s.Arr.SetSpacePressure(req.DiskID, req.UsedPercent)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "space_alert": info.SpaceAlert})
	})

	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	mux.Handle("GET /", http.FileServerFS(staticSub))
	return logRequests(mux)
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe(addr string) error {
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return httpServer.ListenAndServe()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Body != nil {
		defer r.Body.Close()
	}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return false
	}
	return true
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path != "/api/state" {
			fmt.Printf("%s %s %dms\n", r.Method, r.URL.Path, time.Since(start).Milliseconds())
		}
	})
}
