package khaos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	defaultWorkerCount  = 8
	defaultBatchTimeout = 5 * time.Second
	defaultReadTimeout  = 30 * time.Second
	defaultWriteTimeout = 30 * time.Second
	defaultIdleTimeout  = 60 * time.Second
	defaultMaxBodyBytes = int64(10 << 20)
)

// ServerConfig configures the HTTP listener and execution defaults for the batch API.
type ServerConfig struct {
	Addr         string
	WorkerCount  int
	BatchTimeout time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	MaxBodyBytes int64
}

// Server exposes the batch endpoint and wraps the Khaos execution pipeline.
type Server struct {
	httpServer   *http.Server
	resolver     HazardResolver
	storage      StorageEngine
	workerCount  int
	batchTimeout time.Duration
}

func NewServer(cfg ServerConfig, storage StorageEngine) *Server {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = defaultWorkerCount
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = defaultBatchTimeout
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = defaultReadTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = defaultMaxBodyBytes
	}

	s := &Server{
		resolver:     NewSequentialHazardResolver(),
		storage:      storage,
		workerCount:  cfg.WorkerCount,
		batchTimeout: cfg.BatchTimeout,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/riders/batch", s.withMaxBody(cfg.MaxBodyBytes, s.handleBatch))

	s.httpServer = &http.Server{
		Addr:         cfg.Addr,
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	return s
}

func (s *Server) Run(ctx context.Context) error {
	if s == nil || s.httpServer == nil {
		return nil
	}

	go func() {
		<-ctx.Done()
		_ = s.httpServer.Close()
	}()

	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) withMaxBody(limit int64, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if limit > 0 && r.ContentLength > limit {
			http.Error(w, fmt.Sprintf("request body exceeds %d bytes", limit), http.StatusRequestEntityTooLarge)
			return
		}
		if limit > 0 {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		next(w, r)
	}
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Operations []Operation `json:"operations"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.Operations) == 0 {
		http.Error(w, "no operations provided", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"accepted": true,
		"count":    len(req.Operations),
	}); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode response: %v", err), http.StatusInternalServerError)
	}
}
