package khaos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// -----------------------------------------------------------------------
// Wire types
// -----------------------------------------------------------------------

type BatchRequestItem struct {
	SeqID         uint64       `json:"seq_id"`
	OperationType string       `json:"operation_type"` // "READ" | "WRITE" | "DELETE"
	RiderID       string       `json:"rider_id"`
	Payload       *RiderUpdate `json:"payload,omitempty"` // required only for WRITE
}

type BatchResponseItem struct {
	SeqID  uint64 `json:"seq_id"`
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// -----------------------------------------------------------------------
// Server Config & Struct
// -----------------------------------------------------------------------

type Server struct {
	resolver     HazardResolver
	storage      StorageEngine
	workerCount  int
	batchTimeout time.Duration
	httpServer   *http.Server
}

type ServerConfig struct {
	Addr         string        // e.g. ":8080"
	WorkerCount  int           // workers per batch; default 8
	BatchTimeout time.Duration // per-batch execution budget; default 5s
	ReadTimeout  time.Duration // default 10s
	WriteTimeout time.Duration // default 15s
	IdleTimeout  time.Duration // default 60s
	MaxBodyBytes int64         // default 2 MiB
}

const (
	defaultWorkerCount  = 8
	defaultBatchTimeout = 5 * time.Second
	defaultReadTimeout  = 10 * time.Second
	defaultWriteTimeout = 15 * time.Second
	defaultIdleTimeout  = 60 * time.Second
	defaultMaxBodyBytes = 2 << 20 // 2 MiB
)

// NewServer constructs a Server backed by the provided StorageEngine.
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

func (s *Server) withMaxBody(limit int64, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		next(w, r)
	}
}

// -----------------------------------------------------------------------
// Request -> Operation translation
// -----------------------------------------------------------------------

func translateRequest(items []BatchRequestItem) ([]Operation, error) {
	n := len(items)
	ops := make([]Operation, n)
	seen := make([]bool, n)

	for _, item := range items {
		if item.SeqID >= uint64(n) {
			return nil, fmt.Errorf("seq_id %d is out of range for a batch of size %d", item.SeqID, n)
		}
		if seen[item.SeqID] {
			return nil, fmt.Errorf("duplicate seq_id %d", item.SeqID)
		}
		seen[item.SeqID] = true

		opType, err := parseOperationType(item.OperationType)
		if err != nil {
			return nil, fmt.Errorf("seq_id %d: %w", item.SeqID, err)
		}
		if item.RiderID == "" {
			return nil, fmt.Errorf("seq_id %d: rider_id is required", item.SeqID)
		}

		op := Operation{SeqID: item.SeqID, Key: item.RiderID, Type: opType}

		if opType == OpWrite {
			if item.Payload == nil {
				return nil, fmt.Errorf("seq_id %d: payload is required for a WRITE operation", item.SeqID)
			}
			op.Value = *item.Payload
		}

		ops[item.SeqID] = op
	}

	for i, wasSeen := range seen {
		if !wasSeen {
			return nil, fmt.Errorf("missing seq_id %d: batch must be dense and zero-indexed", i)
		}
	}

	return ops, nil
}

func parseOperationType(raw string) (OperationType, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "READ":
		return OpRead, nil
	case "WRITE":
		return OpWrite, nil
	case "DELETE":
		return OpDelete, nil
	default:
		return 0, fmt.Errorf("unknown operation_type %q (must be READ, WRITE, or DELETE)", raw)
	}
}

// -----------------------------------------------------------------------
// Handler
// -----------------------------------------------------------------------

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	var items []BatchRequestItem
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&items); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}

	if len(items) == 0 {
		writeJSON(w, http.StatusOK, []BatchResponseItem{})
		return
	}

	ops, err := translateRequest(items)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	all, runnable, err := s.resolver.Build(ops)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("failed to build dependency graph: %v", err))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.batchTimeout)
	defer cancel()

	rob := NewInOrderReorderBuffer()
	results := rob.Drain(ctx, len(all))

	pool := NewConcurrentWorkerPool(rob, s.workerCount)

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- pool.Run(ctx, s.storage, runnable)
	}()

	responses := make([]BatchResponseItem, 0, len(all))

drainLoop:
	for {
		select {
		case op, ok := <-results:
			if !ok {
				break drainLoop
			}
			item := BatchResponseItem{SeqID: op.SeqID, Result: op.Result}
			if op.Err != nil {
				item.Error = op.Err.Error()
			}
			responses = append(responses, item)

		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				writeError(w, http.StatusGatewayTimeout,
					fmt.Sprintf("batch execution exceeded %s timeout", s.batchTimeout))
			} else {
				log.Printf("khaos: batch request context ended early: %v", ctx.Err())
			}
			return
		}
	}

	if err := <-runErrCh; err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("worker pool error: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, responses)
}

// -----------------------------------------------------------------------
// Response helpers
// -----------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("khaos: failed to encode JSON response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: message})
}

// -----------------------------------------------------------------------
// Lifecycle: start and graceful shutdown
// -----------------------------------------------------------------------

func (s *Server) Run(ctx context.Context) error {
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- s.httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErrCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("khaos: http server error: %w", err)

	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("khaos: graceful shutdown failed: %w", err)
		}
		if err := <-serveErrCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("khaos: http server error during shutdown: %w", err)
		}
		return nil
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("khaos: http server shutdown: %w", err)
	}
	if closer, ok := s.storage.(*PostgresStorageEngine); ok {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("khaos: closing storage engine: %w", err)
		}
	}
	return nil
}

var _ interface{ Close() error } = (*PostgresStorageEngine)(nil)