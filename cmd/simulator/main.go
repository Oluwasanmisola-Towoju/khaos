package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type riderUpdatePayload struct {
	OrderID       string  `json:"order_id"`
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	CurrentStatus string  `json:"current_status"`
	ETAMinutes    int     `json:"eta_minutes"`
}

type batchRequestItem struct {
	SeqID         uint64              `json:"seq_id"`
	OperationType string              `json:"operation_type"`
	RiderID       string              `json:"rider_id"`
	Payload       *riderUpdatePayload `json:"payload,omitempty"`
}

var riderStatuses = []string{"AT_VENDOR", "EN_ROUTE", "ARRIVED", "DELIVERED"}

type metrics struct {
	requestsSent     atomic.Int64
	successCount     atomic.Int64 // HTTP 200
	serverErrorCount atomic.Int64 // HTTP 5xx
	clientErrorCount atomic.Int64 // HTTP 4xx
	timeoutCount     atomic.Int64 // context deadline / client timeout
	networkErrCount  atomic.Int64 // connection refused, DNS failure, etc.

	statusMu    sync.Mutex
	statusCodes map[int]int64

	latenciesMu sync.Mutex
	latencies   []time.Duration
}

func newMetrics() *metrics {
	return &metrics{statusCodes: make(map[int]int64)}
}

func (m *metrics) recordStatus(code int, latency time.Duration) {
	m.statusMu.Lock()
	m.statusCodes[code]++
	m.statusMu.Unlock()

	m.latenciesMu.Lock()
	m.latencies = append(m.latencies, latency)
	m.latenciesMu.Unlock()

	switch {
	case code == http.StatusOK:
		m.successCount.Add(1)
	case code >= 500:
		m.serverErrorCount.Add(1)
	case code >= 400:
		m.clientErrorCount.Add(1)
	}
}

// -----------------------------------------------------------------------
// Workload generation
// -----------------------------------------------------------------------
func buildBatch(rng *rand.Rand, size int, clientID int) []batchRequestItem {
	batch := make([]batchRequestItem, size)
	for i := 0; i < size; i++ {
		batch[i] = batchRequestItem{
			SeqID:         uint64(i),
			OperationType: "WRITE",
			RiderID: fmt.Sprintf("00000000-0000-0000-0000-%012d", clientID*1000+i),
			Payload: &riderUpdatePayload{
				OrderID:       fmt.Sprintf("11111111-1111-1111-1111-%012d", clientID*1000+i),
				Latitude:      -90 + rng.Float64()*180,
				Longitude:     -180 + rng.Float64()*360,
				CurrentStatus: riderStatuses[rng.Intn(len(riderStatuses))],
				ETAMinutes:    rng.Intn(60),
			},
		}
	}
	return batch
}

// -----------------------------------------------------------------------
// Client goroutine
// -----------------------------------------------------------------------
func runClient(ctx context.Context, httpClient *http.Client, url string, batchSize int, clientID int, m *metrics) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)))
	batch := buildBatch(rng, batchSize, clientID)

	body, err := json.Marshal(batch)
	if err != nil {
		// treat it as a network-class error for reporting purposes rather than silently dropping the request from the metrics entirely.
		log.Printf("[client %3d] FATAL: failed to marshal request body: %v", clientID, err)
		m.networkErrCount.Add(1)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.Printf("[client %3d] FATAL: failed to construct request: %v", clientID, err)
		m.networkErrCount.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := httpClient.Do(req)
	latency := time.Since(start)

	m.requestsSent.Add(1)

	if err != nil {
		if isTimeout(err) {
			log.Printf("[client %3d] TIMEOUT after %s: %v", clientID, latency.Round(time.Millisecond), err)
			m.timeoutCount.Add(1)
		} else {
			log.Printf("[client %3d] NETWORK ERROR after %s: %v", clientID, latency.Round(time.Millisecond), err)
			m.networkErrCount.Add(1)
		}
		return
	}
	defer resp.Body.Close()
	
	_, _ = io.Copy(io.Discard, resp.Body)

	m.recordStatus(resp.StatusCode, latency)

	if resp.StatusCode >= 500 {
		log.Printf("[client %3d] HTTP %d after %s", clientID, resp.StatusCode, latency.Round(time.Millisecond))
	}
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// -----------------------------------------------------------------------
// main
// -----------------------------------------------------------------------

func main() {
	var (
		targetURL      string
		concurrency    int
		batchSize      int
		requestTimeout time.Duration
	)

	flag.StringVar(&targetURL, "url", "http://localhost:8080/api/v1/riders/batch", "Khaos API Gateway batch endpoint URL")
	flag.IntVar(&concurrency, "concurrency", 100, "number of concurrent simulated clients (goroutines)")
	flag.IntVar(&batchSize, "batch-size", 50, "number of rider updates per batch request")
	flag.DurationVar(&requestTimeout, "request-timeout", 10*time.Second, "per-request timeout")
	flag.Parse()

	fmt.Printf("Khaos Chaos Simulator\n")
	fmt.Printf("  target:            %s\n", targetURL)
	fmt.Printf("  concurrent clients: %d\n", concurrency)
	fmt.Printf("  batch size:        %d updates/request\n", batchSize)
	fmt.Printf("  total updates:     %d\n", concurrency*batchSize)
	fmt.Printf("  request timeout:   %s\n", requestTimeout)
	fmt.Println()

	httpClient := &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        concurrency,
			MaxIdleConnsPerHost: concurrency,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	m := newMetrics()

	overallCtx, cancel := context.WithTimeout(context.Background(), requestTimeout+5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(concurrency)

	start := time.Now()
	for i := 0; i < concurrency; i++ {
		clientID := i
		go func() {
			defer wg.Done()
			runClient(overallCtx, httpClient, targetURL, batchSize, clientID, m)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	printReport(m, elapsed, concurrency, batchSize)

	// Non-zero exit if the run wasn't a clean sweep of 200s, so this can be wired into CI or a deploy gate as a pass/fail signal, not just a human-readable report.
	if m.successCount.Load() != int64(concurrency) {
		os.Exit(1)
	}
}

// -----------------------------------------------------------------------
// Reporting
// -----------------------------------------------------------------------

func printReport(m *metrics, elapsed time.Duration, concurrency, batchSize int) {
	totalRequests := m.requestsSent.Load()
	success := m.successCount.Load()
	serverErrors := m.serverErrorCount.Load()
	clientErrors := m.clientErrorCount.Load()
	timeouts := m.timeoutCount.Load()
	networkErrs := m.networkErrCount.Load()

	var successRate float64
	if totalRequests > 0 {
		successRate = (float64(success) / float64(totalRequests)) * 100
	}

	totalUpdates := int64(concurrency) * int64(batchSize)

	p50, p95, p99 := percentiles(m.latencies)

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("        SIMULATION SUMMARY REPORT")
	fmt.Println("========================================")
	fmt.Printf("  Total Time:          %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Requests Sent:       %d\n", totalRequests)
	fmt.Printf("  Rider Updates Sent:  %d (%d clients x %d riders)\n", totalUpdates, concurrency, batchSize)
	fmt.Printf("  Throughput:          %.1f requests/sec, %.1f updates/sec\n",
		float64(totalRequests)/elapsed.Seconds(), float64(totalUpdates)/elapsed.Seconds())
	fmt.Println("----------------------------------------")
	fmt.Printf("  HTTP 200 (Success):  %d\n", success)
	fmt.Printf("  HTTP 5xx (Server):   %d\n", serverErrors)
	fmt.Printf("  HTTP 4xx (Client):   %d\n", clientErrors)
	fmt.Printf("  Timeouts:            %d\n", timeouts)
	fmt.Printf("  Network Errors:      %d\n", networkErrs)
	fmt.Printf("  Success Rate:        %.1f%%\n", successRate)
	fmt.Println("----------------------------------------")
	fmt.Printf("  Latency p50:         %s\n", p50.Round(time.Millisecond))
	fmt.Printf("  Latency p95:         %s\n", p95.Round(time.Millisecond))
	fmt.Printf("  Latency p99:         %s\n", p99.Round(time.Millisecond))
	fmt.Println("========================================")

	if serverErrors > 0 || timeouts > 0 || networkErrs > 0 {
		fmt.Println()
		fmt.Println("Non-success responses were recorded (see log lines above for per-client detail).")
	} else if success == int64(concurrency) {
		fmt.Println()
		fmt.Println("Clean sweep: every concurrent batch completed with HTTP 200.")
	}
}

// percentiles computes p50/p95/p99 over latencies via a full sort. This
// is a reporting step that runs exactly once, after all load generation
// has finished with an O(N log N) sort over, at most, `concurrency` samples
// (hundreds, not millions) costs nothing measurable here and keeps the
// implementation simple and obviously correct, unlike a streaming
// approximation which would trade accuracy for a performance benefit
// this call site doesn't need.
func percentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idx := func(p float64) int {
		i := int(p * float64(len(sorted)-1))
		if i < 0 {
			i = 0
		}
		if i >= len(sorted) {
			i = len(sorted) - 1
		}
		return i
	}
	return sorted[idx(0.50)], sorted[idx(0.95)], sorted[idx(0.99)]
}