// reorder.go
package khaos

import (
	"context"
	"sync"
)

// InOrderReorderBuffer buffers out-of-order operations until their sequence
// numbers become contiguous. A mutex protects the pending map, next expected
// value, and the ordered flush loop so output stays strictly sequential.
type InOrderReorderBuffer struct {
	mu           sync.Mutex
	pending      map[uint64]Operation
	nextExpected uint64
	flushed      int
	total        int
	out          chan Operation
	closed       bool
	ctx          context.Context
}

// NewInOrderReorderBuffer creates an empty buffer. Call Drain once before the
// first Commit so the batch size and cancellation context are set.
func NewInOrderReorderBuffer() *InOrderReorderBuffer {
	return &InOrderReorderBuffer{
		pending: make(map[uint64]Operation),
		out:     make(chan Operation),
		ctx:     context.Background(),
	}
}

// Drain records the total number of items and the cancellation context, then
// returns the output channel. Empty batches close the channel immediately.
func (rb *InOrderReorderBuffer) Drain(ctx context.Context, total int) <-chan Operation {
	rb.mu.Lock()
	rb.ctx = ctx
	rb.total = total
	if total == 0 && !rb.closed {
		rb.closed = true
		close(rb.out)
	}
	rb.mu.Unlock()
	return rb.out
}

// Commit stores the operation in pending and flushes any contiguous run that
// starts at nextExpected. The lock covers both the map update and the send loop
// so only one goroutine can emit ordered results at a time.
//
// This keeps the output strictly ordered even when multiple workers commit at
// once. A slow consumer creates backpressure by blocking the send under the
// lock, and ctx cancellation stops a blocked send without leaking goroutines.
func (rb *InOrderReorderBuffer) Commit(node *DAGNode) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.pending[node.SeqID] = node.Operation

	for {
		op, ok := rb.pending[rb.nextExpected]
		if !ok {
			break
		}
		delete(rb.pending, rb.nextExpected)

		select {
		case rb.out <- op:
			rb.nextExpected++
			rb.flushed++
			if rb.flushed == rb.total && !rb.closed {
				rb.closed = true
				close(rb.out)
			}
		case <-rb.ctx.Done():
			return
		}
	}
}

// Compile-time interface check.
var _ ReorderBuffer = (*InOrderReorderBuffer)(nil)
