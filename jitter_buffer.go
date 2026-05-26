package main

import (
	"container/heap"
	"sync"
	"time"
)

// RTPPacket represents a single audio frame packet in the buffer.
type RTPPacket struct {
	Sequence  uint16
	Timestamp uint32
	Payload   []byte
}

// packetHeap implements heap.Interface and holds RTPPackets.
// It automatically sorts packets by their Sequence Number to fix out-of-order delivery.
type packetHeap []RTPPacket

func (h packetHeap) Len() int           { return len(h) }
func (h packetHeap) Less(i, j int) bool { return h[i].Sequence < h[j].Sequence }
func (h packetHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push appends an item to the heap slice.
func (h *packetHeap) Push(x interface{}) {
	*h = append(*h, x.(RTPPacket))
}

// Pop removes and returns the minimum element (lowest Sequence Number) from the heap.
func (h *packetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// JitterBuffer reorders incoming UDP/RTP packets and smooths out network jitter.
type JitterBuffer struct {
	mu          sync.Mutex
	packets     packetHeap
	maxSize     int
	delay       time.Duration
	hasStarted  bool
	startStream time.Time
}

// NewJitterBuffer creates an initialized JitterBuffer.
// maxSize: Maximum number of packets allowed in queue before dropping old ones (prevents memory leaks).
// delay: The amount of buffering time to smooth out network variance (e.g., 60ms - 100ms).
func NewJitterBuffer(maxSize int, delay time.Duration) *JitterBuffer {
	jb := &JitterBuffer{
		maxSize: maxSize,
		delay:   delay,
	}
	heap.Init(&jb.packets)
	return jb
}

// Push inserts a packet into the jitter buffer. It handles deep-copying the payload
// so that the underlying UDP reading buffer can be safely reused.
func (jb *JitterBuffer) Push(seq uint16, ts uint32, payload []byte) {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	// If buffer is full, drop the oldest packet in the heap to prevent infinite latency growth
	if jb.packets.Len() >= jb.maxSize {
		heap.Pop(&jb.packets)
	}

	// Create a hard copy of the payload to avoid data corruption from UDP buffer reuse
	dataCopy := make([]byte, len(payload))
	copy(dataCopy, payload)

	// Push packet into the min-heap (automatic sorting)
	heap.Push(&jb.packets, RTPPacket{Sequence: seq, Timestamp: ts, Payload: dataCopy})

	// Track the exact time the first packet arrived to calculate the playback delay
	if !jb.hasStarted {
		jb.hasStarted = true
		jb.startStream = time.Now()
	}
}

// Pop extracts the next sequential packet from the buffer, provided that the 
// initial target delay has passed. Returns false if no packets are ready.
func (jb *JitterBuffer) Pop() (RTPPacket, bool) {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	// Return false if buffer is empty or streaming hasn't officially triggered yet
	if jb.packets.Len() == 0 || !jb.hasStarted {
		return RTPPacket{}, false
	}

	// Enforce the initial jitter delay. Do not release packets until the network 
	// has had enough time to build up a stable cushion of frames.
	if time.Since(jb.startStream) < jb.delay {
		return RTPPacket{}, false
	}

	// Extract the packet with the lowest sequence number
	pkt := heap.Pop(&jb.packets).(RTPPacket)
	return pkt, true
}

// Flush clears all packets currently stored in the buffer and resets the stream state.
func (jb *JitterBuffer) Flush() {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	
	jb.packets = packetHeap{}
	heap.Init(&jb.packets)
	jb.hasStarted = false
}