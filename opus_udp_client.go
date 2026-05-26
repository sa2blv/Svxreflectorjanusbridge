package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// RTP constants for OPUS
	OpusPayloadType   = 111 // RFC 7587 dynamic payload type
	OpusClockRate     = 48000
	OpusChannels      = 1
	OpusFrameDuration = 60 // milliseconds (20ms frame in RTP terms)
)

// RTPHeader represents an RTP packet header.
type RTPHeader struct {
	V        uint8  // Version (2 bits)
	P        uint8  // Padding (1 bit)
	X        uint8  // Extension (1 bit)
	CC       uint8  // CSRC count (4 bits)
	M        uint8  // Marker (1 bit)
	PT       uint8  // Payload type (7 bits)
	Seq      uint16 // Sequence number
	TS       uint32 // Timestamp
	SSRC     uint32 // Synchronization source
}

// OpusUDPClient manages bidirectional OPUS/RTP audio over UDP.
// Sends to a remote address and receives from a local listening port.
type OpusUDPClient struct {
	// Receive config
	listenAddr string
	listenConn *net.UDPConn

	// Send config
	sendAddr   *net.UDPAddr
	sendConn   *net.UDPConn

	// RX state
	rxSSRC     uint32
	rxTimestamp uint32

	// TX state
	txSSRC    uint32
	txSeq     uint16
	txTS      uint32
	txActive  atomic.Bool
	txStreamID uint32

	// Callbacks
	onStreamStart func(streamID uint32, senderName string)
	onStreamData  func(streamID uint32, packetID uint32, data []byte)
	onStreamStop  func(streamID uint32)

	done   chan struct{}
	mu     sync.Mutex
	closed bool
}

// NewOpusUDPClient creates a new UDP OPUS/RTP client.
// listenAddr: local bind address (e.g., "0.0.0.0:5045")
// sendAddr: remote send address (e.g., "44.5.24.206:5045")
func NewOpusUDPClient(listenAddr, sendAddr string) (*OpusUDPClient, error) {
	// Parse send address
	remoteAddr, err := net.ResolveUDPAddr("udp", sendAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve send address: %w", err)
	}

	return &OpusUDPClient{
		listenAddr: listenAddr,
		sendAddr:   remoteAddr,
		txSSRC:     uint32(time.Now().UnixNano()),
		txSeq:      uint16(time.Now().UnixNano()),
		txTS:       0,
	}, nil
}

func (c *OpusUDPClient) SetStreamStartCallback(cb func(streamID uint32, senderName string)) {
	c.onStreamStart = cb
}

func (c *OpusUDPClient) SetStreamDataCallback(cb func(streamID uint32, packetID uint32, data []byte)) {
	c.onStreamData = cb
}

func (c *OpusUDPClient) SetStreamStopCallback(cb func(streamID uint32)) {
	c.onStreamStop = cb
}

func (c *OpusUDPClient) Done() <-chan struct{} {
	return c.done
}

// Connect binds to the listen address and opens send connection.
func (c *OpusUDPClient) Connect() error {
	c.mu.Lock()
	c.closed = false
	c.done = make(chan struct{})
	c.mu.Unlock()

	// Bind listening UDP socket
	listenUDPAddr, err := net.ResolveUDPAddr("udp", c.listenAddr)
	if err != nil {
		return fmt.Errorf("resolve listen address: %w", err)
	}

	listenConn, err := net.ListenUDP("udp", listenUDPAddr)
	if err != nil {
		return fmt.Errorf("listen UDP: %w", err)
	}
	c.listenConn = listenConn

	// Open sending UDP socket
	sendConn, err := net.DialUDP("udp", nil, c.sendAddr)
	if err != nil {
		listenConn.Close()
		return fmt.Errorf("dial UDP send: %w", err)
	}
	c.sendConn = sendConn

	log.Printf("[OpusUDP] Listening on %s, sending to %s", c.listenAddr, c.sendAddr)
	return nil
}

// StartStream begins a new outgoing audio stream.
func (c *OpusUDPClient) StartStream() (uint32, error) {
	c.mu.Lock()
	if c.sendConn == nil {
		c.mu.Unlock()
		return 0, fmt.Errorf("not connected")
	}
	c.mu.Unlock()

	c.txActive.Store(true)
	streamID := uint32(time.Now().UnixNano() & 0xFFFFFFFF)
	c.txStreamID = streamID
	c.txSeq = uint16(time.Now().UnixNano() & 0xFFFF)
	c.txTS = 0

	log.Printf("[OpusUDP] TX stream started (id=%d)", streamID)
	return streamID, nil
}

// SendAudio sends a single OPUS frame as an RTP packet.
func (c *OpusUDPClient) SendAudio(opusData []byte) error {
	if !c.txActive.Load() {
		return nil
	}

	c.mu.Lock()
	if c.sendConn == nil {
		c.mu.Unlock()
		return fmt.Errorf("not connected")
	}

	seq := c.txSeq
	ts := c.txTS
	c.txSeq++
	c.txTS += (OpusClockRate / 1000) * OpusFrameDuration // increment by frame duration in clock units
	conn := c.sendConn
	c.mu.Unlock()

	// Build RTP header
	header := c.buildRTPHeader(seq, ts, true) // marker=true for each packet
	pkt := c.encodeRTPPacket(header, opusData)

	_, err := conn.Write(pkt)
	return err
}

// StopStream ends the outgoing audio stream.
func (c *OpusUDPClient) StopStream() error {
	c.txActive.Store(false)
	c.mu.Lock()
	streamID := c.txStreamID
	c.txStreamID = 0
	c.mu.Unlock()

	log.Printf("[OpusUDP] TX stream stopped (id=%d)", streamID)
	return nil
}

// RunReader reads incoming RTP/OPUS packets and dispatches events.
func (c *OpusUDPClient) RunReader() {
	defer func() {
		c.mu.Lock()
		if !c.closed {
			c.closed = true
			close(c.done)
		}
		c.mu.Unlock()
	}()

	buf := make([]byte, 4096)
	streamActive := false
	var activeStreamID uint32

	for {
		c.listenConn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, remoteAddr, err := c.listenConn.ReadFromUDP(buf)
		if err != nil {
			if c.isClosed() {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Handle timeout: if stream was active, send stop event
				if streamActive {
					streamActive = false
					if c.onStreamStop != nil {
						c.onStreamStop(activeStreamID)
					}
					log.Printf("[OpusUDP] RX stream timeout (id=%d)", activeStreamID)
				}
				continue
			}
			log.Printf("[OpusUDP] Read error: %v", err)
			return
		}

		// Parse RTP packet
		if n < 12 {
			continue // RTP header is at least 12 bytes
		}

		header, payload, err := c.parseRTPPacket(buf[:n])
		if err != nil {
			log.Printf("[OpusUDP] Parse RTP error: %v", err)
			continue
		}

		// Check for payload type (OPUS = 111, or check if different)
		if header.PT != OpusPayloadType && header.PT != 48 { // 48 is sometimes used for OPUS
			continue
		}

		// Track stream state
		if !streamActive {
			streamActive = true
			activeStreamID = header.SSRC
			c.rxSSRC = header.SSRC
			if c.onStreamStart != nil {
				c.onStreamStart(activeStreamID, remoteAddr.IP.String())
			}
			log.Printf("[OpusUDP] RX stream start: id=%d from=%s", activeStreamID, remoteAddr.IP)
		}

		// Reset timeout on each packet
		c.listenConn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// Send audio data
		if c.onStreamData != nil && len(payload) > 0 {
			c.onStreamData(activeStreamID, uint32(header.Seq), payload)
		}
	}
}

// buildRTPHeader builds an RTP header with given sequence, timestamp, and marker.
func (c *OpusUDPClient) buildRTPHeader(seq uint16, ts uint32, marker bool) *RTPHeader {
	m := uint8(0)
	if marker {
		m = 1
	}
	return &RTPHeader{
		V:    2,                 // RTP version 2
		P:    0,                 // No padding
		X:    0,                 // No extension
		CC:   0,                 // No CSRC
		M:    m,                 // Marker
		PT:   OpusPayloadType,   // Payload type for OPUS
		Seq:  seq,
		TS:   ts,
		SSRC: c.txSSRC,
	}
}

// encodeRTPPacket encodes an RTP header and OPUS payload into a byte slice.
func (c *OpusUDPClient) encodeRTPPacket(header *RTPHeader, payload []byte) []byte {
	pkt := make([]byte, 12+len(payload))

	// Byte 0: V(2), P(1), X(1), CC(4)
	pkt[0] = (header.V << 6) | (header.P << 5) | (header.X << 4) | header.CC

	// Byte 1: M(1), PT(7)
	pkt[1] = (header.M << 7) | header.PT

	// Bytes 2-3: Sequence number
	binary.BigEndian.PutUint16(pkt[2:4], header.Seq)

	// Bytes 4-7: Timestamp
	binary.BigEndian.PutUint32(pkt[4:8], header.TS)

	// Bytes 8-11: SSRC
	binary.BigEndian.PutUint32(pkt[8:12], header.SSRC)

	// Payload
	copy(pkt[12:], payload)

	return pkt
}

// parseRTPPacket decodes an RTP packet and returns the header and payload.
func (c *OpusUDPClient) parseRTPPacket(data []byte) (*RTPHeader, []byte, error) {
	if len(data) < 12 {
		return nil, nil, fmt.Errorf("packet too short")
	}

	header := &RTPHeader{
		V:    (data[0] >> 6) & 0x3,
		P:    (data[0] >> 5) & 0x1,
		X:    (data[0] >> 4) & 0x1,
		CC:   data[0] & 0xF,
		M:    (data[1] >> 7) & 0x1,
		PT:   data[1] & 0x7F,
		Seq:  binary.BigEndian.Uint16(data[2:4]),
		TS:   binary.BigEndian.Uint32(data[4:8]),
		SSRC: binary.BigEndian.Uint32(data[8:12]),
	}

	// Skip CSRC list (4 bytes per CSRC)
	csrcSize := int(header.CC) * 4
	headerLen := 12 + csrcSize

	// Skip extension if present
	if header.X != 0 {
		if len(data) < headerLen+4 {
			return nil, nil, fmt.Errorf("packet too short for extension")
		}
		extLen := int(binary.BigEndian.Uint16(data[headerLen+2:headerLen+4])) * 4
		headerLen += 4 + extLen
	}

	if len(data) < headerLen {
		return nil, nil, fmt.Errorf("packet too short for headers")
	}

	// Skip padding if present
	payloadEnd := len(data)
	if header.P != 0 && len(data) > 0 {
		paddingLen := int(data[len(data)-1])
		if paddingLen > 0 && paddingLen <= len(data)-headerLen {
			payloadEnd -= paddingLen
		}
	}

	payload := data[headerLen:payloadEnd]
	return header, payload, nil
}

func (c *OpusUDPClient) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Close closes both listening and sending connections.
func (c *OpusUDPClient) Close() {
	c.mu.Lock()
	wasClosed := c.closed
	c.closed = true
	if !wasClosed && c.done != nil {
		close(c.done)
	}
	c.mu.Unlock()

	if c.listenConn != nil {
		c.listenConn.Close()
	}
	if c.sendConn != nil {
		c.sendConn.Close()
	}
}
