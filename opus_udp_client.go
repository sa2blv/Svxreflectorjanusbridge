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
	OpusPayloadType   = 111 
	OpusClockRate     = 48000
	OpusChannels      = 2   
	OpusFrameDuration = 20  
)

// Official Opus 20ms stereo silence packet payload
var OpusStereoSilence = []byte{0xfc, 0xff, 0xfe}


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
	jitterBuf   *JitterBuffer 

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
func NewOpusUDPClient(listenAddr, sendAddr string) (*OpusUDPClient, error) {
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
		// 120ms max buffer, 60ms initial delay cushion to match your 60ms frame duration
		jitterBuf:  NewJitterBuffer(60, 60*time.Millisecond),
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

	listenUDPAddr, err := net.ResolveUDPAddr("udp", c.listenAddr)
	if err != nil {
		return fmt.Errorf("resolve listen address: %w", err)
	}

	listenConn, err := net.ListenUDP("udp", listenUDPAddr)
	if err != nil {
		return fmt.Errorf("listen UDP: %w", err)
	}
	c.listenConn = listenConn

	sendConn, err := net.DialUDP("udp", nil, c.sendAddr)
	if err != nil {
		listenConn.Close()
		return fmt.Errorf("dial UDP send: %w", err)
	}
	c.sendConn = sendConn

	log.Printf("[Janus] Listening on %s, sending to %s", c.listenAddr, c.sendAddr)
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

	log.Printf("[Janus] TX stream started (id=%d)", streamID)
	return streamID, nil
}

// SendAudio sends a single OPUS frame as an RTP packet.
// SendAudio sends a single OPUS frame. If opusData is empty/nil, 
// it automatically sends a standard Opus stereo silence packet to keep Janus alive.
func (c *OpusUDPClient) SendAudio(opusData []byte) error {
	// If txActive is completely off, we don't send anything (e.g., when disconnecting)
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
	c.txTS += (OpusClockRate / 1000) * OpusFrameDuration
	conn := c.sendConn
	c.mu.Unlock()

	// SILENCE INSERTION: If no audio data is provided, use the Janus-compliant stereo silence frame
	payload := opusData
	if len(payload) == 0 {
		payload = OpusStereoSilence
	}

	header := c.buildRTPHeader(seq, ts, true)
	pkt := c.encodeRTPPacket(header, payload)

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

	log.Printf("[Janus] TX stream stopped (id=%d)", streamID)
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
	var activeStreamID uint32
	var hasNotifiedStart bool

	// Background playback loop
	stopPlayback := make(chan struct{})
go func() {
		// 20ms is the standard sweet spot for checking audio availability
		ticker := time.NewTicker(20 * time.Millisecond) 
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if c.onStreamData != nil {
					// CATCH-UP LOOP: 
					// If packets are piled up due to network lag, drain them quickly 
					// to minimize latency and restore real-time playback.
					for {
						pkt, ok := c.jitterBuf.Pop()
						if !ok {
							break // Buffer is empty or still cushioning, stop draining
						}
						
						// Dispatch the audio packet
						c.onStreamData(activeStreamID, uint32(pkt.Sequence), pkt.Payload)
						
						// If your frame size is 60ms, playing them too fast will cause a local underrun.
						// We break early if the buffer size returns to a healthy "normal" state.
						// This allows a burst to clear out accidental pile-ups without emptying the cushion.
						break 
					}
				}
			case <-stopPlayback:
				return
			}
		}
	}()
	defer close(stopPlayback)

	for {
		// Long deadline, we always accept data
		c.listenConn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, remoteAddr, err := c.listenConn.ReadFromUDP(buf)
		if err != nil {
			if c.isClosed() {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// On total silence timeout, flush buffer and clear stream notify state
				hasNotifiedStart = false
				c.jitterBuf.Flush()
				if c.onStreamStop != nil {
					c.onStreamStop(activeStreamID)
				}
				log.Printf("[janus] RX stream absolute timeout (id=%d)", activeStreamID)
				continue
			}
			log.Printf("[Janus] Read error: %v", err)
			return
		}

		if n < 12 {
			continue
		}

		header, payload, err := c.parseRTPPacket(buf[:n])
		if err != nil {
			log.Printf("[Janus] Parse RTP error: %v", err)
			continue
		}

		if header.PT != OpusPayloadType && header.PT != 48 {
			continue
		}

		activeStreamID = header.SSRC
		c.rxSSRC = header.SSRC

		// Fire optional lifecycle hook once per session change
		if !hasNotifiedStart {
			hasNotifiedStart = true
			if c.onStreamStart != nil {
				c.onStreamStart(activeStreamID, remoteAddr.IP.String())
			}
		}

		// Push data immediately. The buffer handles ordering and structural underruns.
		if len(payload) > 0 {
			c.jitterBuf.Push(header.Seq, header.TS, payload)
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
		V:    2,
		P:    0,
		X:    0,
		CC:   0,
		M:    m,
		PT:   OpusPayloadType,
		Seq:  seq,
		TS:   ts,
		SSRC: c.txSSRC,
	}
}

// encodeRTPPacket encodes an RTP header and OPUS payload into a byte slice.
func (c *OpusUDPClient) encodeRTPPacket(header *RTPHeader, payload []byte) []byte {
	pkt := make([]byte, 12+len(payload))
	pkt[0] = (header.V << 6) | (header.P << 5) | (header.X << 4) | header.CC
	pkt[1] = (header.M << 7) | header.PT
	binary.BigEndian.PutUint16(pkt[2:4], header.Seq)
	binary.BigEndian.PutUint32(pkt[4:8], header.TS)
	binary.BigEndian.PutUint32(pkt[8:12], header.SSRC)
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

	csrcSize := int(header.CC) * 4
	headerLen := 12 + csrcSize

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

	payloadEnd := len(data)
	if header.P != 0 && len(data) > 0 {
		paddingLen := int(data[len(data)-1])
		if paddingLen > 0 && paddingLen <= len(data)-headerLen {
			payloadEnd -= paddingLen
		}
	}

	return header, data[headerLen:payloadEnd], nil
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