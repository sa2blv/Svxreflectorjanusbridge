package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/hraban/opus.v2"
)

const (
	SVXSampleRate   = 48000
	SVXFrameSize    = 960  // 20ms at 48kHz
	OpusDecodeRate  = 48000 // decode OPUS at 48kHz
	OpusEncodeRate  = 48000 // encode OPUS at 48kHz
	OpusFrameSize   = 960   // 20ms at 48kHz (same as SVX frame)
	OpusFramesPerPkt = 1    // 1 frame per RTP packet
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println("SVX ↔ UDP/OPUS Bridge starting...")

	// --- Configuration ---
svxHost := envRequired("REFLECTOR_HOST")
    svxPort := envInt("REFLECTOR_PORT", 5300)
    svxAuthKey := envRequired("REFLECTOR_AUTH_KEY")
    svxTG := uint32(envInt("REFLECTOR_TG", 1))
    callsign := envRequired("CALLSIGN")
    nodeLocation := envDefault("NODE_LOCATION", "")
    sysop := envDefault("SYSOP", "")

	// UDP OPUS config (replaces Zello)
	janusURL := envRequired("JANUS_URL") // 👈 Replaces OPUS_SEND_ADDR
	//opusListenAddr := envDefault("OPUS_LISTEN_ADDR", "0.0.0.0:5045")
	opusSendAddr := envRequired("JANUS_URL") // e.g., "44.5.24.206:5045"

	redisURL := os.Getenv("REDIS_URL")

	log.Printf("Config: SVX=%s:%d TG=%d | OPUS listen=%s send=%s", 
		svxHost, svxPort, svxTG, opusListenAddr, opusSendAddr)

	// --- OPUS codecs (all 48kHz, matching SVX) ---
	// Since we're working with 48kHz OPUS directly, decoders/encoders are simpler
	svxDec, err := opus.NewDecoder(OpusDecodeRate, 1)
	if err != nil {
		log.Fatalf("OPUS decoder (SVX) init: %v", err)
	}
	svxEnc, err := opus.NewEncoder(OpusEncodeRate, 1, opus.AppVoIP)
	if err != nil {
		log.Fatalf("OPUS encoder (SVX) init: %v", err)
	}
	svxEnc.SetBitrate(24000)
	svxEnc.SetComplexity(5)

	log.Println("OPUS codecs initialized (48kHz, 1 channel)")

	// --- Shutdown signal ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// --- Reconnect loop ---
	backoff := 2 * time.Second
	maxBackoff := time.Minute

	for {
		err := runBridge(svxHost, svxPort, svxAuthKey, svxTG, callsign, nodeLocation, sysop,
			opusListenAddr, opusSendAddr,
			redisURL, svxDec, svxEnc, sigCh)

		if err == errShutdown {
			log.Println("Goodbye")
			return
		}
		if err != nil {
			log.Printf("Bridge error: %v", err)
		}

		log.Printf("Reconnecting in %s...", backoff)
		select {
		case <-sigCh:
			log.Println("Shutdown during reconnect wait")
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

var errShutdown = fmt.Errorf("shutdown")

func runBridge(
	svxHost string, svxPort int, svxAuthKey string, svxTG uint32, callsign string, nodeLocation string, sysop string,
	opusListenAddr string, opusSendAddr string,
	redisURL string,
	svxDec *opus.Decoder, svxEnc *opus.Encoder,
	sigCh <-chan os.Signal,
) error {

	var (
		svxTalking    bool
		opusTalking   bool
		svxTalkMu     sync.Mutex
		opusTalkMu    sync.Mutex
		// Audio inactivity watchdog for OPUS→SVX direction
		opusAudioTimer *time.Timer
		agcSvxToOpus = NewAGCFromEnv("AGC_SVX_TO_EXT_")
		agcOpusToSvx = NewAGCFromEnv("AGC_EXT_TO_SVX_")
		// Voice bandpass filters
		filterSvxToOpus = NewVoiceFilterFromEnv("FILTER_SVX_TO_EXT_", float64(SVXSampleRate))
		filterOpusToSvx = NewVoiceFilterFromEnv("FILTER_EXT_TO_SVX_", float64(SVXSampleRate))
	)
	const opusStreamWatchdog = 1500 * time.Millisecond

	// --- Redis (optional, for metadata) ---
	var redisCli *RedisClient
	if redisURL != "" {
		rc, err := ParseRedisURL(redisURL)
		if err != nil {
			log.Printf("[Redis] URL parse error: %v (disabled)", err)
		} else if err := rc.Connect(); err != nil {
			log.Printf("[Redis] Connect error: %v (disabled)", err)
		} else {
			redisCli = rc
			log.Println("[Redis] Connected")
		}
	}

	// --- SVX Reflector client ---
	svx := NewSVXLinkClient(svxHost, svxPort, svxAuthKey, callsign, nodeLocation, sysop)
	svx.SetExtraNodeInfo(map[string]interface{}{
		"remoteHost": "opus.udp",
		"links": []map[string]interface{}{
			{"localTg": svxTG, "remoteTg": "opus"},
		},
	})

	// --- OPUS UDP client ---
	opus, err := NewOpusUDPClient(opusListenAddr, opusSendAddr)
	if err != nil {
		return fmt.Errorf("OPUS client init: %w", err)
	}

	// --- SVX → OPUS audio path ---
	// Receive OPUS 48kHz from SVX, re-encode for OPUS UDP (same rate, just repackage)
	svx.SetAudioCallback(func(opusFrame []byte) {
		opusTalkMu.Lock()
		if opusTalking {
			opusTalkMu.Unlock()
			return
		}
		opusTalkMu.Unlock()

		// Since both are 48kHz OPUS, we could forward directly,
		// but decode/re-encode to apply filters and AGC.
		pcm := make([]int16, OpusFrameSize)
		n, err := svxDec.Decode(opusFrame, pcm)
		if err != nil {
			log.Printf("[SVX→OPUS] OPUS decode error: %v", err)
			return
		}
		if n == 0 {
			return
		}
		pcm = pcm[:n]
		filterSvxToOpus.Process(pcm)
		agcSvxToOpus.Process(pcm)

		// Re-encode to OPUS
		opusBuf := make([]byte, 512)
		nn, err := svxEnc.Encode(pcm, opusBuf)
		if err != nil {
			log.Printf("[SVX→OPUS] OPUS encode error: %v", err)
			return
		}

		if err := opus.SendAudio(opusBuf[:nn]); err != nil {
			log.Printf("[SVX→OPUS] SendAudio error: %v", err)
		}
	})

	svx.SetTalkerStartCallback(func(tg uint32, cs string) {
		if tg != svxTG || strings.EqualFold(cs, callsign) {
			return
		}

		svxTalkMu.Lock()
		svxTalking = true
		svxTalkMu.Unlock()

		log.Printf("[SVX→OPUS] Talker start: %s on TG %d", cs, tg)
		filterSvxToOpus.Reset()
		agcSvxToOpus.Reset()

		streamID, err := opus.StartStream()
		if err != nil {
			log.Printf("[SVX→OPUS] StartStream error: %v", err)
		} else {
			log.Printf("[SVX→OPUS] OPUS stream started (id=%d)", streamID)
		}
	})

	svx.SetTalkerStopCallback(func(tg uint32, cs string) {
		if tg != svxTG || strings.EqualFold(cs, callsign) {
			return
		}

		svxTalkMu.Lock()
		svxTalking = false
		svxTalkMu.Unlock()

		log.Printf("[SVX→OPUS] Talker stop: %s", cs)

		if err := opus.StopStream(); err != nil {
			log.Printf("[SVX→OPUS] StopStream error: %v", err)
		}
	})

	// --- OPUS → SVX audio path ---
	// Receive OPUS 48kHz from UDP, decode to PCM, apply filters, send to SVX

	opus.SetStreamStartCallback(func(streamID uint32, senderName string) {
		svxTalkMu.Lock()
		if svxTalking {
			svxTalkMu.Unlock()
			log.Printf("[OPUS→SVX] Ignoring stream %d from %s (SVX is talking)", streamID, senderName)
			return
		}
		svxTalkMu.Unlock()

		opusTalkMu.Lock()
		opusTalking = true
		opusTalkMu.Unlock()

		log.Printf("[OPUS→SVX] Stream start from %s (id=%d)", senderName, streamID)
		filterOpusToSvx.Reset()
		agcOpusToSvx.Reset()
		svx.SendTalkerStart(svxTG, callsign)

		// Publish OPUS caller info to Redis
		if redisCli != nil {
			redisCli.SetEX("opus_rx:"+strings.TrimSpace(callsign), 30, senderName)
		}

		if opusAudioTimer != nil {
			opusAudioTimer.Stop()
		}
		opusAudioTimer = time.AfterFunc(opusStreamWatchdog, func() {
			opusTalkMu.Lock()
			if !opusTalking {
				opusTalkMu.Unlock()
				return
			}
			opusTalking = false
			opusTalkMu.Unlock()
			log.Printf("[OPUS→SVX] Stream watchdog timeout — forcing TalkerStop")
			svx.SendTalkerStop(svxTG, callsign)
			if redisCli != nil {
				redisCli.Del("opus_rx:" + strings.TrimSpace(callsign))
			}
		})
	})

	opus.SetStreamDataCallback(func(streamID uint32, packetID uint32, opusData []byte) {
		svxTalkMu.Lock()
		if svxTalking {
			svxTalkMu.Unlock()
			return
		}
		svxTalkMu.Unlock()

		if opusAudioTimer != nil {
			opusAudioTimer.Reset(opusStreamWatchdog)
		}

		// Decode OPUS to PCM 48kHz
		pcm := make([]int16, OpusFrameSize*2) // allocate enough space
		n, err := svxDec.Decode(opusData, pcm)
		if err != nil {
			log.Printf("[OPUS→SVX] OPUS decode error: %v", err)
			return
		}
		if n == 0 {
			return
		}
		pcm = pcm[:n]
		filterOpusToSvx.Process(pcm)
		agcOpusToSvx.Process(pcm)

		// Send to SVX (already 48kHz, 20ms frames)
		if len(pcm) >= SVXFrameSize {
			chunk := pcm[:SVXFrameSize]
			opusBuf := make([]byte, 512)
			nn, err := svxEnc.Encode(chunk, opusBuf)
			if err != nil {
				log.Printf("[OPUS→SVX] OPUS encode error: %v", err)
				return
			}

			if err := svx.SendAudio(opusBuf[:nn]); err != nil {
				log.Printf("[OPUS→SVX] SendAudio error: %v", err)
			}
		}
	})

	opus.SetStreamStopCallback(func(streamID uint32) {
		if opusAudioTimer != nil {
			opusAudioTimer.Stop()
		}

		opusTalkMu.Lock()
		alreadyStopped := !opusTalking
		opusTalking = false
		opusTalkMu.Unlock()

		log.Printf("[OPUS→SVX] Stream stop (id=%d)", streamID)
		if alreadyStopped {
			return
		}
		svx.SendTalkerStop(svxTG, callsign)

		if redisCli != nil {
			redisCli.Del("opus_rx:" + strings.TrimSpace(callsign))
		}
	})

	// --- Connect both sides ---
	if err := svx.Connect(); err != nil {
		return fmt.Errorf("SVX connect: %w", err)
	}
	if err := svx.SelectTG(svxTG); err != nil {
		svx.Close()
		return fmt.Errorf("SVX SelectTG: %w", err)
	}

	if err := opus.Connect(); err != nil {
		svx.Close()
		return fmt.Errorf("OPUS connect: %w", err)
	}

	// --- Start goroutines ---
	go svx.RunTCPReader()
	go svx.RunTCPHeartbeat()
	go svx.RunUDPReader()
	go svx.RunUDPHeartbeat()
	go opus.RunReader()

	log.Printf("Bridge active: SVX TG %d ↔ OPUS %s ↔ %s", svxTG, opusListenAddr, opusSendAddr)

	// --- Wait for disconnect or shutdown ---
	var result error
	select {
	case <-svx.Done():
		log.Println("SVXReflector connection lost")
		result = fmt.Errorf("SVX connection lost")
	case <-opus.Done():
		log.Println("OPUS connection lost")
		result = fmt.Errorf("OPUS connection lost")
	case <-sigCh:
		log.Println("Shutting down...")
		result = errShutdown
	}

	opus.Close()
	svx.Close()
	if redisCli != nil {
		redisCli.Del("opus_rx:" + strings.TrimSpace(callsign))
		redisCli.Close()
	}

	return result
}

func envRequired(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("Required env var %s is not set", key)
	}
	return v
}

func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		n := 0
		fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			return n
		}
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log.Fatalf("Invalid float for %s: %v", key, err)
		}
		return f
	}
	return fallback
}
