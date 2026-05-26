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
	SVXSampleRate    = 48000
	SVXFrameSize     = 960   // 20ms at 48kHz
	OpusDecodeRate   = 48000 // decode OPUS at 48kHz
	OpusEncodeRate   = 48000 // encode OPUS at 48kHz
	OpusFrameSize    = 960   // 20ms at 48kHz (same as SVX frame)
	OpusFramesPerPkt = 1     // 1 frame per RTP packet
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println("SVX <-> UDP/OPUS Bridge starting...")

	// --- Configuration ---
	svxHost := envRequired("REFLECTOR_HOST")
	svxPort := envInt("REFLECTOR_PORT", 5300)
	svxAuthKey := envRequired("REFLECTOR_AUTH_KEY")
	svxTG := uint32(envInt("REFLECTOR_TG", 1))
	callsign := envRequired("CALLSIGN")
	nodeLocation := envDefault("NODE_LOCATION", "")
	sysop := envDefault("SYSOP", "")

	// UDP OPUS config (Janus pipeline)
	opusListenAddr := envDefault("OPUS_LISTEN_ADDR", "0.0.0.0:5045")
	opusSendAddr := envRequired("JANUS_URL") // e.g., "44.5.24.206:5045"

	redisURL := os.Getenv("REDIS_URL")

	log.Printf("Config: SVX=%s:%d TG=%d | OPUS listen=%s send=%s", 
		svxHost, svxPort, svxTG, opusListenAddr, opusSendAddr)

	// --- OPUS codecs initialized for target layout (48kHz, Stereo) ---
	svxDec, err := opus.NewDecoder(OpusDecodeRate, 2) // Changed from 1 to 2 (Stereo)
	if err != nil {
		log.Fatalf("OPUS decoder (SVX) init: %v", err)
	}
	svxEnc, err := opus.NewEncoder(OpusEncodeRate, 2, opus.AppVoIP) // Changed from 1 to 2 (Stereo)
	if err != nil {
		log.Fatalf("OPUS encoder (SVX) init: %v", err)
	}
	svxEnc.SetBitrate(24000)
	svxEnc.SetComplexity(5)

	log.Println("OPUS codecs initialized (48kHz, 2 channels - Stereo)")

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
		svxTalking      bool
		svxTalkMu       sync.Mutex
		agcSvxToOpus    = NewAGCFromEnv("AGC_SVX_TO_EXT_")
		agcOpusToSvx    = NewAGCFromEnv("AGC_EXT_TO_SVX_")
		filterSvxToOpus = NewVoiceFilterFromEnv("FILTER_SVX_TO_EXT_", float64(SVXSampleRate))
		filterOpusToSvx = NewVoiceFilterFromEnv("FILTER_EXT_TO_SVX_", float64(SVXSampleRate))
	)

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

	// --- SVX -> OPUS audio path ---
	svx.SetAudioCallback(func(opusFrame []byte) {
		// Process incoming audio frames from SVX Reflector 
		// Decode to PCM (Stereo format), apply conditioning, re-encode, and push to Janus
		pcm := make([]int16, OpusFrameSize*2) // Account for stereo channels space
		n, err := svxDec.Decode(opusFrame, pcm)
		if err != nil {
			log.Printf("[SVX->OPUS] OPUS decode error: %v", err)
			return
		}
		if n == 0 {
			return
		}
		pcm = pcm[:n*2] // Multiply length by 2 to respect the stereo layout channel pairs
		filterSvxToOpus.Process(pcm)
		agcSvxToOpus.Process(pcm)

		opusBuf := make([]byte, 1024)
		nn, err := svxEnc.Encode(pcm, opusBuf)
		if err != nil {
			log.Printf("[SVX->OPUS] OPUS encode error: %v", err)
			return
		}

		// Feeds the network encoder stream. If silent, client logic manages comfort packet insertions.
		if err := opus.SendAudio(opusBuf[:nn]); err != nil {
			log.Printf("[SVX->OPUS] SendAudio error: %v", err)
		}
	})

	svx.SetTalkerStartCallback(func(tg uint32, cs string) {
		if tg != svxTG || strings.EqualFold(cs, callsign) {
			return
		}

		svxTalkMu.Lock()
		svxTalking = true
		svxTalkMu.Unlock()

		log.Printf("[SVX->OPUS] Talker start: %s on TG %d", cs, tg)
		filterSvxToOpus.Reset()
		agcSvxToOpus.Reset()
	})

	svx.SetTalkerStopCallback(func(tg uint32, cs string) {
		if tg != svxTG || strings.EqualFold(cs, callsign) {
			return
		}

		svxTalkMu.Lock()
		svxTalking = false
		svxTalkMu.Unlock()

		log.Printf("[SVX->OPUS] Talker stop: %s", cs)
	})

	// --- OPUS -> SVX audio path ---
	opus.SetStreamStartCallback(func(streamID uint32, senderName string) {
		log.Printf("[OPUS->SVX] Audio sequence initializing from Janus (%s)", senderName)
		filterOpusToSvx.Reset()
		agcOpusToSvx.Reset()
		svx.SendTalkerStart(svxTG, callsign)

		if redisCli != nil {
			redisCli.SetEX("opus_rx:"+strings.TrimSpace(callsign), 30, senderName)
		}
	})

	opus.SetStreamDataCallback(func(streamID uint32, packetID uint32, opusData []byte) {
		svxTalkMu.Lock()
		if svxTalking {
			svxTalkMu.Unlock()
			return // Avoid looping local microphone audio loops back down into SVX 
		}
		svxTalkMu.Unlock()

		// Decode OPUS stream down to standard PCM 48kHz Stereo 
		pcm := make([]int16, OpusFrameSize*2)
		n, err := svxDec.Decode(opusData, pcm)
		if err != nil {
			log.Printf("[OPUS->SVX] OPUS decode error: %v", err)
			return
		}
		if n == 0 {
			return
		}
		pcm = pcm[:n*2]
		filterOpusToSvx.Process(pcm)
		agcOpusToSvx.Process(pcm)

		if len(pcm) >= SVXFrameSize*2 {
			chunk := pcm[:SVXFrameSize*2]
			opusBuf := make([]byte, 1024)
			nn, err := svxEnc.Encode(chunk, opusBuf)
			if err != nil {
				log.Printf("[OPUS->SVX] OPUS encode error: %v", err)
				return
			}

			if err := svx.SendAudio(opusBuf[:nn]); err != nil {
				log.Printf("[OPUS->SVX] SendAudio error: %v", err)
			}
		}
	})

	opus.SetStreamStopCallback(func(streamID uint32) {
		log.Printf("[OPUS->SVX] Audio sequence ended from Janus side")
		svx.SendTalkerStop(svxTG, callsign)

		if redisCli != nil {
			redisCli.Del("opus_rx:" + strings.TrimSpace(callsign))
		}
	})

	// --- Connect connections and register continuous streams ---
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

	// Immediately initialize continuous delivery session so Janus websocket never hits timeout
	if _, err := opus.StartStream(); err != nil {
		log.Printf("[OPUS] Initial continuous stream spin-up failed: %v", err)
	}

	// --- Start engine background processing loops ---
	go svx.RunTCPReader()
	go svx.RunTCPHeartbeat()
	go svx.RunUDPReader()
	go svx.RunUDPHeartbeat()
	go opus.RunReader()

	log.Printf("Bridge active: SVX TG %d <-> OPUS %s <-> %s", svxTG, opusListenAddr, opusSendAddr)

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