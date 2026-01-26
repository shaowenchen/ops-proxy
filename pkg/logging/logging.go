package logging

import (
	"fmt"
	"log"
	"os"
	"sync"
)

var (
	peerID     string
	peerIDOnce sync.Once
	
	// Async logging channel and worker
	logChan   chan string
	logWorker sync.Once
	logWg     sync.WaitGroup
	logMu     sync.Mutex
)

// initLogWorker starts the async log worker goroutine
func initLogWorker() {
	logMu.Lock()
	defer logMu.Unlock()
	
	logWorker.Do(func() {
		// Create buffered channel to avoid blocking
		// Buffer size: 1000 messages
		logChan = make(chan string, 1000)
		
		logWg.Add(1)
		go func() {
			defer logWg.Done()
			for msg := range logChan {
				log.Print(msg)
			}
		}()
	})
}

// GetPeerID returns the unique peer ID for this instance
func GetPeerID() string {
	peerIDOnce.Do(func() {
		// Try PEER_ID first (allows fixed peer ID), then POD_NAME, then HOSTNAME, then generate a short ID
		peerID = os.Getenv("PEER_ID")
		if peerID == "" {
			peerID = os.Getenv("POD_NAME")
		}
		if peerID == "" {
			peerID = os.Getenv("HOSTNAME")
		}
		if peerID == "" {
			// Generate a short ID from hostname or use a default
			hostname, _ := os.Hostname()
			if hostname != "" {
				// Use last 8 chars of hostname as fallback
				if len(hostname) > 8 {
					peerID = hostname[len(hostname)-8:]
				} else {
					peerID = hostname
				}
			} else {
				peerID = "unknown"
			}
		}
	})
	return peerID
}

// Logf logs a formatted message (async, non-blocking)
func Logf(format string, v ...interface{}) {
	initLogWorker()
	msg := fmt.Sprintf(format, v...)
	
	// Non-blocking send: if channel is full, drop the log message
	select {
	case logChan <- msg:
	default:
		// Channel is full, log directly to avoid blocking (fallback to sync logging)
		log.Print(msg)
	}
}

// Log logs a message (async, non-blocking)
func Log(v ...interface{}) {
	initLogWorker()
	msg := fmt.Sprint(v...)
	
	// Non-blocking send: if channel is full, drop the log message
	select {
	case logChan <- msg:
	default:
		// Channel is full, log directly to avoid blocking (fallback to sync logging)
		log.Print(msg)
	}
}

// Fatalf logs a fatal error and exits (synchronous for fatal errors)
func Fatalf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Fatalf("%s", msg)
}

// Flush waits for all pending log messages to be written
func Flush() {
	logMu.Lock()
	defer logMu.Unlock()
	
	if logChan != nil {
		close(logChan)
		logWg.Wait()
		logChan = nil
		logWorker = sync.Once{}
	}
}
