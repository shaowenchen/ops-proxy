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
)

// GetPeerID returns the unique peer ID for this instance
func GetPeerID() string {
	peerIDOnce.Do(func() {
		// Try POD_NAME first, then HOSTNAME, then generate a short ID
		peerID = os.Getenv("POD_NAME")
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

// Logf logs a formatted message with peer ID prefix
func Logf(format string, v ...interface{}) {
	peerID := GetPeerID()
	msg := fmt.Sprintf(format, v...)
	log.Printf("[peer=%s] %s", peerID, msg)
}

// Log logs a message with peer ID prefix
func Log(v ...interface{}) {
	peerID := GetPeerID()
	msg := fmt.Sprint(v...)
	log.Printf("[peer=%s] %s", peerID, msg)
}

// Fatalf logs a fatal error with peer ID prefix and exits
func Fatalf(format string, v ...interface{}) {
	peerID := GetPeerID()
	msg := fmt.Sprintf(format, v...)
	log.Fatalf("[peer=%s] %s", peerID, msg)
}
