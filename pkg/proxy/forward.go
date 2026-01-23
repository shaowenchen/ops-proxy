package proxy

import (
	"bytes"
	"net"
	"strings"
)

// BufferedConn connection wrapper that supports re-reading
type BufferedConn struct {
	net.Conn
	Buf []byte
	Pos int
}

// Read implements io.Reader interface
func (bc *BufferedConn) Read(b []byte) (n int, err error) {
	if bc.Pos < len(bc.Buf) {
		n = copy(b, bc.Buf[bc.Pos:])
		bc.Pos += n
		return n, nil
	}
	return bc.Conn.Read(b)
}

// DetectProtocolAndRoute detects protocol type and routes
func DetectProtocolAndRoute(data []byte, getDefaultClient func() string, extractHost func([]byte) string, extractSNI func([]byte) string) (protocol, clientName string) {
	// Try to detect HTTP/HTTPS
	if len(data) >= 4 {
		// Check for HTTP methods
		maxLen := 7
		if len(data) < maxLen {
			maxLen = len(data)
		}
		method := string(data[:maxLen])
		if len(method) >= 7 && method[:7] == "CONNECT" {
			// HTTP CONNECT (forward proxy)
			protocol = "http_connect"
			// Route by host (without port) for observability; server may choose an egress client instead.
			if host, _, ok := ExtractConnectHostPort(data); ok {
				clientName = host
			}
			return
		}
		if len(method) >= 3 && (method[:3] == "GET" || method[:4] == "POST" || method[:4] == "PUT" ||
			method[:6] == "DELETE" || method[:4] == "HEAD" || method[:7] == "OPTIONS") {
			// HTTP protocol - parse Host header
			protocol = "http"
			clientName = extractHost(data)
			return
		}

		// Check for TLS handshake (HTTPS)
		if len(data) >= 5 && data[0] == 0x16 && data[1] == 0x03 {
			// TLS ClientHello - extract SNI
			protocol = "https"
			clientName = extractSNI(data)
			return
		}
	}

	// Default to TCP
	protocol = "tcp"
	// For TCP, we need to use a different routing mechanism
	// For now, use the first available client or a default routing
	clientName = getDefaultClient()
	return
}

// FindHTTPEndOfHeaders finds the end position of HTTP headers (\r\n\r\n).
// Returns the index of the first byte of the delimiter, or -1 if not found.
func FindHTTPEndOfHeaders(data []byte) int {
	return bytes.Index(data, []byte("\r\n\r\n"))
}

// ExtractConnectHostPort parses the CONNECT authority from the first request line:
// "CONNECT host:port HTTP/1.1".
// Returns host (without port), port, ok.
func ExtractConnectHostPort(data []byte) (host string, port string, ok bool) {
	// Read the first line only (up to \r\n)
	s := string(data)
	lineEnd := strings.Index(s, "\r\n")
	if lineEnd == -1 {
		lineEnd = len(s)
	}
	line := s[:lineEnd]
	if !strings.HasPrefix(line, "CONNECT ") {
		return "", "", false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(line, "CONNECT "))
	parts := strings.SplitN(rest, " ", 2)
	if len(parts) == 0 {
		return "", "", false
	}
	authority := strings.TrimSpace(parts[0])
	if authority == "" {
		return "", "", false
	}
	h, p, err := net.SplitHostPort(authority)
	if err != nil {
		return "", "", false
	}
	if h == "" || p == "" {
		return "", "", false
	}
	return h, p, true
}

// ExtractHostFromHTTP extracts Host header from HTTP data
func ExtractHostFromHTTP(data []byte) string {
	// Simple HTTP header parsing
	headerStr := string(data)
	lines := strings.Split(headerStr, "\r\n")
	for _, line := range lines {
		if strings.HasPrefix(strings.ToLower(line), "host:") {
			host := strings.TrimSpace(line[5:])
			// Remove port if present
			if idx := strings.Index(host, ":"); idx != -1 {
				host = host[:idx]
			}
			return host
		}
	}
	return ""
}

// ExtractSNI extracts SNI from TLS ClientHello data
// TLS handshake format:
// [0] = 0x16 (Handshake)
// [1-2] = Version (0x03XX)
// [3-4] = Length
// [5] = Handshake Type (0x01 = ClientHello)
// [6-8] = Handshake Length
// [9-10] = Version
// [11-42] = Random (32 bytes)
// [43] = Session ID Length
// ... Session ID
// ... Cipher Suites
// ... Compression Methods
// ... Extensions (where SNI is)
func ExtractSNI(data []byte) string {
	if len(data) < 5 {
		return ""
	}

	// Check TLS handshake record
	if data[0] != 0x16 { // Handshake
		return ""
	}

	// Get record length
	if len(data) < 5 {
		return ""
	}
	recordLen := int(data[3])<<8 | int(data[4])
	if len(data) < 5+recordLen {
		// Not enough data, but try to parse what we have
	}

	// Check handshake type (should be ClientHello = 0x01)
	if len(data) < 6 || data[5] != 0x01 {
		return ""
	}

	// Get handshake length (for validation, but not used in parsing)
	if len(data) < 9 {
		return ""
	}
	_ = int(data[6])<<16 | int(data[7])<<8 | int(data[8]) // handshakeLen, not used but validates structure

	// Skip to ClientHello body
	// Offset 9-10: Version (skip 2 bytes)
	// Offset 11-42: Random (skip 32 bytes)
	// Offset 43: Session ID Length
	offset := 43
	if len(data) < offset+1 {
		return ""
	}

	// Skip Session ID
	sessionIDLen := int(data[offset])
	offset += 1 + sessionIDLen
	if len(data) < offset+2 {
		return ""
	}

	// Skip Cipher Suites (2 bytes length + cipher suites)
	cipherSuiteLen := int(data[offset])<<8 | int(data[offset+1])
	offset += 2 + cipherSuiteLen
	if len(data) < offset+1 {
		return ""
	}

	// Skip Compression Methods (1 byte length + methods)
	compressionLen := int(data[offset])
	offset += 1 + compressionLen
	if len(data) < offset+2 {
		return ""
	}

	// Now we're at Extensions
	// Extensions Length (2 bytes)
	extensionsLen := int(data[offset])<<8 | int(data[offset+1])
	offset += 2
	extensionsEnd := offset + extensionsLen

	if len(data) < extensionsEnd {
		// Not enough data, but try to parse what we have
		extensionsEnd = len(data)
	}

	// Parse extensions to find SNI (extension type 0x0000)
	for offset < extensionsEnd-4 {
		if len(data) < offset+4 {
			break
		}
		extType := int(data[offset])<<8 | int(data[offset+1])
		extLen := int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4

		if extType == 0x0000 { // Server Name Indication
			if len(data) < offset+2 {
				break
			}
			// SNI list length (2 bytes, skip it)
			offset += 2
			if len(data) < offset+3 {
				break
			}
			// SNI entry type (1 byte, should be 0x00 = host_name)
			if data[offset] != 0x00 {
				break
			}
			offset += 1
			// SNI hostname length (2 bytes)
			hostnameLen := int(data[offset])<<8 | int(data[offset+1])
			offset += 2
			if len(data) < offset+hostnameLen {
				break
			}
			// Extract hostname
			hostname := string(data[offset : offset+hostnameLen])
			return hostname
		}

		offset += extLen
	}

	return ""
}
