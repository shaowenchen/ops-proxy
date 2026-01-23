package server

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var proxyProtoV2Sig = []byte{0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a}

// stripProxyProtocol removes HAProxy PROXY protocol header (v1/v2) if present.
// Returns payload bytes (header stripped) and an optional debug info string.
// It may read additional bytes from conn to complete the header.
func stripProxyProtocol(conn net.Conn, _ interface{ GetReadTimeout() time.Duration }, initial []byte, readTimeout time.Duration) (payload []byte, info string) {
	b := append([]byte(nil), initial...)

	readMore := func(need int) bool {
		if need <= 0 {
			return true
		}
		buf := make([]byte, need)
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		n, err := conn.Read(buf)
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil || n <= 0 {
			return false
		}
		b = append(b, buf[:n]...)
		return true
	}

	// PROXY protocol v1: "PROXY TCP4 1.1.1.1 2.2.2.2 123 456\r\n"
	if bytes.HasPrefix(b, []byte("PROXY ")) {
		// Ensure we have a full line.
		for i := 0; i < 4 && !bytes.Contains(b, []byte("\r\n")) && len(b) < 4096; i++ {
			if !readMore(256) {
				break
			}
		}
		idx := bytes.Index(b, []byte("\r\n"))
		if idx == -1 {
			return b, "proxyproto=v1 incomplete=true"
		}
		line := string(b[:idx])
		rest := b[idx+2:]

		parts := strings.Fields(line)
		// parts: PROXY TCP4 src dst sport dport
		if len(parts) >= 6 {
			info = fmt.Sprintf("proxyproto=v1 src=%s:%s dst=%s:%s", parts[2], parts[4], parts[3], parts[5])
		} else {
			info = "proxyproto=v1 parsed=false"
		}
		return rest, info
	}

	// PROXY protocol v2: binary header starts with signature.
	if len(b) >= len(proxyProtoV2Sig) && bytes.Equal(b[:len(proxyProtoV2Sig)], proxyProtoV2Sig) {
		// Need at least 16 bytes fixed header.
		for len(b) < 16 {
			if !readMore(16 - len(b)) {
				break
			}
		}
		if len(b) < 16 {
			return b, "proxyproto=v2 incomplete=true"
		}

		// length is 2 bytes at offset 14
		l := int(b[14])<<8 | int(b[15])
		total := 16 + l
		for len(b) < total && len(b) < 4096 {
			if !readMore(total - len(b)) {
				break
			}
		}
		if len(b) < total {
			return b, "proxyproto=v2 incomplete=true"
		}

		// Parse address info for INET/INET6 if possible.
		verCmd := b[12]
		famProto := b[13]
		_ = verCmd
		src := ""
		dst := ""
		if (famProto>>4)&0x0F == 0x1 && l >= 12 { // AF_INET
			// addr: 4+4+2+2 = 12 bytes
			srcIP := net.IP(b[16 : 16+4]).String()
			dstIP := net.IP(b[20 : 20+4]).String()
			srcPort := int(b[24])<<8 | int(b[25])
			dstPort := int(b[26])<<8 | int(b[27])
			src = srcIP + ":" + strconv.Itoa(srcPort)
			dst = dstIP + ":" + strconv.Itoa(dstPort)
		} else if (famProto>>4)&0x0F == 0x2 && l >= 36 { // AF_INET6
			srcIP := net.IP(b[16 : 16+16]).String()
			dstIP := net.IP(b[32 : 32+16]).String()
			srcPort := int(b[48])<<8 | int(b[49])
			dstPort := int(b[50])<<8 | int(b[51])
			src = net.JoinHostPort(srcIP, strconv.Itoa(srcPort))
			dst = net.JoinHostPort(dstIP, strconv.Itoa(dstPort))
		}
		if src != "" && dst != "" {
			info = fmt.Sprintf("proxyproto=v2 src=%s dst=%s", src, dst)
		} else {
			info = "proxyproto=v2 parsed=true"
		}
		return b[total:], info
	}

	return b, ""
}
