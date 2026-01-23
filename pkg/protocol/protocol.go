package protocol

import "strings"

const (
	CmdRegister = "REGISTER"
	CmdForward  = "FORWARD"
	CmdData     = "DATA"
	CmdSync     = "SYNC"
	CmdPeerID   = "PEER_ID" // New command to identify peer
)

type Registration struct {
	Name     string
	Backend  string
	PeerID   string // Optional: peer identifier for better visibility
	PeerAddr string // Optional: peer's bind address for DATA connections
}

func TrimLine(line string) string {
	return strings.TrimSpace(line)
}

func FormatData(proxyID string) string {
	return CmdData + ":" + proxyID + "\n"
}

func ParseDataLine(line string) (proxyID string, ok bool) {
	line = TrimLine(line)
	if !strings.HasPrefix(line, CmdData+":") {
		return "", false
	}
	proxyID = strings.TrimSpace(strings.TrimPrefix(line, CmdData+":"))
	return proxyID, proxyID != ""
}

func FormatForward(proxyID, name, backendAddr string) string {
	return CmdForward + ":" + proxyID + ":" + name + ":" + backendAddr + "\n"
}

func ParseForwardLine(line string) (proxyID, name, backendAddr string, ok bool) {
	line = TrimLine(line)
	if !strings.HasPrefix(line, CmdForward+":") {
		return "", "", "", false
	}
	parts := strings.SplitN(line, ":", 4)
	if len(parts) != 4 {
		return "", "", "", false
	}
	proxyID = strings.TrimSpace(parts[1])
	name = strings.TrimSpace(parts[2])
	backendAddr = strings.TrimSpace(parts[3])
	if proxyID == "" || name == "" || backendAddr == "" {
		return "", "", "", false
	}
	return proxyID, name, backendAddr, true
}

func FormatRegister(regs []Registration) string {
	parts := make([]string, 0, len(regs))
	for _, r := range regs {
		if strings.TrimSpace(r.Name) == "" || strings.TrimSpace(r.Backend) == "" {
			continue
		}
		parts = append(parts, strings.TrimSpace(r.Name)+":"+strings.TrimSpace(r.Backend))
	}
	return CmdRegister + ":" + strings.Join(parts, ",") + "\n"
}

// FormatRegisterWithPeerID formats REGISTER command with peer ID and peer address
// Format: REGISTER:peer_id:peer_addr:name1:backend1,name2:backend2,...
// peer_addr is the peer's bind address (e.g., "10.0.1.2:6443") for DATA connections
func FormatRegisterWithPeerID(peerID, peerAddr string, regs []Registration) string {
	if peerID == "" {
		return FormatRegister(regs)
	}
	parts := make([]string, 0, len(regs))
	for _, r := range regs {
		if strings.TrimSpace(r.Name) == "" || strings.TrimSpace(r.Backend) == "" {
			continue
		}
		parts = append(parts, strings.TrimSpace(r.Name)+":"+strings.TrimSpace(r.Backend))
	}
	// Include peer_addr in the protocol
	return CmdRegister + ":" + peerID + ":" + peerAddr + ":" + strings.Join(parts, ",") + "\n"
}

func ParseRegisterLine(line string) (regs []Registration, ok bool) {
	line = TrimLine(line)
	if !strings.HasPrefix(line, CmdRegister+":") {
		return nil, false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(line, CmdRegister+":"))
	if rest == "" {
		return nil, true
	}

	// Try to parse new format: REGISTER:peer_id:peer_addr:name1:backend1,...
	// Check if it starts with peer_id:peer_addr prefix
	var peerID, peerAddr string
	var servicesPart string
	
	// Look for pattern: starts with at least 3 colons before first comma
	commaIdx := strings.Index(rest, ",")
	checkPart := rest
	if commaIdx > 0 {
		checkPart = rest[:commaIdx]
	}
	
	colonCount := strings.Count(checkPart, ":")
	if colonCount >= 3 {
		// New format: REGISTER:peer_id:peer_addr:name:backend,...
		parts := strings.SplitN(rest, ":", 3)
		if len(parts) >= 3 {
			peerID = strings.TrimSpace(parts[0])
			peerAddr = strings.TrimSpace(parts[1])
			servicesPart = parts[2]
		} else {
			servicesPart = rest
		}
	} else if colonCount >= 2 {
		// Medium format: REGISTER:peer_id:name:backend,...
		parts := strings.SplitN(rest, ":", 2)
		peerID = strings.TrimSpace(parts[0])
		peerAddr = ""  // No peer_addr in this format
		servicesPart = parts[1]
	} else {
		// Old format: REGISTER:name:backend,...
		peerID = ""
		peerAddr = ""
		servicesPart = rest
	}

	items := strings.Split(servicesPart, ",")
	regs = make([]Registration, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		idx := strings.Index(item, ":")
		if idx == -1 {
			// malformed
			continue
		}
		name := strings.TrimSpace(item[:idx])
		backend := strings.TrimSpace(item[idx+1:])
		regs = append(regs, Registration{
			Name:     name,
			Backend:  backend,
			PeerID:   peerID,
			PeerAddr: peerAddr,
		})
	}
	return regs, true
}

// FormatSync formats a SYNC command with service list
func FormatSync(regs []Registration) string {
	parts := make([]string, 0, len(regs))
	for _, r := range regs {
		if strings.TrimSpace(r.Name) == "" || strings.TrimSpace(r.Backend) == "" {
			continue
		}
		parts = append(parts, strings.TrimSpace(r.Name)+":"+strings.TrimSpace(r.Backend))
	}
	return CmdSync + ":" + strings.Join(parts, ",") + "\n"
}

// FormatSyncWithPeerID formats SYNC command with peer ID and peer address
// Format: SYNC:peer_id:peer_addr:name1:backend1,name2:backend2,...
// peer_addr is the peer's bind address for DATA connections
func FormatSyncWithPeerID(peerID, peerAddr string, regs []Registration) string {
	if peerID == "" {
		return FormatSync(regs)
	}
	parts := make([]string, 0, len(regs))
	for _, r := range regs {
		if strings.TrimSpace(r.Name) == "" || strings.TrimSpace(r.Backend) == "" {
			continue
		}
		parts = append(parts, strings.TrimSpace(r.Name)+":"+strings.TrimSpace(r.Backend))
	}
	return CmdSync + ":" + peerID + ":" + peerAddr + ":" + strings.Join(parts, ",") + "\n"
}

// ParseSyncLine parses a SYNC command line
// Supports old format (SYNC:name:backend,...) and new format (SYNC:peer_id:peer_addr:name:backend,...)
func ParseSyncLine(line string) (regs []Registration, ok bool) {
	line = TrimLine(line)
	if !strings.HasPrefix(line, CmdSync+":") {
		return nil, false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(line, CmdSync+":"))
	if rest == "" {
		return nil, true
	}

	// Try to parse new format: SYNC:peer_id:peer_addr:name1:backend1,...
	var peerID, peerAddr string
	var servicesPart string
	
	// Look for pattern: starts with at least 3 colons before first comma
	commaIdx := strings.Index(rest, ",")
	checkPart := rest
	if commaIdx > 0 {
		checkPart = rest[:commaIdx]
	}
	
	colonCount := strings.Count(checkPart, ":")
	if colonCount >= 3 {
		// New format: SYNC:peer_id:peer_addr:name:backend,...
		parts := strings.SplitN(rest, ":", 3)
		if len(parts) >= 3 {
			peerID = strings.TrimSpace(parts[0])
			peerAddr = strings.TrimSpace(parts[1])
			servicesPart = parts[2]
		} else {
			servicesPart = rest
		}
	} else if colonCount >= 2 {
		// Medium format: SYNC:peer_id:name:backend,...
		parts := strings.SplitN(rest, ":", 2)
		peerID = strings.TrimSpace(parts[0])
		servicesPart = parts[1]
	} else {
		// Old format: SYNC:name:backend,...
		servicesPart = rest
	}

	items := strings.Split(servicesPart, ",")
	regs = make([]Registration, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		idx := strings.Index(item, ":")
		if idx == -1 {
			// malformed
			continue
		}
		name := strings.TrimSpace(item[:idx])
		backend := strings.TrimSpace(item[idx+1:])
		regs = append(regs, Registration{
			Name:     name,
			Backend:  backend,
			PeerID:   peerID,
			PeerAddr: peerAddr,
		})
	}
	return regs, true
}
