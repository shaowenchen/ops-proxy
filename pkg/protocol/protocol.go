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
	Name    string
	Backend string
	PeerID  string // Optional: peer identifier for better visibility
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

// FormatRegisterWithPeerID formats REGISTER command with peer ID
// Format: REGISTER:peer_id:name1:backend1,name2:backend2,...
func FormatRegisterWithPeerID(peerID string, regs []Registration) string {
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
	return CmdRegister + ":" + peerID + ":" + strings.Join(parts, ",") + "\n"
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

	// Try to parse with peer_id first (new format): REGISTER:peer_id:name1:backend1,...
	// Check if first part before first comma contains exactly 2 colons (peer_id:name:backend pattern)
	commaIdx := strings.Index(rest, ",")
	var peerID string
	var servicesPart string
	
	if commaIdx == -1 {
		// Single service, check colon count
		colonCount := strings.Count(rest, ":")
		if colonCount >= 2 {
			// New format with peer_id
			parts := strings.SplitN(rest, ":", 2)
			peerID = strings.TrimSpace(parts[0])
			servicesPart = parts[1]
		} else {
			// Old format without peer_id
			servicesPart = rest
		}
	} else {
		firstService := rest[:commaIdx]
		colonCount := strings.Count(firstService, ":")
		if colonCount >= 2 {
			// New format: extract peer_id from first part
			parts := strings.SplitN(rest, ":", 2)
			peerID = strings.TrimSpace(parts[0])
			servicesPart = parts[1]
		} else {
			// Old format
			servicesPart = rest
		}
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
		regs = append(regs, Registration{Name: name, Backend: backend, PeerID: peerID})
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

// FormatSyncWithPeerID formats SYNC command with peer ID
// Format: SYNC:peer_id:name1:backend1,name2:backend2,...
func FormatSyncWithPeerID(peerID string, regs []Registration) string {
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
	return CmdSync + ":" + peerID + ":" + strings.Join(parts, ",") + "\n"
}

// ParseSyncLine parses a SYNC command line
// Supports both old format (SYNC:name:backend,...) and new format (SYNC:peer_id:name:backend,...)
func ParseSyncLine(line string) (regs []Registration, ok bool) {
	line = TrimLine(line)
	if !strings.HasPrefix(line, CmdSync+":") {
		return nil, false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(line, CmdSync+":"))
	if rest == "" {
		return nil, true
	}

	// Try to parse with peer_id first (new format): SYNC:peer_id:name1:backend1,...
	commaIdx := strings.Index(rest, ",")
	var peerID string
	var servicesPart string
	
	if commaIdx == -1 {
		// Single service, check colon count
		colonCount := strings.Count(rest, ":")
		if colonCount >= 2 {
			// New format with peer_id
			parts := strings.SplitN(rest, ":", 2)
			peerID = strings.TrimSpace(parts[0])
			servicesPart = parts[1]
		} else {
			// Old format without peer_id
			servicesPart = rest
		}
	} else {
		firstService := rest[:commaIdx]
		colonCount := strings.Count(firstService, ":")
		if colonCount >= 2 {
			// New format: extract peer_id from first part
			parts := strings.SplitN(rest, ":", 2)
			peerID = strings.TrimSpace(parts[0])
			servicesPart = parts[1]
		} else {
			// Old format
			servicesPart = rest
		}
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
		regs = append(regs, Registration{Name: name, Backend: backend, PeerID: peerID})
	}
	return regs, true
}
