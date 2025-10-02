package lib

import (
	"errors"
	"fmt"
	"github.com/iancoleman/strcase"
	"strings"
)

// FormatSourcePeer parses a Peer string of the form
//
//	Peer{MConn{<addr>} <peerID> <direction>}
//
// and returns "<peerID>@<addr>".
func FormatSourcePeer(s string) (string, error) {
	const prefix = "Peer{MConn{"
	const suffix = "}"

	// 1. Basic validation
	if !strings.HasPrefix(s, prefix) || !strings.HasSuffix(s, suffix) {
		return "", fmt.Errorf("invalid format, must start with %q and end with %q", prefix, suffix)
	}

	// 2. Strip prefix and trailing "}"
	body := s[len(prefix) : len(s)-len(suffix)]
	//    example: "127.0.0.1:57186} 1d8ff... in"

	// 3. Split at the closing brace of the addr
	parts := strings.SplitN(body, "}", 2)
	if len(parts) != 2 {
		return "", errors.New("unable to split address and remainder")
	}
	addr := parts[0] // "127.0.0.1:57186"
	remainder := strings.TrimSpace(parts[1])
	//    example: "1d8ff37135f1583255143662b1e1343677c59332 in"

	// 4. Extract peerID (the first field in remainder)
	fields := strings.Fields(remainder)
	if len(fields) < 1 {
		return "", fmt.Errorf("no peerID found in %q", remainder)
	}
	peerID := fields[0]

	// 5. Build final string
	return fmt.Sprintf("%s@%s", peerID, addr), nil

}

func FormatStep(step string) (string, error) {
	step, found := strings.CutPrefix(step, "RoundStep")
	if !found {
		return "", fmt.Errorf("invalid step format, must start with 'RoundStep'")
	}
	return strcase.ToLowerCamel(step), nil
}

func ExtractPeerIdOnly(pid string) string {
	if atIndex := strings.Index(pid, "@"); atIndex != -1 {
		return pid[:atIndex]
	}
	return pid
}
