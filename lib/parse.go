package lib

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/bft-labs/cometbft-analyzer-types/pkg/core"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func ParseRoundInfo(input string) (uint64, uint64, string, error) {
	parts := strings.Split(input, "/")
	if len(parts) != 3 {
		return 0, 0, "", fmt.Errorf("invalid input format, expected 'height/round/step', got '%s'", input)
	}

	height, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, "", fmt.Errorf("invalid height '%s': %w", parts[0], err)
	}

	round, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, "", fmt.Errorf("invalid round '%s': %w", parts[1], err)
	}

	step, err := FormatStep(parts[2])
	if err != nil {
		return 0, 0, "", fmt.Errorf("invalid step '%s': %w", parts[2], err)
	}

	return height, round, step, nil
}

func ParseProposalString(s string) (*core.Proposal, error) {
	// 1) Escape the braces.
	// 2) Capture:
	//    1: height
	//    2: round
	//    3: blockHash
	//    4: partSetTotal
	//    5: partSetHash (first 6 bytes)
	//    6: polRound
	//    7: sigFingerprint (first 6 bytes)
	//    8: timestamp
	re := regexp.MustCompile(
		`^Proposal\{` +
			`(\d+)\/(\d+) ` +
			`\(([0-9A-F]+):(\d+):([0-9A-F]+), (-?\d+)\) ` +
			`([0-9A-F]+) @ ` +
			`(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)` +
			`\}$`,
	)
	matches := re.FindStringSubmatch(s)
	if matches == nil {
		return nil, fmt.Errorf("invalid Proposal format: %q", s)
	}

	// Parse height and round
	height, err := strconv.ParseUint(matches[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid height: %w", err)
	}
	round, err := strconv.ParseUint(matches[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid round: %w", err)
	}

	// Parse PartSetHeader.Total from group 4
	psTotal, err := strconv.ParseUint(matches[4], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid part set total: %w", err)
	}

	// Parse POLRound
	polRound, err := strconv.ParseInt(matches[6], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid pol round: %w", err)
	}

	// Parse signature fingerprint (first 6 bytes)
	sigFP := matches[7]

	// Parse timestamp
	ts := MustParseUtcTimestamp(matches[8])

	// Build BlockID (uses BlockID.String ⇒ hash:PartSetHeader)
	blockID := core.BlockID{
		Hash: matches[3],
		PartSetHeader: core.PartSetHeader{
			Total: psTotal,
			Hash:  matches[5],
		},
	}

	return &core.Proposal{
		Height:    height,
		Round:     round,
		PolRound:  polRound,
		BlockID:   blockID,
		Signature: sigFP, // just the fingerprint we captured
		Timestamp: ts,
	}, nil
}

func ParseVoteString(s string) (*core.Vote, error) {
	s = strings.TrimSpace(s)
	if s == "nil-Vote" {
		return nil, nil
	}

	// 1) Strip surrounding [ ] and leading "Vote "
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		s = s[1 : len(s)-1]
	}
	s = strings.TrimPrefix(s, "Vote ")

	// 2) Expect "Vote{...}"
	if !strings.HasPrefix(s, "Vote{") || !strings.HasSuffix(s, "}") {
		return nil, fmt.Errorf("unexpected vote format: %q", s)
	}
	body := s[len("Vote{") : len(s)-1]
	parts := strings.Fields(body)

	// 3) Find the "@" index (timestamp delimiter)
	atIdx := -1
	for i, tok := range parts {
		if tok == "@" {
			atIdx = i
			break
		}
	}
	if atIdx < 0 {
		return nil, fmt.Errorf("missing '@' in vote string")
	}
	if atIdx < 5 {
		return nil, fmt.Errorf("not enough fields before '@': %v", parts[:atIdx])
	}

	// 4) Parse validator index/address
	viAddr := strings.SplitN(parts[0], ":", 2)
	if len(viAddr) != 2 {
		return nil, fmt.Errorf("invalid validator info %q", parts[0])
	}
	validatorIndex, err := strconv.ParseUint(viAddr[0], 10, 64)
	if err != nil {
		return nil, err
	}
	validatorAddress := viAddr[1]

	// 5) Parse height, round, and type
	hrt := strings.SplitN(parts[1], "/", 3)
	if len(hrt) != 3 {
		return nil, fmt.Errorf("invalid height/round/type %q", parts[1])
	}
	height, err := strconv.ParseUint(hrt[0], 10, 64)
	if err != nil {
		return nil, err
	}
	round, err := strconv.ParseUint(hrt[1], 10, 64)
	if err != nil {
		return nil, err
	}
	rawType := hrt[2]
	voteType := rawType
	if i := strings.Index(rawType, "("); i >= 0 {
		if j := strings.LastIndex(rawType, ")"); j > i {
			voteType = rawType[i+1 : j]
		}
	}

	// 6) Parse BlockID (three fields)
	blockHash := parts[2]
	partSetHash := parts[3]
	partTotal, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return nil, err
	}
	blockID := core.BlockID{
		Hash: blockHash,
		PartSetHeader: core.PartSetHeader{
			Total: partTotal,
			Hash:  partSetHash,
		},
	}

	// 7) Any fields between the blockID and "@" are signature/extension
	var signature, extension string
	sigExtCount := atIdx - 5
	if sigExtCount >= 1 {
		signature = parts[5]
	}
	if sigExtCount >= 2 {
		extension = parts[6]
	}

	// 8) Parse timestamp
	if atIdx+1 >= len(parts) {
		return nil, fmt.Errorf("missing timestamp after '@'")
	}
	timestamp, err := time.Parse(time.RFC3339Nano, parts[atIdx+1])
	if err != nil {
		return nil, err
	}

	return &core.Vote{
		Type:             strings.ToLower(voteType),
		Height:           height,
		Round:            round,
		BlockId:          blockID,
		Timestamp:        timestamp,
		ValidatorAddress: validatorAddress,
		ValidatorIndex:   validatorIndex,
		Signature:        signature,
		Extension:        extension,
	}, nil
}

func ParseBlockString(s string) (*core.Block, error) {
	if s == "nil-Block" {
		return nil, nil
	}

	parser := &blockParser{
		input:   s,
		scanner: bufio.NewScanner(strings.NewReader(s)),
	}

	return parser.parseBlock()
}

type blockParser struct {
	input   string
	scanner *bufio.Scanner
	lineNum int
}

func (p *blockParser) nextLine() (string, error) {
	if !p.scanner.Scan() {
		if err := p.scanner.Err(); err != nil {
			return "", err
		}
		return "", errors.New("unexpected end of input")
	}
	p.lineNum++
	return p.scanner.Text(), nil
}

func (p *blockParser) parseBlock() (*core.Block, error) {
	line, err := p.nextLine()
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "Block{") {
		return nil, fmt.Errorf("expected 'Block{', got '%s'", line)
	}

	block := &core.Block{}

	if err := p.expectSection("Header{"); err != nil {
		return nil, err
	}
	header, err := p.parseHeader()
	if err != nil {
		return nil, fmt.Errorf("failed to parse header: %w", err)
	}
	block.Header = *header

	if err := p.expectSection("Data{"); err != nil {
		return nil, err
	}
	data, err := p.parseData()
	if err != nil {
		return nil, fmt.Errorf("failed to parse data: %w", err)
	}
	block.Data = *data

	if err := p.expectSection("EvidenceData{"); err != nil {
		return nil, err
	}
	evidence, err := p.parseEvidenceData()
	if err != nil {
		return nil, fmt.Errorf("failed to parse evidence: %w", err)
	}
	block.Evidence = *evidence

	if err := p.expectSection("Commit{"); err != nil {
		return nil, err
	}
	lastCommit, err := p.parseCommit()
	if err != nil {
		return nil, fmt.Errorf("failed to parse last commit: %w", err)
	}
	block.LastCommit = lastCommit

	line, err = p.nextLine()
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "}#") {
		return nil, fmt.Errorf("expected closing brace with hash, got '%s'", line)
	}

	return block, nil
}

func (p *blockParser) expectSection(expected string) error {
	line, err := p.nextLine()
	if err != nil {
		return err
	}
	if !strings.Contains(line, expected) {
		return fmt.Errorf("expected '%s', got '%s'", expected, line)
	}
	return nil
}

func (p *blockParser) parseHeader() (*core.Header, error) {
	header := &core.Header{}

	for i := 0; i < 14; i++ {
		line, err := p.nextLine()
		if err != nil {
			return nil, err
		}

		parts := strings.SplitN(strings.TrimSpace(line), " ", 2)
		if len(parts) < 2 {
			continue
		}

		fieldName := parts[0]
		value := strings.TrimSpace(parts[1])

		switch fieldName {
		case "Version:":
			version, err := parseConsensusVersion(value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse version: %w", err)
			}
			header.Version = version
		case "ChainID:":
			header.ChainID = value
		case "Height:":
			height, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse height: %w", err)
			}
			header.Height = height
		case "Time:":
			t, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				t, err = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", value)
				if err != nil {
					return nil, fmt.Errorf("failed to parse time: %w", err)
				}
			}
			header.Time = t
		case "LastBlockID:":
			blockID, err := parseBlockID(value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse LastBlockID: %w", err)
			}
			header.LastBlockID = blockID
		case "LastCommit:", "Data:", "Validators:", "NextValidators:",
			"App:", "Consensus:", "Results:", "Evidence:":
			hash := value
			switch fieldName {
			case "LastCommit:":
				header.LastCommitHash = hash
			case "Data:":
				header.DataHash = hash
			case "Validators:":
				header.ValidatorsHash = hash
			case "NextValidators:":
				header.NextValidatorsHash = hash
			case "App:":
				header.AppHash = hash
			case "Consensus:":
				header.ConsensusHash = hash
			case "Results:":
				header.LastResultsHash = hash
			case "Evidence:":
				header.EvidenceHash = hash
			}
		case "Proposer:":
			header.ProposerAddress = value
		}
	}

	_, err := p.nextLine()
	if err != nil {
		return nil, err
	}

	return header, nil
}

func (p *blockParser) parseData() (*core.Data, error) {
	data := &core.Data{
		Txs: []core.Tx{},
	}

	for {
		line, err := p.nextLine()
		if err != nil {
			return nil, err
		}

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "}#") {
			break
		}

		if strings.Contains(line, " bytes)") {
			re := regexp.MustCompile(`([0-9A-F]+) \(\d+ bytes\)`)
			matches := re.FindStringSubmatch(line)
			if len(matches) > 1 {
				txBytes, err := hex.DecodeString(matches[1])
				if err == nil {
					data.Txs = append(data.Txs, core.Tx(txBytes))
				}
			}
		}
	}

	return data, nil
}

func (p *blockParser) parseEvidenceData() (*core.EvidenceData, error) {
	evidence := &core.EvidenceData{
		Evidence: []core.Evidence{},
	}

	for {
		line, err := p.nextLine()
		if err != nil {
			return nil, err
		}

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "}#") {
			break
		}
	}

	return evidence, nil
}

func (p *blockParser) parseCommit() (*core.Commit, error) {
	commit := &core.Commit{}

	line, err := p.nextLine()
	if err != nil {
		return nil, err
	}
	if height, err := parseFieldInt64(line, "Height:"); err == nil {
		commit.Height = height
	}

	line, err = p.nextLine()
	if err != nil {
		return nil, err
	}
	if round, err := parseFieldInt32(line, "Round:"); err == nil {
		commit.Round = round
	}

	line, err = p.nextLine()
	if err != nil {
		return nil, err
	}
	if strings.Contains(line, "BlockID:") {
		parts := strings.SplitN(line, "BlockID:", 2)
		if len(parts) == 2 {
			blockID, err := parseBlockID(strings.TrimSpace(parts[1]))
			if err == nil {
				commit.BlockID = blockID
			}
		}
	}

	line, err = p.nextLine()
	if err != nil {
		return nil, err
	}
	if !strings.Contains(line, "Signatures:") {
		return nil, fmt.Errorf("expected 'Signatures:', got '%s'", line)
	}

	commit.Signatures = []core.CommitSig{}

	for {
		line, err = p.nextLine()
		if err != nil {
			return nil, err
		}

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "}#") {
			break
		}

		if strings.HasPrefix(line, "CommitSig{") {
			sig, err := parseCommitSig(line)
			if err == nil {
				commit.Signatures = append(commit.Signatures, sig)
			}
		}
	}

	return commit, nil
}

func parseConsensusVersion(s string) (core.Consensus, error) {
	var version core.Consensus

	re := regexp.MustCompile(`\{(\d+)\s+(\d+)\}`)
	matches := re.FindStringSubmatch(s)
	if len(matches) == 3 {
		block, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			return version, err
		}
		app, err := strconv.ParseUint(matches[2], 10, 64)
		if err != nil {
			return version, err
		}
		version.Block = block
		version.App = app
		return version, nil
	}

	re = regexp.MustCompile(`\{Block:(\d+)\s+App:(\d+)\}`)
	matches = re.FindStringSubmatch(s)
	if len(matches) == 3 {
		block, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			return version, err
		}
		app, err := strconv.ParseUint(matches[2], 10, 64)
		if err != nil {
			return version, err
		}
		version.Block = block
		version.App = app
		return version, nil
	}

	return version, fmt.Errorf("invalid version format: %s", s)
}

func parseBlockID(s string) (core.BlockID, error) {
	var blockID core.BlockID

	parts := strings.Split(s, ":")
	if len(parts) < 2 {
		return blockID, fmt.Errorf("invalid BlockID format: %s", s)
	}

	blockID.Hash = parts[0]

	if len(parts) >= 3 {
		total, err := strconv.ParseUint(parts[1], 10, 32)
		if err == nil {
			blockID.PartSetHeader = core.PartSetHeader{
				Total: total,
				Hash:  parts[2],
			}
		}
	} else if strings.Contains(s, "{") {
		re := regexp.MustCompile(`\{(\d+):([0-9A-F]+)\}`)
		matches := re.FindStringSubmatch(s)
		if len(matches) == 3 {
			total, err := strconv.ParseUint(matches[1], 10, 64)
			if err != nil {
				return blockID, err
			}
			blockID.PartSetHeader = core.PartSetHeader{
				Total: total,
				Hash:  matches[2],
			}
		}
	}

	return blockID, nil
}

func parseFieldInt64(line, fieldName string) (int64, error) {
	if !strings.Contains(line, fieldName) {
		return 0, fmt.Errorf("field %s not found", fieldName)
	}
	parts := strings.SplitN(line, fieldName, 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid format")
	}
	return strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
}

func parseFieldInt32(line, fieldName string) (int32, error) {
	if !strings.Contains(line, fieldName) {
		return 0, fmt.Errorf("field %s not found", fieldName)
	}
	parts := strings.SplitN(line, fieldName, 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid format")
	}
	val, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 32)
	return int32(val), err
}

func parseCommitSig(s string) (core.CommitSig, error) {
	var sig core.CommitSig

	re := regexp.MustCompile(`CommitSig\{([0-9A-F]+) by ([0-9A-F]+) on (\d+) @ ([^}]+)\}`)
	matches := re.FindStringSubmatch(s)
	if len(matches) != 5 {
		if strings.Contains(s, "nil-CommitSig") || strings.Contains(s, "BlockIDFlagAbsent") {
			return core.NewCommitSigAbsent(), nil
		}
		return sig, fmt.Errorf("invalid CommitSig format: %s", s)
	}

	sig.Signature = matches[1]
	sig.ValidatorAddress = matches[2]

	flag, err := strconv.ParseInt(matches[3], 10, 8)
	if err != nil {
		return sig, err
	}
	sig.BlockIDFlag = core.BlockIDFlag(flag)

	t, err := time.Parse(time.RFC3339, matches[4])
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05.999999999Z", matches[4])
		if err != nil {
			return sig, fmt.Errorf("failed to parse timestamp: %w", err)
		}
	}
	sig.Timestamp = t

	return sig, nil
}
