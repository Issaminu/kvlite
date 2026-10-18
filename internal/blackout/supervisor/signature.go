package main

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// modulePath is the module of KVLite.
// The signature of a panic or a timeout uses the stack frames of functions in this module.
const modulePath = "github.com/Issaminu/kvlite"

const (
	// signatureFrames is the number of stack frames in the signature of a panic or a timeout.
	signatureFrames = 3
	// maxFirstLine is the maximum length of a first line and of a signature text, in bytes.
	maxFirstLine = 400
)

var (
	reportPrefix = regexp.MustCompile(`^blackout seed \d+ failed at step (\d+): `)
	prefixedHex  = regexp.MustCompile(`\b0[xX][0-9a-fA-F]+\b`)
	hexRun       = regexp.MustCompile(`\b[0-9a-fA-F]{6,}\b`)
	decimal      = regexp.MustCompile(`[0-9]+`)
	crashLine    = regexp.MustCompile(`(?m)^(panic|fatal error): .*$`)
)

// normalize replaces the parts of a message that change from run to run.
// A 0x number and a hexadecimal run of 6 or more characters with a digit and a letter become H.
// Then each decimal number becomes N.
// A run of only digits stays decimal, so that a large number and a small number in the same place give the same text.
func normalize(text string) string {
	text = prefixedHex.ReplaceAllString(text, "H")
	text = hexRun.ReplaceAllStringFunc(text, func(run string) string {
		if strings.ContainsAny(run, "0123456789") && strings.ContainsAny(run, "abcdefABCDEF") {
			return "H"
		}
		return run
	})
	return decimal.ReplaceAllString(text, "N")
}

// signatureText returns the text that groups failures of one kind.
// firstLine is the first line of the report, and output is the captured output of the worker.
func signatureText(kind, firstLine, output string) string {
	var text string
	switch kind {
	case kindFail:
		text = kind + ": " + normalize(reportPrefix.ReplaceAllString(firstLine, ""))
	case kindPanic, kindTimeout:
		if frames := stackFrames(output, signatureFrames); len(frames) > 0 {
			text = kind + ": " + strings.Join(frames, " < ")
		} else if kind == kindPanic {
			text = kind + ": " + normalize(strings.TrimPrefix(firstLine, "panic: "))
		} else {
			text = kind + ": no stack frames in " + modulePath
		}
	default:
		text = kind + ": " + normalize(firstLine)
	}
	return truncate(text, maxFirstLine)
}

// signatureID returns the first 12 hexadecimal characters of the SHA-256 hash of text.
func signatureID(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])[:12]
}

// stackFrames returns the names of the first limit functions of modulePath in a Go traceback.
// It uses the first goroutine that has such a function, and it drops arguments, file names, and line numbers.
func stackFrames(output string, limit int) []string {
	for _, block := range strings.Split(output, "\n\n") {
		block = strings.TrimLeft(block, "\n")
		if !strings.HasPrefix(block, "goroutine ") {
			continue
		}
		var frames []string
		for _, line := range strings.Split(block, "\n")[1:] {
			if line == "" || line[0] == '\t' || line[0] == ' ' || strings.HasPrefix(line, "created by ") {
				continue
			}
			open := strings.LastIndexByte(line, '(')
			if open <= 0 || !strings.HasSuffix(line, ")") {
				continue
			}
			if name := line[:open]; strings.HasPrefix(name, modulePath) {
				frames = append(frames, name)
				if len(frames) == limit {
					break
				}
			}
		}
		if len(frames) > 0 {
			return frames
		}
	}
	return nil
}

// crashMessage returns the first "panic:" or "fatal error:" line of output, or "" when there is none.
func crashMessage(output string) string {
	return strings.TrimSpace(crashLine.FindString(output))
}

// failedStep returns the step that a report's first line names, or -1 when the line names no step.
func failedStep(firstLine string) int {
	match := reportPrefix.FindStringSubmatch(firstLine)
	if match == nil {
		return -1
	}
	step, err := strconv.Atoi(match[1])
	if err != nil {
		return -1
	}
	return step
}

// firstLineOf returns the first line of text that is not empty.
func firstLineOf(text string) string {
	for line := range strings.Lines(text) {
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
	}
	return ""
}

// truncate shortens text to at most limit bytes, at a UTF-8 boundary.
func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	for limit > 0 && !utf8.RuneStart(text[limit]) {
		limit--
	}
	return text[:limit]
}
