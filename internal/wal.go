package internal

import (
	"bufio"
	"log"
	"os"
	"strconv"
)

func ReplayLog(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var pairs = make(map[string]string)

	scanner := bufio.NewScanner(file)
	var counter int
	for scanner.Scan() {
		counter++
		lineBytes := scanner.Bytes()

		key, value, err := parseEntry(lineBytes)
		if err != nil {
			if err == ErrChecksumCompaisonFailed { // skip KV pairs where checksum compairson fails
				log.Println("Checksum comparison failed on line", counter)
				continue
			}
			return nil, err
		}

		pairs[key] = value

	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return pairs, nil
}

func parseEntry(binaryLine []byte) (string, string, error) {
	// Parse the log entry. Log entries are represented in binary in the following format:
	// [key length];[value length];[32-bitchecksum][key][value]
	// Seperators: ^ seperator    ^ seperator      ^----^ no seperator between (checksum and key) or (key and value)

	line := string(binaryLine)

	// Step 1: getting the bounds of the key length and value length

	var endOfKeyLength int
	var endOfValueLength int

	for i := range line {
		if line[i] != ';' {
			continue
		}
		if endOfKeyLength != 0 {
			endOfKeyLength = i
		} else {
			endOfValueLength = i
			break
		}
	}

	keyLength, err := strconv.Atoi(line[0:endOfKeyLength])
	if err != nil {
		return "", "", err
	}

	valueLength, err := strconv.Atoi(line[endOfKeyLength+1 : endOfValueLength])
	if err != nil {
		return "", "", err
	}

	// Step 2: check

	startOfKey := endOfValueLength + 33 // skipping 1 char for end seperator of Value + 32 chars for checksum
	endOfKey := startOfKey + keyLength

	key := line[startOfKey : endOfKey+1]             // +1 since the end value when slicing is exculded
	value := line[endOfKey+1 : endOfKey+valueLength] // +1 since the value starts immediatly after the key

	// Step 3: verify checksum match (protecting against cosmic bit-flip)
	// TODO: make entire logic logic use the []byte version rather than the string-to-byte that we're doing here
	checksumString := line[endOfValueLength+1 : endOfValueLength+32]

	checksum, err := StringToUint32(checksumString)

	if err != nil {
		return key, value, err
	}

	keyValChecksum := getKVChecksum(key, value)

	if checksum != keyValChecksum {
		return "", "", ErrChecksumCompaisonFailed
	}

	return key, value, nil
}
