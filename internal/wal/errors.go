package wal

import "errors"

var (
	ErrNodeRecordMissingFinal = errors.New("WAL node record has no final node") // ErrNodeRecordMissingFinal means a node record has no final node.
	ErrRecordPayloadTooLarge  = errors.New("record payload exceeds page size")  // ErrRecordPayloadTooLarge means a record payload exceeds the configured page size.
	ErrNodePageMismatch       = errors.New("WAL node record changes page ID")   // ErrNodePageMismatch means that the original and final nodes of one NodeRecord have different page IDs.
	ErrRecordPayloadRequired  = errors.New("record has no payload")             // ErrRecordPayloadRequired means a committed page has no node or encoded payload.
	ErrWriteFailed            = errors.New("WAL write failed")                  // ErrWriteFailed means a commit failed after it started to write the WAL. The WAL then rejects later commits.
)
