package wal

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/Issaminu/kvlite/internal/btree"
	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

// RecordType tells recovery how to read a WAL record payload.
type RecordType uint8

// WAL record type values define the file format.
const (
	// RecordTypeCommit ends one WAL transaction. Its payload is empty.
	RecordTypeCommit RecordType = 0
	// RecordTypeMeta stores one complete metadata image.
	RecordTypeMeta RecordType = 1
	// RecordTypeAllocation stores one complete allocation page.
	RecordTypeAllocation RecordType = 2
	// RecordTypeNode stores one complete logical node image.
	RecordTypeNode RecordType = 3
	// RecordTypeBytePatch stores replacement byte ranges for one existing data or allocation page image. See [ApplyBytePatch].
	RecordTypeBytePatch RecordType = 4
	// RecordTypeEntryPatch stores entry changes, addressed by key, for one existing leaf or branch.
	RecordTypeEntryPatch RecordType = 5

	recordTypeSize          = 1 // A record type uses one byte.
	recordTransactionIDSize = 8 // A transaction ID uses one uint64 value.
	recordPayloadLengthSize = 4 // A payload length uses one uint32 value.
	ChecksumSize            = 4 // A CRC32C checksum uses one uint32 value.
	HeaderSize              = recordTypeSize + page.IDSize + recordTransactionIDSize + recordPayloadLengthSize
)

// MaxNodeImageSize bounds a complete logical node or its patch payload.
const MaxNodeImageSize = btree.MaxNodeImageSize

func recordPayloadLimit(kind RecordType, pageSize int64) int64 {
	switch kind {
	case RecordTypeNode, RecordTypeBytePatch, RecordTypeEntryPatch:
		return MaxNodeImageSize
	case RecordTypeAllocation:
		return pageSize
	default:
		return pageSize
	}
}

// ValidateNodeImage checks the complete compact image and its physical span.
// The WAL record must already have passed its checksum check.
func ValidateNodeImage(image []byte, id page.ID, pageSize int64) (int, error) {
	if err := btree.ValidateWALNode(image, id); err != nil {
		return 0, err
	}
	count, err := btree.NodeSpan(image, id, pageSize)
	if err != nil {
		return 0, err
	}
	maxPages := uint64(int64(^uint64(0)>>1) / pageSize)
	if uint64(count) > maxPages || uint64(id) > maxPages-uint64(count) {
		return 0, page.ErrInvalid
	}
	return count, nil
}

type TxID uint64

// RecordHeader is the fixed-size header of every WAL record.
type RecordHeader struct {
	Type   RecordType
	PageID page.ID
	TxID   TxID
}

// NodeRecord carries one changed data node from a write transaction to [WAL.Commit].
// Commit writes a complete image for the first change to each page after a checkpoint.
// Later changes can use a byte patch or an entry patch when it is smaller than the complete final image.
// A multi-page node uses one logical image. Only its head has a WAL record.
// NodeRecord exists only in memory. The WAL file does not store this structure.
type NodeRecord struct {
	// Original is the committed node before the transaction.
	// It can be nil for a new page or when the caller requires a complete image.
	Original *btree.MutableNode
	// Final is the private node image to commit.
	Final *btree.MutableNode
}

// AllocationRecord carries one complete allocation segment page to [WAL.Commit].
// It exists only in memory. The WAL file stores its page ID and payload.
type AllocationRecord struct {
	PageID  page.ID
	Payload []byte
}

// WALRecord represents one record in the WAL file format.
// [WAL.ReadRecords] returns WALRecord values during recovery.
// Header.Type defines whether Payload contains a compact node, complete physical page,
// byte-patch ranges, entry-patch operations, encoded metadata, or no bytes for a commit marker.
type WALRecord struct {
	Header RecordHeader
	// Payload contains bytes that can be written to the WAL file.
	Payload []byte
}

// EncodeWALRecord returns one encoded WAL record.
// checksumSeed selects the WAL generation. Use [RecordChecksumSeed] to get it from WAL file bytes.
// Node images and patches use MaxNodeImageSize. Allocation images use the
// physical page size. Other payloads must also fit in pageSize.
func EncodeWALRecord(record *WALRecord, pageSize int64, checksumSeed uint32) ([]byte, error) {
	size, err := EncodedWALRecordSize(record, pageSize)
	if err != nil {
		return nil, err
	}

	return AppendEncodedWALRecord(make([]byte, 0, size), record, checksumSeed), nil
}

// EncodedWALRecordSize returns the encoded size of record.
// It rejects payloads larger than the bounded size for their record type.
func EncodedWALRecordSize(record *WALRecord, pageSize int64) (int, error) {
	payloadSize := len(record.Payload)
	if pageSize <= 0 || int64(payloadSize) > recordPayloadLimit(record.Header.Type, pageSize) {
		return 0, fmt.Errorf("record payload %d bytes: %w", payloadSize, ErrRecordPayloadTooLarge)
	}
	return HeaderSize + payloadSize + ChecksumSize, nil
}

// AppendEncodedWALRecord appends one encoded WAL record to data.
// checksumSeed selects the WAL generation, as in [EncodeWALRecord].
// It does not validate the payload size.
func AppendEncodedWALRecord(data []byte, record *WALRecord, checksumSeed uint32) []byte {
	return appendEncodedPayloadRecord(data, record.Header, record.Payload, checksumSeed)
}

func appendEncodedPayloadRecord(data []byte, header RecordHeader, payload []byte, checksumSeed uint32) []byte {
	headerStart := len(data)
	data = appendRecordHeader(data, header, len(payload))
	payloadStart := len(data)
	data = append(data, payload...)
	return appendRecordChecksum(data, headerStart, payloadStart, checksumSeed)
}

func appendEncodedNodeRecord(data []byte, header RecordHeader, node *btree.MutableNode, checksumSeed uint32) []byte {
	headerStart := len(data)
	data = appendRecordHeader(data, header, btree.WALNodeEncodedSize(node))
	payloadStart := len(data)
	data = btree.AppendEncodedWALNode(data, node)
	return appendRecordChecksum(data, headerStart, payloadStart, checksumSeed)
}

func appendRecordHeader(data []byte, header RecordHeader, payloadSize int) []byte {
	data = append(data, byte(header.Type))
	data = binary.LittleEndian.AppendUint64(data, uint64(header.PageID))
	data = binary.LittleEndian.AppendUint64(data, uint64(header.TxID))
	return binary.LittleEndian.AppendUint32(data, uint32(payloadSize))
}

func appendRecordChecksum(data []byte, headerStart, payloadStart int, checksumSeed uint32) []byte {
	checksum := computeRecordChecksum(checksumSeed, data[headerStart:payloadStart], data[payloadStart:])
	data = binary.LittleEndian.AppendUint32(data, checksum)
	return data
}

// DecodeWALRecord reads and validates one WAL record from r.
// checksumSeed selects the WAL generation, as in [EncodeWALRecord].
// A record from another generation fails with [page.ErrChecksum].
func DecodeWALRecord(r io.Reader, pageSize int64, checksumSeed uint32) (*WALRecord, error) {
	headerData := make([]byte, HeaderSize)
	if err := fileio.ReadFull(r, headerData); err != nil {
		return nil, err // io.EOF at a clean boundary; io.ErrUnexpectedEOF on a torn tail
	}

	data := headerData
	record := &WALRecord{}
	record.Header.Type = RecordType(data[0])
	data = data[recordTypeSize:]
	record.Header.PageID = page.ID(binary.LittleEndian.Uint64(data[:page.IDSize]))
	data = data[page.IDSize:]
	record.Header.TxID = TxID(binary.LittleEndian.Uint64(data[:recordTransactionIDSize]))
	data = data[recordTransactionIDSize:]
	payloadSize := binary.LittleEndian.Uint32(data[:recordPayloadLengthSize])
	if pageSize <= 0 || int64(payloadSize) > recordPayloadLimit(record.Header.Type, pageSize) {
		// A torn tail can leave a bogus length prefix. Treat it as an integrity
		// failure before allocating an image.
		return nil, fmt.Errorf("record payload size %d exceeds its limit: %w", payloadSize, page.ErrChecksum)
	}

	var err error
	if record.Payload, err = readRecordPayload(r, int64(payloadSize)); err != nil {
		return nil, err
	}

	var checksumData [ChecksumSize]byte
	if err := fileio.ReadFull(r, checksumData[:]); err != nil {
		return nil, err
	}
	checksum := binary.LittleEndian.Uint32(checksumData[:])
	if computeRecordChecksum(checksumSeed, headerData, record.Payload) != checksum {
		return nil, page.ErrChecksum
	}

	return record, nil
}

// readRecordPayload checks the available bytes before a large allocation when
// the reader reports its length. An unknown stream grows only as bytes arrive.
func readRecordPayload(r io.Reader, size int64) ([]byte, error) {
	if size > int64(int(^uint(0)>>1)) {
		return nil, page.ErrChecksum
	}
	available := int64(-1)
	switch reader := r.(type) {
	case *io.SectionReader:
		position, err := reader.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		available = reader.Size() - position
	case interface{ Len() int }:
		available = int64(reader.Len())
	}
	if available >= 0 {
		if size+ChecksumSize > available {
			return nil, io.ErrUnexpectedEOF
		}
		payload := make([]byte, int(size))
		if err := fileio.ReadFull(r, payload); err != nil {
			return nil, err
		}
		return payload, nil
	}
	payload, err := io.ReadAll(io.LimitReader(r, size))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) != size {
		return nil, io.ErrUnexpectedEOF
	}
	return payload, nil
}

// IsCommitMarker reports whether record ends one WAL transaction.
func IsCommitMarker(record *WALRecord) bool {
	return record.Header.Type == RecordTypeCommit
}

// computeRecordChecksum returns the CRC32C checksum of the generation salt, header, and payload.
// checksumSeed is the checksum of the salt alone, so a record checksum binds the record to its generation.
func computeRecordChecksum(checksumSeed uint32, header, payload []byte) uint32 {
	return checksum.Update32(checksumSeed, header, payload)
}
