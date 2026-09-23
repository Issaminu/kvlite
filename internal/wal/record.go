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
	// RecordTypeNode stores one complete compact data-node image.
	RecordTypeNode RecordType = 2
	// RecordTypePatch stores replacement ranges for one existing data node.
	RecordTypePatch RecordType = 3

	recordTypeSize          = 1 // A record type uses one byte.
	recordTransactionIDSize = 8 // A transaction ID uses one uint64 value.
	recordPayloadLengthSize = 4 // A payload length uses one uint32 value.
	ChecksumSize            = 4 // A CRC32C checksum uses one uint32 value.
	HeaderSize              = recordTypeSize + page.IDSize + recordTransactionIDSize + recordPayloadLengthSize
)

type TxID uint64

// RecordHeader is the fixed-size header of every WAL record.
type RecordHeader struct {
	Type   RecordType
	PageID page.ID
	TxID   TxID
}

// NodeRecord carries one changed data node from a write transaction to [WAL.Commit].
// WAL.Commit compares Original and Final and writes a page patch when it is smaller than the complete final image.
// NodeRecord exists only in memory. The WAL file does not store this structure.
type NodeRecord struct {
	// Original is the committed node before the transaction.
	// It is nil for a new page.
	Original *btree.Node
	// Final is the private node image to commit.
	Final *btree.Node
}

// WALRecord represents one record in the WAL file format.
// [WAL.ReadRecords] returns WALRecord values during recovery.
// Header.Type defines whether Payload contains a complete node image, page-patch ranges, encoded metadata, or no bytes for a commit marker.
type WALRecord struct {
	Header RecordHeader
	// Payload contains bytes that can be written to the WAL file.
	Payload []byte
}

// EncodeWALRecord returns one encoded WAL record.
// It returns an error when Payload is larger than pageSize.
func EncodeWALRecord(record *WALRecord, pageSize int64) ([]byte, error) {
	size, err := EncodedWALRecordSize(record, pageSize)
	if err != nil {
		return nil, err
	}

	return AppendEncodedWALRecord(make([]byte, 0, size), record), nil
}

// EncodedWALRecordSize returns the encoded size of record.
// It returns an error when Payload is larger than pageSize.
func EncodedWALRecordSize(record *WALRecord, pageSize int64) (int, error) {
	payloadSize := len(record.Payload)
	if int64(payloadSize) > pageSize {
		return 0, fmt.Errorf("record payload exceeds page size (%d > %d)", payloadSize, pageSize)
	}
	return HeaderSize + payloadSize + ChecksumSize, nil
}

// AppendEncodedWALRecord appends one encoded WAL record to data.
// It does not validate the payload size.
func AppendEncodedWALRecord(data []byte, record *WALRecord) []byte {
	return appendEncodedPayloadRecord(data, record.Header, record.Payload)
}

func appendEncodedPayloadRecord(data []byte, header RecordHeader, payload []byte) []byte {
	headerStart := len(data)
	data = appendRecordHeader(data, header, len(payload))
	payloadStart := len(data)
	data = append(data, payload...)
	return appendRecordChecksum(data, headerStart, payloadStart)
}

func appendEncodedNodeRecord(data []byte, header RecordHeader, node *btree.Node) []byte {
	headerStart := len(data)
	data = appendRecordHeader(data, header, btree.WALNodeEncodedSize(node))
	payloadStart := len(data)
	data = btree.AppendEncodedWALNode(data, node)
	return appendRecordChecksum(data, headerStart, payloadStart)
}

func appendEncodedPagePatchRecord(data []byte, header RecordHeader, patch *PagePatch) []byte {
	headerStart := len(data)
	data = appendRecordHeader(data, header, patch.encodedSize())
	payloadStart := len(data)
	data = patch.appendEncoded(data)
	return appendRecordChecksum(data, headerStart, payloadStart)
}

func appendRecordHeader(data []byte, header RecordHeader, payloadSize int) []byte {
	data = append(data, byte(header.Type))
	data = binary.LittleEndian.AppendUint64(data, uint64(header.PageID))
	data = binary.LittleEndian.AppendUint64(data, uint64(header.TxID))
	return binary.LittleEndian.AppendUint32(data, uint32(payloadSize))
}

func appendRecordChecksum(data []byte, headerStart, payloadStart int) []byte {
	checksum := computeRecordChecksum(data[headerStart:payloadStart], data[payloadStart:])
	data = binary.LittleEndian.AppendUint32(data, checksum)
	return data
}

// DecodeWALRecord reads and validates one WAL record from r.
func DecodeWALRecord(r io.Reader, pageSize int64) (*WALRecord, error) {
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
	if int64(payloadSize) > pageSize {
		// A torn tail can leave a bogus length prefix. Treat it as an integrity
		// failure so the caller does not allocate more than one page.
		return nil, fmt.Errorf("record payload size %d exceeds page size %d: %w", payloadSize, pageSize, page.ErrChecksum)
	}

	record.Payload = make([]byte, payloadSize)
	if err := fileio.ReadFull(r, record.Payload); err != nil {
		return nil, err
	}

	var checksumData [ChecksumSize]byte
	if err := fileio.ReadFull(r, checksumData[:]); err != nil {
		return nil, err
	}
	checksum := binary.LittleEndian.Uint32(checksumData[:])
	if computeRecordChecksum(headerData, record.Payload) != checksum {
		return nil, page.ErrChecksum
	}

	return record, nil
}

// IsCommitMarker reports whether record ends one WAL transaction.
func IsCommitMarker(record *WALRecord) bool {
	return record.Header.Type == RecordTypeCommit
}

func computeRecordChecksum(header, payload []byte) uint32 {
	return checksum.Sum32(header, payload)
}
