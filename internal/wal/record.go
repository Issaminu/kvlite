package wal

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/Issaminu/kvlite/internal/checksum"
	"github.com/Issaminu/kvlite/internal/fileio"
	"github.com/Issaminu/kvlite/internal/page"
)

type RecordType uint8

const (
	RecordTypeData   RecordType = 0
	RecordTypeMeta   RecordType = 1
	RecordTypeCommit RecordType = 2

	recordTypeSize          = 1 // A record type uses one byte.
	recordTransactionIDSize = 8 // A transaction ID uses one uint64 value.
	recordContentLengthSize = 4 // A content length uses one uint32 value.
	ChecksumSize            = 4 // A CRC32C checksum uses one uint32 value.
	HeaderSize              = recordTypeSize + page.IDSize + recordTransactionIDSize + recordContentLengthSize
)

type TxID uint64

// RecordHeader is the fixed-size head of every WAL record.
type RecordHeader struct {
	Type   RecordType
	PageID page.ID
	TxID   TxID
}

type Record struct {
	Header      RecordHeader
	PageContent []byte
}

func EncodeRecord(record *Record, pageSize int64) ([]byte, error) {
	size, err := EncodedRecordSize(record, pageSize)
	if err != nil {
		return nil, err
	}

	return AppendEncodedRecord(make([]byte, 0, size), record), nil
}

func EncodedRecordSize(record *Record, pageSize int64) (int, error) {
	if int64(len(record.PageContent)) > pageSize {
		return 0, fmt.Errorf("record page content exceeds page size (%d > %d)", len(record.PageContent), pageSize)
	}
	return HeaderSize + len(record.PageContent) + ChecksumSize, nil
}

func AppendEncodedRecord(data []byte, record *Record) []byte {
	headerStart := len(data)
	data = append(data, byte(record.Header.Type))
	data = binary.LittleEndian.AppendUint64(data, uint64(record.Header.PageID))
	data = binary.LittleEndian.AppendUint64(data, uint64(record.Header.TxID))
	data = binary.LittleEndian.AppendUint32(data, uint32(len(record.PageContent)))
	headerEnd := len(data)
	data = append(data, record.PageContent...)
	checksum := computeRecordChecksum(data[headerStart:headerEnd], record.PageContent)
	data = binary.LittleEndian.AppendUint32(data, checksum)

	return data
}

func DecodeRecord(r io.Reader, pageSize int64) (*Record, error) {
	headerData := make([]byte, HeaderSize)
	if err := fileio.ReadFull(r, headerData); err != nil {
		return nil, err // io.EOF at a clean boundary; io.ErrUnexpectedEOF on a torn tail
	}

	data := headerData
	record := &Record{}
	record.Header.Type = RecordType(data[0])
	data = data[recordTypeSize:]
	record.Header.PageID = page.ID(binary.LittleEndian.Uint64(data[:page.IDSize]))
	data = data[page.IDSize:]
	record.Header.TxID = TxID(binary.LittleEndian.Uint64(data[:recordTransactionIDSize]))
	data = data[recordTransactionIDSize:]
	contentSize := binary.LittleEndian.Uint32(data[:recordContentLengthSize])
	if int64(contentSize) > pageSize {
		// A torn tail can leave a bogus length prefix. Treat it as an integrity
		// failure so the caller does not allocate more than one page.
		return nil, fmt.Errorf("record content_size %d exceeds page size %d: %w", contentSize, pageSize, page.ErrChecksum)
	}

	record.PageContent = make([]byte, contentSize)
	if err := fileio.ReadFull(r, record.PageContent); err != nil {
		return nil, err
	}

	var checksumData [ChecksumSize]byte
	if err := fileio.ReadFull(r, checksumData[:]); err != nil {
		return nil, err
	}
	checksum := binary.LittleEndian.Uint32(checksumData[:])
	if computeRecordChecksum(headerData, record.PageContent) != checksum {
		return nil, page.ErrChecksum
	}

	return record, nil
}

func IsCommitMarker(record *Record) bool {
	return record.Header.Type == RecordTypeCommit
}

func computeRecordChecksum(header, content []byte) uint32 {
	return checksum.Sum32(header, content)
}
