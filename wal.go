package kvlite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
)

const (
	recordTypeData   uint8 = 0
	recordTypeMeta   uint8 = 1
	recordTypeCommit uint8 = 2 // a commit marker: ends one transaction, no content
)

// recordHeaderSize is the fixed header size: type(1) + pgid(8) + txid(8) + content_size(4)
const recordHeaderSize = 1 + 8 + 8 + 4

type WAL struct {
	db               *DB
	path             string
	file             *os.File
	collectedRecords map[Pgid]Record // Mapping Page ID to it's corresponding record. Only used temporarily to aggregate records that happen within a write operation, then flush at once
	nextTxid         Txid            // sequence number stamped on the next committed transaction
}

// RecordHeader is the fixed-size head of every WAL record.
type RecordHeader struct {
	recordType  uint8
	pgid        Pgid
	txid        Txid
	contentSize uint32 // length of `Record.pageContent`. In the WAL file, it acts as pageContent's prefixed length
}

func (h RecordHeader) encode() []byte {
	b := make([]byte, 0, recordHeaderSize)
	b = append(b, h.recordType)
	b = binary.LittleEndian.AppendUint64(b, uint64(h.pgid))
	b = binary.LittleEndian.AppendUint64(b, uint64(h.txid))
	b = binary.LittleEndian.AppendUint32(b, h.contentSize)
	return b
}

// decodeRecordHeader parses a header from its raw bytes.
func decodeRecordHeader(raw []byte) RecordHeader {
	return RecordHeader{
		recordType:  raw[0],
		pgid:        Pgid(binary.LittleEndian.Uint64(raw[1:9])),
		txid:        Txid(binary.LittleEndian.Uint64(raw[9:17])),
		contentSize: binary.LittleEndian.Uint32(raw[17:21]),
	}
}

type Record struct {
	header      RecordHeader
	pageContent []byte // actual content of the `Record`. We can identify it's length by a prefixed attribute `header.contentSize`
	checksum    uint64
}

func (wal *WAL) insertNodeRecord(node *Node) {
	buf := new(bytes.Buffer)
	encodeNode(node, buf)

	record := Record{
		header:      RecordHeader{recordType: recordTypeData, pgid: node.pgid},
		pageContent: buf.Bytes(),
	}

	wal.collectRecord(&record)
}

func (wal *WAL) insertMetaRecord(meta *Meta) {
	buf := new(bytes.Buffer)
	meta.encode(buf)

	record := Record{
		header:      RecordHeader{recordType: recordTypeMeta, pgid: 0}, // 0 is the meta page
		pageContent: buf.Bytes(),
	}

	wal.collectRecord(&record)
}

func (wal *WAL) collectRecord(record *Record) {
	wal.collectedRecords[record.header.pgid] = *record
}

func (wal *WAL) readRecords() (*[]Record, error) {
	if !wal.hasRecords() {
		return nil, nil
	}

	var records []Record
	for {
		record, err := decodeRecord(wal.file, wal.db.meta.pageSize)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) { // we have already fully read all records
				break
			}
			return nil, err
		}
		records = append(records, *record)
	}

	return &records, nil
}

func (wal *WAL) hasRecords() bool {
	fi, err := wal.file.Stat()
	if err != nil {
		return false
	}
	return fi.Size() > 0
}

// Truncate the WAL file.
// Important: only truncate the file after making sure that it's content has been ingested to the database
func (wal *WAL) clear() error {
	if err := os.Truncate(wal.path, 0); err != nil {
		return err
	}
	// rewind the handle so writes after a recovery start at the beginning
	_, err := wal.file.Seek(0, io.SeekStart)
	return err
}

// Delete the WAL file
// Important: only delete the file when running db.Close()
func (wal *WAL) delete() error {
	// close the file before deletion
	err := wal.file.Close()
	if err != nil {
		return err
	}
	// delete the file from the disk
	err = os.Remove(wal.path)
	return err
}

// Encodes a *Record instance into an io.Writer
func encodeRecord(w io.Writer, record *Record, pageSize int64) error {
	if int64(len(record.pageContent)) > pageSize {
		return fmt.Errorf("record page content exceeds page size (%d > %d)", len(record.pageContent), pageSize)
	}

	record.header.contentSize = uint32(len(record.pageContent))
	header := record.header.encode()

	checksum := computeRecordChecksum(header, record.pageContent)

	if err := writeFull(w, header); err != nil {
		return fmt.Errorf("write record header: %w", err)
	}
	// no need to use `writeLengthPrefixedBytes` here since it's implicitely happening:
	// the 4 bytes that sit before pageContent is it's prefixed length (`RecordHeader.contentSize`)
	if err := writeFull(w, record.pageContent); err != nil {
		return fmt.Errorf("write record pageContent: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("write record checksum: %w", err)
	}

	return nil
}

// Decodes a *Record instance from an io.Reader
func decodeRecord(r io.Reader, pageSize int64) (*Record, error) {
	header := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err // io.EOF at a clean boundary; io.ErrUnexpectedEOF on a torn tail
	}

	record := &Record{header: decodeRecordHeader(header)}
	if int64(record.header.contentSize) > pageSize {
		return nil, fmt.Errorf("record content_size %d exceeds page size %d", record.header.contentSize, pageSize)
	}

	record.pageContent = make([]byte, record.header.contentSize)
	if _, err := io.ReadFull(r, record.pageContent); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &record.checksum); err != nil {
		return nil, err
	}
	if computeRecordChecksum(header, record.pageContent) != record.checksum {
		return nil, ErrChecksum
	}

	return record, nil
}

func (wal *WAL) applyRecordToDatabase(record *Record, pageSize int64) error {
	offset := int64(record.header.pgid) * pageSize
	if _, err := wal.db.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	// content is stored unpadded in the WAL; pad it to a full page for the main file
	page := record.pageContent
	if int64(len(page)) < pageSize {
		padded := make([]byte, pageSize)
		copy(padded, page)
		page = padded
	}
	if _, err := wal.db.file.Write(page); err != nil {
		return err
	}
	// return wal.db.file.Sync()
	return nil
}

func computeRecordChecksum(header, content []byte) uint64 {
	hashFunc := fnv.New64a()
	hashFunc.Write(header)
	hashFunc.Write(content)
	return hashFunc.Sum64()
}

func (wal *WAL) persistCollectedRecords() error {
	if len(wal.collectedRecords) == 0 {
		return nil
	}

	txid := wal.nextTxid
	wal.nextTxid++

	largeBuf := new(bytes.Buffer)
	recordBuffer := new(bytes.Buffer)

	for _, record := range wal.collectedRecords {
		record.header.txid = txid

		if err := encodeRecord(recordBuffer, &record, wal.db.meta.pageSize); err != nil {
			return err
		}

		// append to `largeBuf`
		_, err := largeBuf.ReadFrom(recordBuffer)
		if err != nil {
			return err
		}
	}

	wal.insertCommitMarker(largeBuf, txid)

	if err := writeFull(wal.file, largeBuf.Bytes()); err != nil {
		return fmt.Errorf("persist multiple records: %w", err)
	}

	// reset for the next write batch
	wal.collectedRecords = make(map[Pgid]Record)

	return nil
}

func (wal *WAL) insertCommitMarker(buf *bytes.Buffer, txid Txid) error {
	recordBuffer := new(bytes.Buffer)
	commitMarker := &Record{
		header:      RecordHeader{recordType: recordTypeCommit, pgid: 0, txid: txid},
		pageContent: nil,
	}

	encodeRecord(recordBuffer, commitMarker, wal.db.meta.pageSize)

	_, err := buf.ReadFrom(recordBuffer)
	if err != nil {
		return err
	}

	return nil
}

func isRecordCommitMarker(record *Record) bool {
	return record.header.recordType == recordTypeCommit
}
