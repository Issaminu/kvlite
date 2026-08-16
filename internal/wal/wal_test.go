package wal

import (
	"bytes"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

func TestRecordCodec_UsesFixedLittleEndianLayout(t *testing.T) {
	record := &Record{
		Header: RecordHeader{
			Type:   RecordTypeData,
			PageID: page.ID(1),
			TxID:   TxID(2),
		},
		PageContent: []byte("xy"),
	}
	// Layout: record type, page ID, transaction ID, content length, content,
	// and the FNV-1a checksum of all preceding bytes.
	want := []byte{
		0,
		1, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0, 0, 0, 0, 0,
		2, 0, 0, 0,
		'x', 'y',
		0xf1, 0x3c, 0x9c, 0xc5, 0x52, 0x74, 0xd3, 0x4f,
	}

	encoded, err := EncodeRecord(record, int64(len(record.PageContent)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encode WAL record: got %x, want %x", encoded, want)
	}

	decoded, err := DecodeRecord(bytes.NewReader(encoded), int64(len(record.PageContent)))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Header != record.Header || !bytes.Equal(decoded.PageContent, record.PageContent) {
		t.Fatalf("decode WAL record: got %+v, want %+v", decoded, record)
	}
}
