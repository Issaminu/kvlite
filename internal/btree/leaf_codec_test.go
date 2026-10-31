package btree

import (
	"bytes"
	"testing"
)

func TestLeafImageEditKeepsOriginalAndAllocation(t *testing.T) {
	const pageSize = 128
	original := NewLeafNode(3)
	if err := original.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), 512))); err != nil {
		t.Fatal(err)
	}
	original.SetStoragePageCount(NodePageCount(original, pageSize))
	data := make([]byte, original.StoragePageCount()*pageSize)
	if err := EncodeNodeInto(data, original, pageSize, false); err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(data)
	view, err := ReadNodeImage(data, 3, pageSize, false, false)
	if err != nil {
		t.Fatal(err)
	}
	private, err := view.Clone(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	private.DeleteLeafEntries(0, nil)
	private.OwnImageBytes()
	if private.EntryCount() != 0 || private.StoragePageCount() != original.StoragePageCount() {
		t.Fatal("delete lost allocation ownership")
	}
	if !bytes.Equal(data, before) {
		t.Fatal("private edit changed committed bytes")
	}
}
