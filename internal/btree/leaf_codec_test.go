package btree

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestDeleteMappedLeafPreservesImageAndSpan(t *testing.T) {
	for _, checksums := range []bool{false, true} {
		node := NewLeafNode(3)
		if err := node.InsertEntry(NewEntry(0, []byte("key"), bytes.Repeat([]byte("v"), 1024))); err != nil {
			t.Fatal(err)
		}
		const pageSize = 256
		data := make([]byte, NodePageCount(node, pageSize)*pageSize)
		if err := EncodeNodeInto(data, node, pageSize, checksums); err != nil {
			t.Fatal(err)
		}
		before := bytes.Clone(data)
		empty, err := DeleteMappedLeaf(data, 3, pageSize, checksums, []byte("key"))
		if err != nil || empty == nil || empty.EntryCount() != 0 || empty.StoragePageCount() != len(data)/pageSize {
			t.Fatalf("empty leaf: %v, %v", empty, err)
		}
		if !bytes.Equal(data, before) {
			t.Fatal("preparation changed the committed image")
		}
		if missing, err := DeleteMappedLeaf(data, 3, pageSize, checksums, []byte("missing")); err != nil || missing != nil {
			t.Fatalf("missing key: %v, %v", missing, err)
		}
		if checksums {
			data[len(data)-1] ^= 1
		} else {
			binary.LittleEndian.PutUint32(data[NodeHeaderSize+4:], uint32(len(data)+1))
		}
		if bad, err := DeleteMappedLeaf(data, 3, pageSize, checksums, []byte("key")); err == nil || bad != nil {
			t.Fatalf("corrupt image accepted: %v, %v", bad, err)
		}
	}
}
