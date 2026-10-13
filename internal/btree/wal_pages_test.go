package btree

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

func TestEncodeWALNodePagesInto(t *testing.T) {
	const pageSize = 64
	for _, size := range []int{0, 56, 57, 58, 1000} {
		for _, branch := range []bool{false, true} {
			t.Run(fmt.Sprintf("bytes=%d/branch=%t", size, branch), func(t *testing.T) {
				node := NewLeafNode(3)
				if branch {
					node = NewRootNode(3, NewLeafNode(4), NewLeafNode(5), make([]byte, size))
				} else if err := node.InsertEntry(NewEntry(0, []byte("key"), make([]byte, size))); err != nil {
					t.Fatal(err)
				}
				ids := make([]page.ID, NodePageCount(node, pageSize))
				for i := range ids {
					ids[i] = page.ID(3 + i)
				}
				want, err := EncodeWALNodePages(node, pageSize, ids)
				if err != nil {
					t.Fatal(err)
				}
				total := 0
				for _, data := range want {
					total += len(data)
				}
				if got := WALNodePagesEncodedSize(node, pageSize); got != total {
					t.Fatalf("encoded size: got %d, want %d", got, total)
				}
				for _, padded := range []bool{false, true} {
					buffers := make([][]byte, len(ids))
					pages := make([][]byte, len(ids))
					for i := range pages {
						length := len(want[i])
						if padded {
							length = pageSize
						}
						buffers[i] = bytes.Repeat([]byte{0xff}, length+2)
						pages[i] = buffers[i][1 : length+1 : length+1]
					}
					if err := EncodeWALNodePagesInto(node, pageSize, ids, pages); err != nil {
						t.Fatal(err)
					}
					for i, got := range pages {
						if !bytes.Equal(got, want[i]) || cap(got) != len(got) || &got[0] != &buffers[i][1] {
							t.Fatalf("destination %d does not contain the expected image", i)
						}
						if buffers[i][0] != 0xff || buffers[i][len(buffers[i])-1] != 0xff {
							t.Fatalf("destination %d changed bytes outside its bounds", i)
						}
					}
					pages[len(pages)-1] = pages[len(pages)-1][:len(pages[len(pages)-1])-1]
					if err := EncodeWALNodePagesInto(node, pageSize, ids, pages); !errors.Is(err, ErrInvalid) {
						t.Fatalf("short destination: got %v, want ErrInvalid", err)
					}
				}
			})
		}
	}
}
