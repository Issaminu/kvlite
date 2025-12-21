package warpdb

import (
	"log"

	"github.com/Issaminu/warp-db/internal"
)

type WarpDB struct {
	path        string
	options     internal.Options
	collections []internal.Collection
}

func (warp *WarpDB) Open(path string, options internal.Options) {
	if path[len(path)-7:] != ".warpdb" {
		log.Fatal("path should end in '.warpdb'")
	}

}

func main() {

}
