package main

import (
	"fmt"
	"log"

	warpdb "github.com/Issaminu/warp-db"
	"github.com/Issaminu/warp-db/internal"
)

func main() {

	db := warpdb.StartDatabase("mydb.warpdb", internal.DefaultConfig())

	fmt.Println("Database started at path:", db.Path)

	// list collections

	fmt.Println("meta:", db.Metadata.CollectionCount, "collections")

	val, err := db.Get("default", "test")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Value for 'test' in 'default' collection:", val)

}
