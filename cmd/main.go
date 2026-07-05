package main

import (
	"fmt"
	"log"

	"github.com/Issaminu/kvlite"
	"github.com/Issaminu/kvlite/internal"
)

func main() {

	db := kvlite.StartDatabase("mydb.kvdb", internal.DefaultConfig())

	fmt.Println("Database started at path:", db.Path)

	// list collections

	fmt.Println("meta:", db.Metadata.CollectionCount, "collections")

	val, err := db.Get("default", "test")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Value for 'test' in 'default' collection:", val)

}
