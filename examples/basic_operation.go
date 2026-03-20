package main

import (
	bitcask "bitcask-my"
	"bitcask-my/common"
	"fmt"
)

func main() {
	opts := common.DefaultOptions
	opts.DirPath = "/tmp/bitcask-data"
	db, err := bitcask.Open(
		opts,
	)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("name"), []byte("茉莉"))
	if err != nil {
		panic(err)
	}
	value, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("name: %s\n", value)

}
