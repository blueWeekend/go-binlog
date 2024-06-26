package main

import (
	"fmt"
	"go-binlog/src"
)

func main() {
	lister, err := src.NewBinlogLister(&src.Config{
		User:     "root",
		Addr:     "127.0.0.1:3306",
		Password: "mysqlRoot1",
	})
	if err != nil {
		panic(err)
	}
	b := BinLog{}
	lister.RegisterEventHandler(b)
	go func() {
		for err := range lister.Errors() {
			fmt.Println("err:", err.Error())
		}
	}()
	// lister.GetBinLogData(b, nil, 0)
	fmt.Println("start", err == nil)
	err = lister.Run()
	fmt.Println("reslllllll", err == nil, err)
}
