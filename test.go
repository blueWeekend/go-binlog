package main

import (
	"fmt"
	"github.com/blueWeekend/go-binlog/v1"
)

func main() {
	lister, err := binlog.NewBinlogLister(&binlog.Config{
		User:     "root",
		Addr:     "127.0.0.1:3306",
		Password: "",
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
