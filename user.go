package main

import (
	"fmt"

	"github.com/blueWeekend/go-binlog/v1"
)

type BinLog struct {
	Id       int64  `db:"id"`
	UserName string `db:"user_name"`
	ClueId   int64  `db:"clue_id"`
}

func (BinLog) TableName() string {
	return "binlog"
}

func (BinLog) DbName() string {
	return "t"
}

func (BinLog) OnUpdate(datas ...binlog.UpdateHandler) {
	fmt.Println("onupdate", datas)
}
func (BinLog) OnDelete(datas ...any) {
	data := make([]BinLog, 0, len(datas))
	for i := range datas {
		data = append(data, *(datas[i].(*BinLog)))
	}
	fmt.Println("OnDelete", data)
}
func (BinLog) OnInsert(datas ...any) {
	data := make([]BinLog, 0, len(datas))
	for i := range datas {
		data = append(data, *(datas[i].(*BinLog)))
	}
	fmt.Println("OnInsert", data)
}

func (BinLog) Schema() any {
	return &BinLog{}
}
