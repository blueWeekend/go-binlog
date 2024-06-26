package main

import "fmt"

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

func (BinLog) OnUpdate(from, to any) {
	fmt.Println("onupdate", from, to)
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
