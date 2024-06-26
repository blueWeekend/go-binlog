package main

import (
	"fmt"
	"reflect"
)

type Interface interface {
	// 这里定义接口方法
	SomeMethod()
}

type A struct {
	Field1 string
	Field2 int
	// 其他字段...
}

// 实现 Interface 接口的方法
func (a A) SomeMethod() {
	// 实现方法的具体逻辑
}

// MyFunction 接收 Interface 类型的参数
func MyFunction(i Interface) {
	v := reflect.ValueOf(i)
	s := reflect.Indirect(v)
	t := s.Type()
	fmt.Println("ttttt", t)
	num := t.NumField()
	fmt.Println(num)
	// 使用反射获取结构体类型
	// t := reflect.TypeOf(i)

	// // 确保参数是结构体
	// if t.Kind() != reflect.Struct {
	// 	fmt.Println("参数不是结构体")
	// 	return
	// }

	// // 遍历结构体字段并输出字段名
	// for j := 0; j < t.NumField(); j++ {
	// 	field := t.Field(j)
	// 	fmt.Println("字段名:", field.Name)
	// 	// 可以根据需要获取其他字段信息，如类型、标签等
	// }
}

func main() {
	a := A{Field1: "value1", Field2: 42}
	MyFunction(a)
}
