package binlog

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	jsoniter "github.com/json-iterator/go"
	// jsoniter "github.com/json-iterator/go"
)

type BinlogParser struct {
	columnTag string
}

func (m *BinlogParser) GetBinLogData(element interface{}, e *canal.RowsEvent, n int) error {
	value := reflect.ValueOf(element).Elem()
	num := value.NumField()
	t := value.Type()
	for k := 0; k < num; k++ {
		columnName := t.Field(k).Tag.Get(m.columnTag)
		name := value.Field(k).Type().Name()
		switch name {
		case "bool":
			value.Field(k).SetBool(m.boolHelper(e, n, columnName))
		case "int64", "uint64", "int", "uint", "int32", "uint32", "int8", "uint8":
			value.Field(k).SetInt(m.intHelper(e, n, columnName))
		case "string":
			value.Field(k).SetString(m.stringHelper(e, n, columnName))
		case "Time":
			timeVal := m.dateTimeHelper(e, n, columnName)
			value.Field(k).Set(reflect.ValueOf(timeVal))
		case "float64", "float32":
			value.Field(k).SetFloat(m.floatHelper(e, n, columnName))
		default:
			newObject := reflect.New(value.Field(k).Type()).Interface()
			json := m.stringHelper(e, n, columnName)
			err := jsoniter.Unmarshal([]byte(json), &newObject)
			if err != nil {
				return err
			}
			value.Field(k).Set(reflect.ValueOf(newObject).Elem().Convert(value.Field(k).Type()))
		}
	}
	return nil
}

func (m *BinlogParser) dateTimeHelper(e *canal.RowsEvent, n int, columnName string) time.Time {

	columnId := m.getBinlogIdByName(e, columnName)
	if e.Table.Columns[columnId].Type != schema.TYPE_TIMESTAMP {
		panic("Not dateTime type")
	}
	t, _ := time.Parse("2006-01-02 15:04:05", e.Rows[n][columnId].(string))

	return t
}

func (m *BinlogParser) intHelper(e *canal.RowsEvent, n int, columnName string) int64 {

	columnId := m.getBinlogIdByName(e, columnName)
	if e.Table.Columns[columnId].Type != schema.TYPE_NUMBER {
		return 0
	}

	switch v := e.Rows[n][columnId].(type) {
	case int8:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return e.Rows[n][columnId].(int64)
	case int:
		return int64(e.Rows[n][columnId].(int))
	case uint8:
		return int64(e.Rows[n][columnId].(uint8))
	case uint16:
		return int64(e.Rows[n][columnId].(uint16))
	case uint32:
		return int64(e.Rows[n][columnId].(uint32))
	case uint64:
		return int64(e.Rows[n][columnId].(uint64))
	case uint:
		return int64(e.Rows[n][columnId].(uint))
	}
	return 0
}

func (m *BinlogParser) floatHelper(e *canal.RowsEvent, n int, columnName string) float64 {

	columnId := m.getBinlogIdByName(e, columnName)
	if e.Table.Columns[columnId].Type != schema.TYPE_FLOAT {
		panic("Not float type")
	}

	switch e.Rows[n][columnId].(type) {
	case float32:
		return float64(e.Rows[n][columnId].(float32))
	case float64:
		return float64(e.Rows[n][columnId].(float64))
	}
	return float64(0)
}

func (m *BinlogParser) boolHelper(e *canal.RowsEvent, n int, columnName string) bool {

	val := m.intHelper(e, n, columnName)
	return val == 1
}

func (m *BinlogParser) stringHelper(e *canal.RowsEvent, n int, columnName string) string {

	columnId := m.getBinlogIdByName(e, columnName)
	if e.Table.Columns[columnId].Type == schema.TYPE_ENUM {

		values := e.Table.Columns[columnId].EnumValues
		if len(values) == 0 {
			return ""
		}
		if e.Rows[n][columnId] == nil {
			//Если в енум лежит нуул ставим пустую строку
			return ""
		}

		return values[e.Rows[n][columnId].(int64)-1]
	}

	value := e.Rows[n][columnId]

	switch value := value.(type) {
	case []byte:
		return string(value)
	case string:
		return value
	}
	return ""
}

func (m *BinlogParser) getBinlogIdByName(e *canal.RowsEvent, name string) int {
	for id, value := range e.Table.Columns {
		if value.Name == name {
			return id
		}
	}
	panic(fmt.Sprintf("There is no column %s in table %s.%s", name, e.Table.Schema, e.Table.Name))
}

func parseTagSetting(tags reflect.StructTag) map[string]string {
	settings := map[string]string{}
	for _, str := range []string{tags.Get("db")} {
		tags := strings.Split(str, ";")
		for _, value := range tags {
			v := strings.Split(value, ":")

			k := strings.TrimSpace(strings.ToUpper(v[0]))
			if len(v) >= 2 {
				settings[k] = strings.Join(v[1:], ":")
			} else {
				settings[k] = k
			}
		}
	}
	return settings
}
