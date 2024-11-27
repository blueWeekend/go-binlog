package binlog

import (
	"encoding/json"

	"github.com/dgraph-io/badger/v3"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type PositionHandler interface {
	UpdatePos(pos mysql.Position) error
	GetLatestPos() (mysql.Position, error)
}

type DefaultPosHandler struct {
	badgerCli *badger.DB
	dataKey   []byte
}

func NewDefaultPosHandler(dir string) (*DefaultPosHandler, error) {
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		return nil, err
	}
	return &DefaultPosHandler{
		badgerCli: db,
		dataKey:   []byte("binlog_pos"),
	}, nil
}

func (d *DefaultPosHandler) close() {
	d.badgerCli.Close()
}

func (d *DefaultPosHandler) UpdatePos(pos mysql.Position) error {
	return d.badgerCli.Update(func(txn *badger.Txn) error {
		posJson, err := json.Marshal(pos)
		if err != nil {
			return err
		}
		return txn.Set(d.dataKey, posJson)
	})
}

func (d *DefaultPosHandler) GetLatestPos() (pos mysql.Position, err error) {
	err = d.badgerCli.View(func(txn *badger.Txn) error {
		item, err := txn.Get(d.dataKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) > 0 {
				return json.Unmarshal(val, &pos)
			}
			return nil
		})
	})
	return
}
