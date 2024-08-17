package binlog

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type EventHandler interface {
	DbName() string
	TableName() string
	OnUpdate(from, to any)
	OnDelete(datas ...any)
	OnInsert(datas ...any)
	Schema() any
}

type BinlogHandler struct {
	canal.DummyEventHandler
	BinlogParser
	eventMap sync.Map
	config   *Config
	canalCli *canal.Canal
	errors   chan error
}

func (b *BinlogHandler) OnRow(e *canal.RowsEvent) error {
	var err error
	defer func() {
		if panic := recover(); panic != nil {
			b.handlerError(errors.New("panic: " + fmt.Sprint(panic)))
		}
		if err != nil {
			b.handlerError(err)
		}
	}()
	val, ok := b.eventMap.Load(e.Table.Schema + "." + e.Table.Name)
	if !ok {
		return nil
	}
	hander := val.(EventHandler)
	var n = 0
	var step = 1
	var inserts, deletes []any
	if e.Action == canal.UpdateAction {
		n = 1
		step = 2
	} else {
		inserts = make([]any, 0, len(e.Rows))
		deletes = make([]any, 0, len(e.Rows))
	}
	for i := n; i < len(e.Rows); i += step {
		data := hander.Schema()
		err = b.GetBinLogData(data, e, i)
		if err != nil {
			return err
		}
		switch e.Action {
		case canal.UpdateAction:
			oldData := hander.Schema()
			err = b.GetBinLogData(oldData, e, i-1)
			if err != nil {
				return err
			}
			hander.OnUpdate(oldData, data)
		case canal.InsertAction:
			inserts = append(inserts, data)
		case canal.DeleteAction:
			deletes = append(deletes, data)
		default:
			err = errors.New("unknown action of onRow")
			return err
		}
	}
	if len(inserts) > 0 {
		hander.OnInsert(inserts...)
	}
	if len(deletes) > 0 {
		hander.OnDelete(deletes...)
	}
	return nil
}

func (h *BinlogHandler) String() string {
	return "BinlogHandler"
}

func NewBinlogLister(config *Config) (*BinlogHandler, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Addr
	cfg.User = config.User
	cfg.Password = config.Password
	cfg.Dump.ExecutionPath = ""
	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, err
	}
	if config.ColumnTag == "" {
		config.ColumnTag = "db"
	}
	if config.PosHandler == nil {
		posHandler, err := NewDefaultPosHandler("./binlog_position")
		if err != nil {
			return nil, err
		}
		config.PosHandler = posHandler
	}

	lister := &BinlogHandler{
		BinlogParser: BinlogParser{
			columnTag: config.ColumnTag,
		},
		canalCli: c,
		errors:   make(chan error, 1),
		config:   config,
	}
	lister.canalCli.SetEventHandler(lister)
	return lister, nil
}

func (b *BinlogHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, _ mysql.GTIDSet, _ bool) error {
	err := b.config.PosHandler.UpdatePos(pos)
	if err != nil {
		b.handlerError(err)
	}
	return err
}

func (b *BinlogHandler) OnRotate(header *replication.EventHeader, event *replication.RotateEvent) error {
	err := b.config.PosHandler.UpdatePos(mysql.Position{
		Pos:  uint32(event.Position),
		Name: string(event.NextLogName),
	})
	if err != nil {
		b.handlerError(err)
	}
	return err
}

func (b *BinlogHandler) RegisterEventHandler(e EventHandler) {
	value := reflect.ValueOf(e.Schema())
	kind := value.Kind()
	if kind != reflect.Ptr {
		panic(fmt.Errorf("expected struct pointer, got %s", kind.String()))
	}
	key := e.DbName() + "." + e.TableName()
	b.eventMap.Store(key, e)
}

func (b *BinlogHandler) Run() error {
	masterPos, err := b.canalCli.GetMasterPos()
	if err != nil {
		return err
	}
	pos, err := b.config.PosHandler.GetLatestPos()
	if err != nil {
		b.handlerError(err)
	}
	if pos.Name != "" {
		return b.canalCli.RunFrom(pos)
	}
	defer func() {
		if handler, ok := b.config.PosHandler.(*DefaultPosHandler); ok {
			handler.close()
		}
		b.canalCli.Close()
	}()
	return b.canalCli.RunFrom(masterPos)
}

func (b *BinlogHandler) handlerError(err error) {
	select {
	case b.errors <- err:
	default:
	}
}

func (b *BinlogHandler) Errors() <-chan error {
	return b.errors
}
