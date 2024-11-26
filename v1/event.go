package binlog

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type EventHandler interface {
	DbName() string
	TableName() string
	OnUpdate(datas ...UpdateHandler)
	OnDelete(datas ...any)
	OnInsert(datas ...any)
	Schema() any
}

type BinlogHandler struct {
	canal.DummyEventHandler
	BinlogParser
	eventMap map[string]EventHandler
	config   *Config
	canalCli *canal.Canal
	errors   chan error
	running  bool
}

type rowsEvent struct {
	*canal.RowsEvent
	tableKey string
}

type UpdateHandler struct {
	From any
	To   any
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
	event := &rowsEvent{
		RowsEvent: e,
		tableKey:  e.Table.Schema + "." + e.Table.Name,
	}
	hander, ok := b.eventMap[event.tableKey]
	if !ok {
		return nil
	}
	var n = 0
	var step = 1
	var inserts, deletes []any
	var updateHandlers []UpdateHandler
	if e.Action == canal.UpdateAction {
		n = 1
		step = 2
	} else {
		inserts = make([]any, 0, len(e.Rows))
		deletes = make([]any, 0, len(e.Rows))
	}
	for i := n; i < len(e.Rows); i += step {
		data := hander.Schema()
		err = b.GetBinLogData(data, event, i)
		if err != nil {
			return err
		}
		switch e.Action {
		case canal.UpdateAction:
			oldData := hander.Schema()
			err = b.GetBinLogData(oldData, event, i-1)
			if err != nil {
				return err
			}
			updateHandlers = append(updateHandlers, UpdateHandler{
				From: oldData,
				To:   data,
			})
		case canal.InsertAction:
			inserts = append(inserts, data)
		case canal.DeleteAction:
			deletes = append(deletes, data)
		default:
			err = errors.New("unknown action of onRow")
			return err
		}
	}
	if len(updateHandlers) > 0 {
		hander.OnUpdate(updateHandlers...)
		return nil
	}
	if len(inserts) > 0 {
		hander.OnInsert(inserts...)
		return nil
	}
	if len(deletes) > 0 {
		hander.OnDelete(deletes...)
		return nil
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
			onceMap:   make(map[string]*tableSchema, 16),
		},
		canalCli: c,
		errors:   make(chan error, 1),
		config:   config,
		eventMap: make(map[string]EventHandler, 16),
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
	if b.running {
		panic("can not register event handler after Run")
	}
	value := reflect.ValueOf(e.Schema())
	kind := value.Kind()
	if kind != reflect.Ptr {
		panic(fmt.Errorf("expected struct pointer, got %s", kind.String()))
	}
	key := e.DbName() + "." + e.TableName()
	b.eventMap[key] = e
	b.registerOnce(key)
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
	defer b.Close()
	b.running = true
	return b.canalCli.RunFrom(masterPos)
}

func (b *BinlogHandler) Close() {
	if !b.running {
		return
	}
	b.running = false
	b.canalCli.Close()
	if handler, ok := b.config.PosHandler.(*DefaultPosHandler); ok {
		handler.close()
	}
	close(b.errors)
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
