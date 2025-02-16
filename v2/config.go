package binlog

type Config struct {
	Addr     string
	User     string
	Password string

	ColumnTag string

	PosHandler      PositionHandler
}
