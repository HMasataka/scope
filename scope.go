package scope

import (
	"context"
	"database/sql"
	"time"

	"github.com/HMasataka/transactor"
	"github.com/HMasataka/transactor/rdbms"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/suite"
	"github.com/volatiletech/sqlboiler/v4/boil"
)

const contextScopeKey = "current_scope"

type scope struct {
	deletables []any
}

func withScope(ctx context.Context, s *scope) context.Context {
	return context.WithValue(ctx, contextScopeKey, s)
}

func currentScope(ctx context.Context) *scope {
	v := ctx.Value(contextScopeKey)
	if v != nil {
		return v.(*scope)
	}
	return nil
}

type insertable interface {
	Insert(context.Context, boil.ContextExecutor, boil.Columns) error
}

type deletable interface {
	Delete(ctx context.Context, exec boil.ContextExecutor) (int64, error)
}

type softDeletable interface {
	Delete(ctx context.Context, exec boil.ContextExecutor, hardDelete bool) (int64, error)
}

type sqlboilerModel interface {
	insertable
	deletable
}

type sqlboilerSoftModel interface {
	insertable
	softDeletable
}

func NewClient() *Client {
	return &Client{}
}

type Client struct {
	suite.Suite
	conn *sql.DB
	Conn rdbms.ClientProvider
	TX   transactor.Transactor
}

type DatabaseConnecter interface {
	Connect() (*sql.DB, error)
}

type DefaultDatabaseConnector struct{}

func (d DefaultDatabaseConnector) Connect() (*sql.DB, error) {
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		return nil, err
	}

	c := mysql.Config{
		DBName:    "db",
		User:      "user",
		Passwd:    "password",
		Addr:      "localhost:3306",
		Net:       "tcp",
		ParseTime: true,
		Collation: "utf8mb4_unicode_ci",
		Loc:       jst,
	}

	db, err := sql.Open("mysql", c.FormatDSN())
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (c *Client) Init(dbConnector DatabaseConnecter) error {
	conn, err := dbConnector.Connect()
	if err != nil {
		return err
	}

	c.conn = conn
	db := rdbms.NewConnectionProvider(c.conn)
	c.Conn = rdbms.NewClientProvider(db)
	c.TX = rdbms.NewTransactor(db)

	return nil
}

func (c *Client) Term() {
	err := c.conn.Close()
	c.Assert().NoError(err)
}

func (c *Client) Scoped(ctx context.Context, fn func(ctx context.Context)) {
	s := &scope{}
	ctx = withScope(ctx, s)
	defer func() {
		if len(s.deletables) == 0 {
			return
		}
		err := c.DeleteRows(ctx, s.deletables...)
		c.Assert().NoError(err)
	}()
	fn(ctx)
}

func (c *Client) Insert(ctx context.Context, i sqlboilerModel) {
	conn := c.Conn.CurrentClient(ctx)

	cs := currentScope(ctx)
	cs.deletables = append(cs.deletables, i)

	err := i.Insert(ctx, conn, boil.Infer())
	c.Assert().NoError(err)
}

func (c *Client) InsertSoftModel(ctx context.Context, i sqlboilerSoftModel) {
	conn := c.Conn.CurrentClient(ctx)

	cs := currentScope(ctx)
	cs.deletables = append(cs.deletables, i)

	err := i.Insert(ctx, conn, boil.Infer())
	c.Assert().NoError(err)
}

func (c *Client) DeleteRows(ctx context.Context, deletables ...any) error {
	conn := c.Conn.CurrentClient(ctx)

	return c.TX.Required(ctx, func(ctx context.Context) error {
		for i := len(deletables); i > 0; i-- {
			d, ok := deletables[i-1].(deletable)
			if ok {
				_, err := d.Delete(ctx, conn)
				if err != nil {
					return err
				}
			}

			sd, ok := deletables[i-1].(softDeletable)
			if ok {
				_, err := sd.Delete(ctx, conn, true)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}
