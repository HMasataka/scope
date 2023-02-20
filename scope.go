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
	deletables []deletable
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

type sqlboilerModel interface {
	insertable
	deletable
}

func NewClient(db rdbms.ConnectionProvider, tx transactor.Transactor) *Client {
	return &Client{
		DB: db,
		TX: tx,
	}
}

type Client struct {
	suite.Suite
	conn *sql.DB
	Conn rdbms.ClientProvider
	TX   transactor.Transactor
}

func connectDB() *sql.DB {
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		panic(err)
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
		panic(err)
	}

	return db
}

func (c *Client) Init() {
	c.conn = connectDB()
	db := rdbms.NewConnectionProvider(c.conn)
	c.Conn = rdbms.NewClientProvider(db)
	c.TX = rdbms.NewTransactor(db)
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

func (c *Client) DeleteRows(ctx context.Context, deletables ...deletable) error {
	conn := c.Conn.CurrentClient(ctx)

	return c.TX.Required(ctx, func(ctx context.Context) error {
		for _, deletable := range deletables {
			_, err := deletable.Delete(ctx, conn)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
