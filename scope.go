package scope

import (
	"context"

	"github.com/HMasataka/transactor"
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

func NewClient(db transactor.ConnectionProvider, tx transactor.Transactor) *Client {
	return &Client{
		DB: db,
		TX: tx,
	}
}

type Client struct {
	suite.Suite
	DB transactor.ConnectionProvider
	TX transactor.Transactor
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
	conn := c.DB.CurrentConnection(ctx)

	cs := currentScope(ctx)
	cs.deletables = append(cs.deletables, i)

	err := i.Insert(ctx, conn, boil.Infer())
	c.Assert().NoError(err)
}

func (c *Client) DeleteRows(ctx context.Context, deletables ...deletable) error {
	conn := c.DB.CurrentConnection(ctx)

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
