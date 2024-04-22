package cloudsql

import (
	"context"
	"database/sql"
	"fmt"
	"net"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

type Connection struct {
	User     string
	Password string
	Database string
	URL      string // e.g. 'project:region:instance'

	dialFunc pgconn.DialFunc
}

type Option func(*Connection)

func WithDialFunc(dialFunc pgconn.DialFunc) Option {
	return func(c *Connection) {
		c.dialFunc = dialFunc
	}
}

func (c Connection) Connect(connOpts ...Option) (*sql.DB, error) {
	for _, opt := range connOpts {
		opt(&c)
	}
	dsn := fmt.Sprintf("user=%s password=%s database=%s", c.User, c.Password, c.Database)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	var opts []cloudsqlconn.Option
	d, err := cloudsqlconn.NewDialer(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	if c.dialFunc == nil {
		c.dialFunc = func(ctx context.Context, network, instance string) (net.Conn, error) {
			return d.Dial(ctx, c.URL)
		}
	}
	// Use the Cloud SQL connector to handle connecting to the instance.
	// This approach does *NOT* require the Cloud SQL proxy.
	config.DialFunc = c.dialFunc
	dbURI := stdlib.RegisterConnConfig(config)
	dbPool, err := sql.Open("pgx", dbURI)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	return dbPool, nil
}
