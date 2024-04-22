package cloudsql

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/buildpeak/sqltestutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

// localPostgresDialer is a dialer that connects to a local postgres instance started as a container
var localPostgresDialer = func(cfg *pgx.ConnConfig) pgconn.DialFunc {
	return func(ctx context.Context, network, instance string) (net.Conn, error) {
		return net.Dial("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port))
	}
}

func TestNewConnection(t *testing.T) {
	ctx := context.Background()

	// Start a local postgres container
	pg, err := sqltestutil.StartPostgresContainer(ctx, "13")
	if err != nil {
		t.Errorf("Error starting postgres container: %v", err)
		return
	}

	t.Cleanup(func() {
		err := pg.Shutdown(ctx)
		assert.NoError(t, err)
	})

	cfg, err := pgx.ParseConfig(pg.ConnectionString())
	assert.NoError(t, err)

	conn := &Connection{
		User:     cfg.User,
		Password: cfg.Password,
		Database: cfg.Database,
		URL:      pg.ConnectionString(),
	}
	fmt.Printf("Connection: %v\n", conn)

	db, err := conn.Connect(WithDialFunc(localPostgresDialer(cfg)))
	assert.NoError(t, err)
	if err != nil {
		t.Errorf("Error connecting to database: %v", err)
	}
	defer db.Close()

	_, err = db.Query("SELECT tablename from pg_catalog.pg_tables")
	assert.NoError(t, err)
}
