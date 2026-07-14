package testutil

import (
	"embed"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/VitoNaychev/egt-challenge/persistence/repo/migrations"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const postgresConnectTimeout = 30 * time.Second

var migrationsFS embed.FS

func SetupPostgres(t testing.TB) (*pgxpool.Pool, func()) {
	t.Helper()

	postgresContainer, err := postgres.Run(t.Context(),
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(postgresConnectTimeout),
		),
	)
	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}

	connStr, err := postgresContainer.ConnectionString(t.Context(), "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %s", err)
	}

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("failed to parse connection string: %s", err)
	}

	poolConfig.MinConns = 1
	poolConfig.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(t.Context(), poolConfig)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %s", err)
	}

	migrateUp(t, connStr)

	return pool, func() {
		pool.Close()
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}
}

func migrateUp(t testing.TB, connStr string) {
	t.Helper()

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		t.Fatalf("load migrations: %s", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", src,
		strings.Replace(connStr, "postgres://", "pgx5://", 1))
	if err != nil {
		t.Fatalf("create migrator: %s", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil {
		t.Fatalf("run migrations: %s", err)
	}
}
