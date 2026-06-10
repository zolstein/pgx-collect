package pgx_collect_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	pgxc "github.com/zolstein/pgx-collect"
)

type Person struct {
	Name string
	Age  sql.Null[int64]
}

type Record struct {
	ID int64
	Person
	CreatedAt time.Time
}

const N = 1000

var conn *pgx.Conn

func TestMain(m *testing.M) {
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	ctx := context.Background()
	container, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:16"),
		postgres.WithDatabase("bench"),
		postgres.WithUsername("user"),
		postgres.WithPassword("pass"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return 0, err
	}
	defer container.Terminate(ctx)
	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 0, err
	}
	conn, err = pgx.Connect(ctx, connStr)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	records := generatePeople(N)
	err = seedPeople(ctx, records)
	if err != nil {
		return 0, err
	}
	records = nil

	return m.Run(), nil
}

func BenchmarkPgxCollectAppendRows(b *testing.B) {
	ctx := context.Background()
	records := make([]Record, N)
	for i := 0; i < b.N; i++ {
		records = records[:0]
		rows, err := conn.Query(ctx, "select id, name, age, created_at from people") // Returns 1000 rows
		if err != nil {
			b.Fatal(err)
		}
		records, err := pgxc.AppendRows(records, rows, pgxc.RowToStructByName[Record])
		if err != nil {
			b.Fatal(err)
		}
		if len(records) != N {
			b.Fatal()
		}
	}

}

func BenchmarkPgxAppendRows(b *testing.B) {
	ctx := context.Background()
	records := make([]Record, N)
	for i := 0; i < b.N; i++ {
		records = records[:0]
		rows, err := conn.Query(ctx, "select id, name, age, created_at from people") // Returns 1000 rows
		if err != nil {
			b.Fatal(err)
		}
		records, err := pgx.AppendRows(records, rows, pgx.RowToStructByName[Record])
		if err != nil {
			b.Fatal(err)
		}
		if len(records) != N {
			b.Fatal()
		}
	}
}

var adjectives = []string{"happy", "sad", "fast", "slow", "bright", "dark", "tall", "short"}
var nouns = []string{"alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi"}

func generatePeople(n int) []Record {
	rng := rand.New(rand.NewPCG(42, 42)) // fixed seed = reproducible
	records := make([]Record, n)
	for i := range records {
		records[i] = Record{
			ID: int64(i + 1),
			Person: Person{
				Name: adjectives[rng.IntN(len(adjectives))] + "_" + nouns[rng.IntN(len(nouns))],
				Age: func() sql.Null[int64] {
					if rng.IntN(10) == 0 { // ~10% NULL ages
						return sql.Null[int64]{}
					}
					return sql.Null[int64]{V: int64(18 + rng.IntN(65)), Valid: true}
				}(),
			},
			CreatedAt: time.Now().Add(-time.Duration(rng.IntN(365*24)) * time.Hour),
		}
	}
	return records
}

func seedPeople(ctx context.Context, records []Record) error {
	_, err := conn.Exec(ctx, `
		DROP TABLE IF EXISTS people;
		CREATE TABLE people (
			id         BIGINT PRIMARY KEY,
			name       TEXT        NOT NULL,
			age        BIGINT,
			created_at TIMESTAMPTZ NOT NULL
		);
	`)
	if err != nil {
		return err
	}
	_, err = conn.CopyFrom(
		ctx,
		pgx.Identifier{"people"},
		[]string{"id", "name", "age", "created_at"},
		pgx.CopyFromSlice(len(records), func(i int) ([]any, error) {
			r := records[i]
			var age any
			if r.Age.Valid {
				age = r.Age.V
			}
			return []any{r.ID, r.Name, age, r.CreatedAt}, nil
		}),
	)
	return err
}
