package pgx_collect_test

import (
	"database/sql"
	"testing"
	"time"

	pgxc "github.com/zolstein/pgx-collect"
	pgxc_internal "github.com/zolstein/pgx-collect/internal"
	. "github.com/zolstein/pgx-collect/internal/testutils"
)

func BenchmarkInitialize(b *testing.B) {
	type Person struct {
		Name string
		Age  sql.NullInt64
	}

	type Record struct {
		ID int64
		Person
		CreatedAt time.Time
	}

	rows := MakeMockRows("id,name,age,created_at", nil)

	b.Run("cold", func(b *testing.B) {
		b.Run("struct-by-pos", func(b *testing.B) {
			var rowTo pgxc.RowSpec[Record] = pgxc.RowToStructByPos[Record]
			scanner := rowTo.Scanner()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pgxc_internal.ClearStructFieldCaches()
				scanner.Initialize(rows)
			}
		})

		b.Run("struct-by-name", func(b *testing.B) {
			var rowTo pgxc.RowSpec[Record] = pgxc.RowToStructByName[Record]
			scanner := rowTo.Scanner()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pgxc_internal.ClearStructFieldCaches()
				scanner.Initialize(rows)
			}
		})

		b.Run("struct-by-name-lax", func(b *testing.B) {
			var rowTo pgxc.RowSpec[Record] = pgxc.RowToStructByNameLax[Record]
			scanner := rowTo.Scanner()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pgxc_internal.ClearStructFieldCaches()
				scanner.Initialize(rows)
			}
		})
	})

	b.Run("hot", func(b *testing.B) {
		b.Run("struct-by-pos", func(b *testing.B) {
			var rowTo pgxc.RowSpec[Record] = pgxc.RowToStructByPos[Record]
			scanner := rowTo.Scanner()
			scanner.Initialize(rows) // First time to populate cache
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scanner.Initialize(rows)
			}
		})

		b.Run("struct-by-name", func(b *testing.B) {
			var rowTo pgxc.RowSpec[Record] = pgxc.RowToStructByName[Record]
			scanner := rowTo.Scanner()
			scanner.Initialize(rows) // First time to populate cache
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scanner.Initialize(rows)
			}
		})

		b.Run("struct-by-name-lax", func(b *testing.B) {
			var rowTo pgxc.RowSpec[Record] = pgxc.RowToStructByNameLax[Record]
			scanner := rowTo.Scanner()
			scanner.Initialize(rows) // First time to populate cache
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scanner.Initialize(rows)
			}
		})
	})
}
