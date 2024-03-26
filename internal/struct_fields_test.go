package pgx_collect_test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	pgxc "github.com/zolstein/pgx-collect"
	pgxc_internal "github.com/zolstein/pgx-collect/internal"
	. "github.com/zolstein/pgx-collect/internal/testutils"
)

func TestNamedStructHashCollision(t *testing.T) {
	type HashCollision struct {
		F1 int `db:"7mohtcOFVz"`
		F2 int `db:"c1E51sSEyx"`
	}

	rows1 := MakeMockRows("7mohtcOFVz", OneRow(100))
	rows2 := MakeMockRows("c1E51sSEyx", OneRow(100))
	expected1 := HashCollision{F1: 100}
	expected2 := HashCollision{F2: 100}
	checkScanOne(t, rows1, expected1)
	checkScanOne(t, rows2, expected2)
	collisions := pgxc_internal.CollidingFieldSets(
		reflect.TypeOf(HashCollision{}),
		[]string{"7mohtcOFVz"},
	)
	require.Len(t, collisions, 2)
}

func TestNamedStructHashCollisionTorture(t *testing.T) {
	type HashCollision struct {
		F1 int `db:"7mohtcOFVz"`
		F2 int `db:"c1E51sSEyx"`
	}

	fields := []string{
		"7mohtcOFVz",
		"c1E51sSEyx",
	}
	expected := []HashCollision{
		{F1: 100},
		{F2: 100},
	}

	for i := 0; i < 1000; i++ {
		pgxc_internal.ClearStructFieldCaches()
		numWorkers := 10
		var wgSignal sync.WaitGroup
		var wgDone sync.WaitGroup
		wgSignal.Add(1)
		wgDone.Add(numWorkers)
		for i := 0; i < numWorkers; i++ {
			go func(idx int) {
				rows := MakeMockRows(fields[idx], OneRow(100))
				wgSignal.Wait()
				checkScanOne(t, rows, expected[idx])
				wgDone.Done()
			}(i % 2)
		}
		wgSignal.Done()
		wgDone.Wait()

		collisions := pgxc_internal.CollidingFieldSets(reflect.TypeOf(HashCollision{}), fields[:1])
		require.Len(t, collisions, 2)
	}
}

func checkScanOne[T any](
	t *testing.T,
	rows *MockRows,
	expected T,
) {
	t.Helper()
	var rowSpec pgxc.RowSpec[T] = pgxc.RowToStructByNameLax[T]
	rs := rowSpec.Scanner()
	err := rs.Initialize(rows)
	require.NoError(t, err)
	rows.Next()
	var val T
	err = rs.ScanRowInto(&val, rows)
	require.NoError(t, err)
	require.Equal(t, expected, val)
}
