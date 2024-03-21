package pgx_collect_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	pgxc "github.com/zolstein/pgx-collect"
)

func TestCollectRows(t *testing.T) {
	makeExpectedAndRows := func(size int) ([]int, *mockRows) {
		expected := make([]int, size)
		for i := range expected {
			expected[i] = i + 1
		}

		vals := make([]any, size)
		for i, v := range expected {
			vals[i] = v
		}

		rows := MockRows("id", oneCol(vals...))
		return expected, rows
	}

	sizes := []int{0, 1, 2, 7, 1000}
	t.Run("success", func(t *testing.T) {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
				expected, rows := makeExpectedAndRows(size)

				actual, err := pgxc.CollectRows(rows, pgxc.RowTo[int])
				require.NoError(t, err)
				require.Equal(t, expected, actual)
				require.True(t, rows.closed)

				rows.Reset()
				pgxVal, err := pgx.CollectRows(rows, pgx.RowTo[int])
				require.NoError(t, err)
				require.Equal(t, pgxVal, actual)
			})
		}
	})

	t.Run("error", func(t *testing.T) {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
				_, rows := makeExpectedAndRows(size)
				rows.ThenErr(fmt.Errorf("arbitrary error"))

				actual, err := pgxc.CollectRows(rows, pgxc.RowTo[int])
				require.Error(t, err)
				require.Nil(t, actual)
				require.True(t, rows.closed)
			})
		}
	})

	t.Run("panic", func(t *testing.T) {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
				_, rows := makeExpectedAndRows(size)
				rows.ThenPanic(fmt.Errorf("arbitrary error"))

				defer func() {
					err := recover()
					if err == nil {
						require.Fail(t, "Did not panic.")
					}
					require.True(t, rows.closed)
				}()
				pgxc.CollectRows(rows, pgxc.RowTo[int])
				require.Fail(t, "Did not panic.")
			})
		}
	})
}

func TestAppendRows(t *testing.T) {
	makeExpectedAndRows := func(size int) ([]int, *mockRows) {
		expected := make([]int, size)
		for i := range expected {
			expected[i] = i + 1
		}

		vals := make([]any, size)
		for i, v := range expected {
			vals[i] = v
		}

		rows := MockRows("id", oneCol(vals...))
		return expected, rows
	}

	sizes := []int{0, 1, 2, 7, 1000}
	t.Run("success", func(t *testing.T) {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
				expected, rows := makeExpectedAndRows(size)

				base := []int{1, 2, 3}
				unchanged := []int{1, 2, 3}
				expected = append(base, expected...)

				actual, err := pgxc.AppendRows(base, rows, pgxc.RowTo[int])
				require.NoError(t, err)
				require.Equal(t, expected, actual)
				require.True(t, rows.closed)
				require.Equal(t, unchanged, base)

				rows.Reset()
				pgxVal, err := pgx.AppendRows(base, rows, pgx.RowTo[int])
				require.NoError(t, err)
				require.Equal(t, pgxVal, actual)
			})
		}
	})

	t.Run("error", func(t *testing.T) {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
				_, rows := makeExpectedAndRows(size)
				rows.ThenErr(fmt.Errorf("arbitrary error"))

				base := []int{1, 2, 3}
				unchanged := []int{1, 2, 3}

				actual, err := pgxc.AppendRows(base, rows, pgxc.RowTo[int])
				require.Error(t, err)
				require.Nil(t, actual)
				require.True(t, rows.closed)
				require.Equal(t, unchanged, base)
			})
		}
	})

	t.Run("panic", func(t *testing.T) {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
				_, rows := makeExpectedAndRows(size)
				rows.ThenPanic(fmt.Errorf("arbitrary error"))

				base := []int{1, 2, 3}
				unchanged := []int{1, 2, 3}

				defer func() {
					err := recover()
					if err == nil {
						require.Fail(t, "Did not panic.")
					}
					require.True(t, rows.closed)
					require.Equal(t, unchanged, base)
				}()
				pgxc.AppendRows(base, rows, pgxc.RowTo[int])
				require.Fail(t, "Did not panic.")
			})
		}
	})
}

func TestCollectOneRow(t *testing.T) {
	makeRows := func(size int) *mockRows {
		vals := make([]any, size)
		for i := range vals {
			vals[i] = i + 1
		}

		rows := MockRows("id", oneCol(vals...))
		return rows
	}

	sizes := []int{1, 2}
	t.Run("success", func(t *testing.T) {
		for _, size := range sizes {
			t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
				rows := makeRows(size)
				expected := 1

				actual, err := pgxc.CollectOneRow(rows, pgxc.RowTo[int])
				require.NoError(t, err)
				require.Equal(t, expected, actual)
				require.True(t, rows.closed)

				rows.Reset()
				pgxVal, err := pgx.CollectOneRow(rows, pgx.RowTo[int])
				require.NoError(t, err)
				require.Equal(t, pgxVal, actual)
			})
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Run("no-rows", func(t *testing.T) {
			rows := makeRows(0)
			actual, err := pgxc.CollectOneRow(rows, pgxc.RowTo[int])
			require.ErrorIs(t, err, pgx.ErrNoRows)
			require.Zero(t, actual)
		})
		t.Run("scan-err", func(t *testing.T) {
			rows := makeRows(0)
			rows.ThenErr(fmt.Errorf("arbitrary error"))
			actual, err := pgxc.CollectOneRow(rows, pgxc.RowTo[int])
			require.Error(t, err)
			require.NotErrorIs(t, err, pgx.ErrNoRows)
			require.Zero(t, actual)
		})
	})

	t.Run("panic", func(t *testing.T) {
		rows := makeRows(0)
		rows.ThenPanic(fmt.Errorf("arbitrary error"))
		defer func() {
			err := recover()
			if err == nil {
				require.Fail(t, "Did not panic.")
			}
			require.True(t, rows.closed)
		}()
		pgxc.CollectOneRow(rows, pgxc.RowTo[int])
		require.Fail(t, "Did not panic.")
	})
}

func TestCollectExcatlyOneRow(t *testing.T) {
	makeRows := func(size int) *mockRows {
		vals := make([]any, size)
		for i := range vals {
			vals[i] = i + 1
		}

		rows := MockRows("id", oneCol(vals...))
		return rows
	}

	t.Run("success", func(t *testing.T) {
		rows := makeRows(1)
		expected := 1

		actual, err := pgxc.CollectExactlyOneRow(rows, pgxc.RowTo[int])
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		require.True(t, rows.closed)

		rows.Reset()
		pgxVal, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[int])
		require.NoError(t, err)
		require.Equal(t, pgxVal, actual)
	})

	t.Run("error", func(t *testing.T) {
		t.Run("no-rows", func(t *testing.T) {
			rows := makeRows(0)
			actual, err := pgxc.CollectExactlyOneRow(rows, pgxc.RowTo[int])
			require.ErrorIs(t, err, pgx.ErrNoRows)
			require.Zero(t, actual)
		})
		t.Run("too-many-rows", func(t *testing.T) {
			rows := makeRows(2)
			actual, err := pgxc.CollectExactlyOneRow(rows, pgxc.RowTo[int])
			require.ErrorIs(t, err, pgx.ErrTooManyRows)
			require.Zero(t, actual)
		})
		t.Run("scan-err", func(t *testing.T) {
			rows := makeRows(0)
			rows.ThenErr(fmt.Errorf("arbitrary error"))
			actual, err := pgxc.CollectExactlyOneRow(rows, pgxc.RowTo[int])
			require.Error(t, err)
			require.NotErrorIs(t, err, pgx.ErrNoRows)
			require.Zero(t, actual)
		})
	})

	t.Run("panic", func(t *testing.T) {
		rows := makeRows(0)
		rows.ThenPanic(fmt.Errorf("arbitrary error"))
		defer func() {
			err := recover()
			if err == nil {
				require.Fail(t, "Did not panic.")
			}
			require.True(t, rows.closed)
		}()
		pgxc.CollectExactlyOneRow(rows, pgxc.RowTo[int])
		require.Fail(t, "Did not panic.")
	})
}

func TestSimpleRowScanner(t *testing.T) {
	rows := MockRows("id", oneRow(1))
	checkScanOne(t, rows, pgxc.RowTo[int], pgx.RowTo[int], 1)
}

func TestAddrRowScanner(t *testing.T) {
	rows := MockRows("id", oneRow(1))
	checkScanOne(t, rows, pgxc.RowToAddrOf[int], pgx.RowToAddrOf[int], ref(1))
}

func TestMapRowScanner(t *testing.T) {
	rows := MockRows("id,name,age", oneRow(1, "Alice", 30))
	expected := map[string]any{
		"id":   1,
		"name": "Alice",
		"age":  30,
	}
	checkScanOne(t, rows, pgxc.RowToMap, pgx.RowToMap, expected)
}

func TestPositionalStructRowScanner(t *testing.T) {
	{
		type person struct {
			Name string
			Age  int32
		}

		rows := MockRows("name,age", oneRow("Alice", int32(30)))
		expected := person{"Alice", 30}
		checkStructByPosSuccess(t, rows, expected)
	}
	{
		type person struct {
			Name string
			Age  int32 `db:"-"`
		}

		rows := MockRows("name", oneRow("Alice"))
		expected := person{Name: "Alice"}
		checkStructByPosSuccess(t, rows, expected)
	}
	{
		type Name struct {
			First string
			Last  string
		}

		type person struct {
			Name
			Age int32
		}

		rows := MockRows("first_name,last_name,age", oneRow("John", "Smith", int32(25)))
		expected := person{Name{First: "John", Last: "Smith"}, 25}
		checkStructByPosSuccess(t, rows, expected)
	}
	{
		type Sandwich struct {
			Bread string
			Salad string
		}
		type Drink struct {
			Ml int
		}

		type meal struct {
			Sandwich
			Drink
		}

		rows := MockRows("bread,salad,drink_ml", oneRow("Baguette", "Lettuce", 500))
		expected := meal{Sandwich{"Baguette", "Lettuce"}, Drink{500}}
		checkStructByPosSuccess(t, rows, expected)
	}
	{
		type name struct {
			First string
			Last  string
		}

		type person struct {
			name
			Age int32
		}

		rows := MockRows("first_name,last_name,age", oneRow("John", "Smith", int32(25)))
		expected := person{name{"John", "Smith"}, 25}
		checkStructByPosSuccess(t, rows, expected)
	}
	{
		type Name struct {
			First string
			Last  string
		}

		type person struct {
			*Name
			Age int32
		}

		rows := MockRows("first_name,last_name,age", oneRow("John", "Smith", int32(25)))
		checkStructByPosFail[person](t, rows)
	}
}

func checkStructByPosSuccess[T any](t *testing.T, rows *mockRows, expected T) {
	t.Helper()
	rs := pgxc.RowToStructByPos[T]
	rsAddr := pgxc.RowToAddrOfStructByPos[T]
	pgxRowTo := pgx.RowToStructByPos[T]
	pgxRowToAddr := pgx.RowToAddrOfStructByPos[T]
	checkScanOne(t, rows, rs, pgxRowTo, expected)
	rows.Reset()
	checkScanOne(t, rows, rsAddr, pgxRowToAddr, &expected)
}

func checkStructByPosFail[T any](t *testing.T, rows *mockRows) {
	t.Helper()
	rs := pgxc.RowToStructByPos[T]
	rsAddr := pgxc.RowToAddrOfStructByPos[T]
	pgxRowTo := pgx.RowToStructByPos[T]
	pgxRowToAddr := pgx.RowToAddrOfStructByPos[T]
	checkInitFails(t, rows, rs, pgxRowTo)
	rows.Reset()
	checkInitFails(t, rows, rsAddr, pgxRowToAddr)
}

func TestNamedStructRowScanner(t *testing.T) {
	{
		const accID = "d5e49d3f"
		type person struct {
			Last      string
			First     string
			Age       int32
			AccountID string
		}

		{
			rows := MockRows("first,last,age,account_id", oneRow("John", "Smith", int32(25), accID))
			expected := person{"Smith", "John", 25, accID}
			checkStructByNameSuccess(t, rows, expected)
		}
		{
			rows := MockRows("last,age,account_id", oneRow("Smith", int32(25), accID))
			checkStructByNameFail[person](t, rows)
		}
		{
			rows := MockRows(
				"first,last,age,account_id,ignore",
				oneRow("John", "Smith", int32(25), accID, nil),
			)
			checkStructByNameFail[person](t, rows)
		}
	}
	{
		type Name struct {
			Last  string `db:"last_name"`
			First string `db:"first_name"`
		}

		type person struct {
			Ignore bool `db:"-"`
			Name
			Age int32
		}

		{
			rows := MockRows("first_name,last_name,age", oneRow("John", "Smith", int32(25)))
			expected := person{Name: Name{"Smith", "John"}, Age: 25}
			checkStructByNameSuccess(t, rows, expected)
		}
		{
			rows := MockRows("last,age", oneRow("Smith", int32(25)))
			checkStructByNameFail[person](t, rows)
		}
		{
			rows := MockRows("first,last,age,ignore", oneRow("John", "Smith", int32(25), nil))
			checkStructByNameFail[person](t, rows)
		}
		{
			rows := MockRows("last,age", oneRow("Smith", int32(25)))
			checkStructByNameFail[person](t, rows)
		}
	}
}

func checkStructByNameSuccess[T any](t *testing.T, rows *mockRows, expected T) {
	t.Helper()
	rs := pgxc.RowToStructByName[T]
	rsAddr := pgxc.RowToAddrOfStructByName[T]
	pgxRowTo := pgx.RowToStructByName[T]
	pgxRowToAddr := pgx.RowToAddrOfStructByName[T]
	checkScanOne(t, rows, rs, pgxRowTo, expected)
	rows.Reset()
	checkScanOne(t, rows, rsAddr, pgxRowToAddr, &expected)
}

func checkStructByNameFail[T any](t *testing.T, rows *mockRows) {
	t.Helper()
	rs := pgxc.RowToStructByName[T]
	rsAddr := pgxc.RowToAddrOfStructByName[T]
	pgxRowTo := pgx.RowToStructByName[T]
	pgxRowToAddr := pgx.RowToAddrOfStructByName[T]
	checkInitFails(t, rows, rs, pgxRowTo)
	rows.Reset()
	checkInitFails(t, rows, rsAddr, pgxRowToAddr)
}

func TestLaxNamedStructRowScanner(t *testing.T) {
	{
		type person struct {
			Last   string
			First  string
			Age    int32
			Ignore bool `db:"-"`
		}

		{
			rows := MockRows("first,last,age", oneRow("John", "Smith", int32(25)))
			expected := person{"Smith", "John", 25, false}
			checkStructByNameLaxSuccess(t, rows, expected)
		}
		{
			rows := MockRows("first,age", oneRow("John", int32(25)))
			expected := person{"", "John", 25, false}
			checkStructByNameLaxSuccess(t, rows, expected)
		}
		{
			rows := MockRows("first,last,age,ignore", oneRow("John", "Smith", int32(25), nil))
			checkStructByNameLaxFail[person](t, rows)
		}
		{
			rows := MockRows(
				"first,middle,last,age,ignore",
				oneRow("John", "D.", "Smith", int32(25), nil),
			)
			checkStructByNameLaxFail[person](t, rows)
		}
		{
			rows := MockRows("last,age,ignore", oneRow("Smith", int32(25), nil))
			checkStructByNameLaxFail[person](t, rows)
		}
	}
	{
		type Name struct {
			Last  string `db:"last_name"`
			First string `db:"first_name"`
		}

		type person struct {
			Ignore bool `db:"-"`
			Name
			Age int32
		}

		{
			rows := MockRows("first_name,last_name,age", oneRow("John", "Smith", int32(25)))
			expected := person{false, Name{"Smith", "John"}, 25}
			checkStructByNameLaxSuccess(t, rows, expected)
		}
		{
			rows := MockRows("first_name,age", oneRow("John", int32(25)))
			expected := person{false, Name{"", "John"}, 25}
			checkStructByNameLaxSuccess(t, rows, expected)
		}
		{
			rows := MockRows(
				"first_name,last_name,age,ignore",
				oneRow("John", "Smith", int32(25), nil),
			)
			checkStructByNameLaxFail[person](t, rows)
		}
		{
			rows := MockRows(
				"middle_name,last_name,age,ignore",
				oneRow("D.", "Smith", int32(25), nil),
			)
			checkStructByNameLaxFail[person](t, rows)
		}
		{
			rows := MockRows("last_name,age,ignore", oneRow("Smith", int32(25), nil))
			checkStructByNameLaxFail[person](t, rows)
		}
	}
	{
		type AnotherTable struct{}
		type User struct {
			UserID int    `json:"userId" db:"user_id"`
			Name   string `json:"name"   db:"name"`
		}
		type UserAPIKey struct {
			UserAPIKeyID int `json:"userApiKeyId" db:"user_api_key_id"`
			UserID       int `json:"userId"       db:"user_id"`

			User         *User         `json:"user"         db:"user"`
			AnotherTable *AnotherTable `json:"anotherTable" db:"another_table"`
		}

		rows := MockRows("user_api_key_id,user_id,user", oneRow(101, 1, &User{1, "John Doe"}))
		expected := UserAPIKey{101, 1, &User{1, "John Doe"}, nil}
		checkStructByNameLaxSuccess(t, rows, expected)
	}
}

func checkStructByNameLaxSuccess[T any](t *testing.T, rows *mockRows, expected T) {
	t.Helper()
	rs := pgxc.RowToStructByNameLax[T]
	rsAddr := pgxc.RowToAddrOfStructByNameLax[T]
	pgxRowTo := pgx.RowToStructByNameLax[T]
	pgxRowToAddr := pgx.RowToAddrOfStructByNameLax[T]
	checkScanOne(t, rows, rs, pgxRowTo, expected)
	rows.Reset()
	checkScanOne(t, rows, rsAddr, pgxRowToAddr, &expected)
}

func checkStructByNameLaxFail[T any](t *testing.T, rows *mockRows) {
	t.Helper()
	rs := pgxc.RowToStructByNameLax[T]
	rsAddr := pgxc.RowToAddrOfStructByNameLax[T]
	pgxRowTo := pgx.RowToStructByNameLax[T]
	pgxRowToAddr := pgx.RowToAddrOfStructByNameLax[T]
	checkInitFails(t, rows, rs, pgxRowTo)
	rows.Reset()
	checkInitFails(t, rows, rsAddr, pgxRowToAddr)
}

func checkScanOne[T any](
	t *testing.T,
	rows *mockRows,
	rowSpec pgxc.RowSpec[T],
	rowTo pgx.RowToFunc[T],
	expected T,
) {
	t.Helper()
	rs := rowSpec.Scanner()
	err := rs.Initialize(rows)
	require.NoError(t, err)
	rows.Next()
	var val T
	err = rs.ScanRowInto(&val, rows)
	require.NoError(t, err)
	require.Equal(t, expected, val)
	// Also check that we match vanilla pgx.
	pgxVal, pgxErr := rowTo(rows)
	require.NoError(t, pgxErr)
	require.Equal(t, pgxVal, val)
}

func checkInitFails[T any](
	t *testing.T,
	rows *mockRows,
	rowSpec pgxc.RowSpec[T],
	rowTo pgx.RowToFunc[T],
) {
	t.Helper()
	rs := rowSpec.Scanner()
	err := rs.Initialize(rows)
	require.Error(t, err)
	rows.Next()
	_, pgxErr := rowTo(rows)
	require.Error(t, pgxErr)
	require.Equal(t, err, pgxErr)
}

func ref[T any](val T) *T {
	return &val
}

type rows [][]any

func oneCol[T any](vals ...T) rows {
	rows := make(rows, len(vals))
	for i, v := range vals {
		rows[i] = []any{v}
	}
	return rows
}

func oneRow(vals ...any) rows {
	return rows{vals}
}

type mockRows struct {
	descs  []pgconn.FieldDescription
	data   rows
	rowIdx int
	done   bool
	err    error
	closed bool
	panic  bool
}

var _ pgx.Rows = (*mockRows)(nil)

func MockRows(fieldNames string, data [][]any) *mockRows {
	fields := strings.Split(fieldNames, ",")
	descs := make([]pgconn.FieldDescription, len(fields))
	for i, s := range fields {
		descs[i].Name = s
	}
	return &mockRows{
		descs:  descs,
		data:   data,
		rowIdx: -1,
		done:   false,
	}
}

func (m *mockRows) ThenErr(err error) {
	m.err = err
}

func (m *mockRows) ThenPanic(err error) {
	m.err = err
	m.panic = true
}

func (m *mockRows) Reset() {
	m.rowIdx = -1
	m.done = false
	m.closed = false
}

func (m *mockRows) FieldDescriptions() []pgconn.FieldDescription {
	return m.descs
}

func (m *mockRows) Err() error {
	if !m.done {
		panic("Err called before done")
	}
	return m.err
}

func (m *mockRows) Next() bool {
	m.rowIdx++
	off := 0
	if m.err != nil {
		off = 1
	}
	m.done = m.rowIdx >= len(m.data)+off
	return !m.done
}

func (m *mockRows) Scan(dest ...any) error {
	// This is necessary to work with pgx.RowTo.
	if len(dest) == 1 {
		if rs, ok := dest[0].(pgx.RowScanner); ok {
			return rs.ScanRow(m)
		}
	}
	if m.done {
		panic("Scan called after done")
	}
	if m.rowIdx == len(m.data) {
		if m.panic {
			panic(m.err)
		}
		return m.err
	}
	data := m.data[m.rowIdx]
	if len(data) != len(dest) {
		return fmt.Errorf("len(data) (%v) != len(dest) (%v)", data, dest)
	}
	for i, d := range dest {
		elem := data[i]
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(elem))
	}
	return nil
}

func (m *mockRows) Values() ([]any, error) {
	if m.done {
		panic("Scan called after done")
	}
	src := m.data[m.rowIdx]
	dst := make([]any, len(src))
	copy(dst, src)
	return dst, nil
}

func (m *mockRows) Close() {
	m.done = true
	m.closed = true
}

func (m *mockRows) RawValues() [][]byte {
	// We need to implement this much in order to work with pgx.RowToStructByPos
	return make([][]byte, len(m.descs))
}

func (m *mockRows) Conn() *pgx.Conn {
	panic("mockRows does not implement Conn")
}

func (m *mockRows) CommandTag() pgconn.CommandTag {
	if !m.done {
		panic("CommandTag called before done")
	}
	panic("mockRows does not implement CommandTag")
}
