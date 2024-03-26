package testutils

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type rows [][]any

func OneCol[T any](vals ...T) rows {
	rows := make(rows, len(vals))
	for i, v := range vals {
		rows[i] = []any{v}
	}
	return rows
}

func OneRow(vals ...any) rows {
	return rows{vals}
}

type MockRows struct {
	descs  []pgconn.FieldDescription
	data   rows
	rowIdx int
	done   bool
	err    error
	closed bool
	panic  bool
}

var _ pgx.Rows = (*MockRows)(nil)

func MakeMockRows(fieldNames string, data [][]any) *MockRows {
	fields := strings.Split(fieldNames, ",")
	descs := make([]pgconn.FieldDescription, len(fields))
	for i, s := range fields {
		descs[i].Name = s
	}
	return &MockRows{
		descs:  descs,
		data:   data,
		rowIdx: -1,
		done:   false,
	}
}

func (m *MockRows) ThenErr(err error) {
	m.err = err
}

func (m *MockRows) ThenPanic(err error) {
	m.err = err
	m.panic = true
}

func (m *MockRows) Reset() {
	m.rowIdx = -1
	m.done = false
	m.closed = false
}

func (m *MockRows) FieldDescriptions() []pgconn.FieldDescription {
	return m.descs
}

func (m *MockRows) Err() error {
	if !m.done {
		panic("Err called before done")
	}
	return m.err
}

func (m *MockRows) Next() bool {
	m.rowIdx++
	off := 0
	if m.err != nil {
		off = 1
	}
	m.done = m.rowIdx >= len(m.data)+off
	return !m.done
}

func (m *MockRows) Scan(dest ...any) error {
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

func (m *MockRows) Values() ([]any, error) {
	if m.done {
		panic("Scan called after done")
	}
	src := m.data[m.rowIdx]
	dst := make([]any, len(src))
	copy(dst, src)
	return dst, nil
}

func (m *MockRows) Close() {
	m.done = true
	m.closed = true
}

func (m *MockRows) RawValues() [][]byte {
	// We need to implement this much in order to work with pgx.RowToStructByPos
	return make([][]byte, len(m.descs))
}

func (m *MockRows) Conn() *pgx.Conn {
	panic("MockRows does not implement Conn")
}

func (m *MockRows) CommandTag() pgconn.CommandTag {
	if !m.done {
		panic("CommandTag called before done")
	}
	panic("MockRows does not implement CommandTag")
}

func (m *MockRows) IsClosed() bool {
	return m.closed
}

func Ref[T any](val T) *T {
	return &val
}
