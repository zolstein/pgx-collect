package pgx_collect

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const structTagKey = "db"

// Scanner provides an interface for scanning a row into a receiver value,
// while doing type-checking and initialization only once per query.
type Scanner[T any] interface {
	// Initialize sets up the Scanner and validates it against the rows.
	// Initialize must be called once before ScanRowInto.
	Initialize(rows pgx.Rows) error
	// ScanRowInto scans the row into the receiver.
	ScanRowInto(receiver *T, rows pgx.Rows) error
}

// RowSpec defines a specification for scanning rows into a given type.
//
// Note on the weird type definitions:
// RowSpec returns a struct containing a private function pointer because:
//  1. We want to be able to manage the lifecycle of the returned value inside the
//     collection functions. (E.g. we may decide to pool scanners for reuse.)
//     In order to do this safely, we need to ensure the Scanner returned by
//     the inner function isn't referenced outside of the collecting function.
//     This requires that we have a function returning a scanner.
//  2. Returning a struct allows us to extend this value in the future if necessary.
//     By comparison, returning a function would not, and would require a (technically)
//     breaking change if the type needed to change in the future.
//  3. Returning a non-exported type lets us hide as many details as possible from
//     the public API and restrict the only valid usage to:
//     pgx.CollectRows(rows, RowTo[Type])
//  4. RowSpec is itself a function to provide a place to put the generic type
//     parameter. rowSpecRes cannot be a constant, since then there would be no
//     place to put the type parameter. Since rowSpecRes cannot be constructed in
//     client code (by design) it can't be applied when creating a struct value.
type RowSpec[T any] func() rowSpecRes[T]

type rowSpecRes[T any] struct {
	fn func() Scanner[T]
}

func (rs RowSpec[T]) Scanner() Scanner[T] {
	return rs().fn()
}

// AppendRows iterates through rows, scanning each row according to into,
// and appending the results into a slice of T.
func AppendRows[T any, S ~[]T](slice S, rows pgx.Rows, into RowSpec[T]) (S, error) {
	return AppendRowsUsing(slice, rows, into().fn())
}

// AppendRowsUsing iterates through rows, scanning each row with the scanner,
// and appending the results into a slice of T.
func AppendRowsUsing[T any, S ~[]T](
	slice S,
	rows pgx.Rows,
	scanner Scanner[T],
) (s S, err error) {
	defer rows.Close()

	if err := scanner.Initialize(rows); err != nil {
		return nil, err
	}

	startingLen := len(slice)
	var startingPtr *T
	if cap(slice) > 0 {
		startingPtr = &slice[:cap(slice)][0]
	}

	defer func() {
		// Named return values guarantee this err is the err that's actually returned.
		if err != nil && len(slice) > startingLen && &slice[0] == startingPtr {
			// An error occurred AND slice still has the same backing array as the input slice.
			// Therefore, some written values are visible in the input slice. This could cause
			// problems, especially if T contains pointers which are kept alive.
			// To mitigate this, zero out the slice beyond the starting length.
			for i := range slice[startingLen:] {
				var zero T
				slice[startingLen+i] = zero
			}
		}
	}()

	for rows.Next() {
		i := len(slice)
		var zero T
		slice = append(slice, zero)
		err := scanner.ScanRowInto(&slice[i], rows)
		if err != nil {
			return nil, err
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return slice, nil
}

// CollectRows iterates through rows, scanning each row according to into,
// and collecting the results into a slice of T.
func CollectRows[T any](rows pgx.Rows, into RowSpec[T]) ([]T, error) {
	return CollectRowsUsing(rows, into().fn())
}

// CollectRowsUsing iterates through rows, scanning each row with the scanner,
// and collecting the results into a slice of T.
func CollectRowsUsing[T any](rows pgx.Rows, scanner Scanner[T]) ([]T, error) {
	return AppendRowsUsing([]T{}, rows, scanner)
}

// CollectOneRow scans the first row in rows and returns the result.
// If no rows are found returns an error where errors.Is(pgx.ErrNoRows) is true.
// CollectOneRow is to CollectRows as QueryRow is to Query.
func CollectOneRow[T any](rows pgx.Rows, into RowSpec[T]) (T, error) {
	return CollectOneRowUsing(rows, into().fn())
}

// CollectOneRowUsing scans the first row in rows and returns the result.
// If no rows are found returns an error where errors.Is(pgx.ErrNoRows) is true.
// CollectOneRowUsing is to CollectRowsUsing as QueryRow is to Query.
func CollectOneRowUsing[T any](rows pgx.Rows, scanner Scanner[T]) (T, error) {
	defer rows.Close()

	var (
		err   error
		value T
		zero  T
	)

	err = scanner.Initialize(rows)
	if err != nil {
		return zero, err
	}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return zero, err
		}
		return zero, pgx.ErrNoRows
	}

	err = scanner.ScanRowInto(&value, rows)
	if err != nil {
		return zero, err
	}

	rows.Close()

	err = rows.Err()
	if err != nil {
		return zero, err
	}

	return value, nil
}

// CollectExactlyOneRow scans the first row in rows and returns the result.
//   - If no rows are found returns an error where errors.Is(pgx.ErrNoRows) is true.
//   - If more than 1 row is found returns an error where errors.Is(ErrTooManyRows) is true.
func CollectExactlyOneRow[T any](rows pgx.Rows, into RowSpec[T]) (T, error) {
	return CollectExactlyOneRowUsing(rows, into().fn())
}

// CollectExactlyOneRowUsing scans the first row in rows and returns the result.
//   - If no rows are found returns an error where errors.Is(pgx.ErrNoRows) is true.
//   - If more than 1 row is found returns an error where errors.Is(ErrTooManyRows) is true.
func CollectExactlyOneRowUsing[T any](rows pgx.Rows, scanner Scanner[T]) (T, error) {
	defer rows.Close()

	var (
		err   error
		value T
		zero  T
	)

	err = scanner.Initialize(rows)
	if err != nil {
		return zero, err
	}

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return zero, err
		}

		return zero, pgx.ErrNoRows
	}

	err = scanner.ScanRowInto(&value, rows)
	if err != nil {
		return zero, err
	}

	if rows.Next() {
		return zero, pgx.ErrTooManyRows
	}

	rows.Close()

	err = rows.Err()
	if err != nil {
		return zero, err
	}

	return value, nil
}

type simpleScanner[T any] struct {
	scanTargets []any
}

var _ Scanner[struct{}] = (*simpleScanner[struct{}])(nil)

// newSimpleScanner returns a Scanner that scans a row into a T.
func newSimpleScanner[T any]() Scanner[T] {
	return &simpleScanner[T]{}
}

// newAddrOfSimpleScanner returns a Scanner that scans a row into a *T.
func newAddrOfSimpleScanner[T any]() Scanner[*T] {
	return newAddrScanner(newSimpleScanner[T]())
}

// RowTo scans a row into a T.
func RowTo[T any]() rowSpecRes[T] {
	return rowSpecRes[T]{fn: newSimpleScanner[T]}
}

// RowToAddrOf scans a row into a *T.
func RowToAddrOf[T any]() rowSpecRes[*T] {
	return rowSpecRes[*T]{fn: newAddrOfSimpleScanner[T]}
}

func (rs *simpleScanner[T]) Initialize(rows pgx.Rows) error {
	return nil
}

func (rs *simpleScanner[T]) ScanRowInto(receiver *T, rows pgx.Rows) error {
	if rs.scanTargets == nil {
		rs.scanTargets = make([]any, 1)
	}
	rs.scanTargets[0] = receiver
	return rows.Scan(rs.scanTargets...)
}

type positionalStructScanner[T any] struct {
	structScanner[T]
}

var _ Scanner[struct{}] = (*positionalStructScanner[struct{}])(nil)

// newPositionalStructScanner returns a Scanner that scans a T from a row.
// T must be a struct. T must have the same number of public fields as row has fields.
// The row and T fields will be matched by position.
// If the "db" struct tag is "-" then the field will be ignored.
func newPositionalStructScanner[T any]() Scanner[T] {
	return &positionalStructScanner[T]{}
}

// newPositionalStructScanner returns a Scanner that scans a *T from a row.
// T must be a struct. T must have the same number of public fields as row has fields.
// The row and T fields will be matched by position.
// If the "db" struct tag is "-" then the field will be ignored.
func newAddrOfPositionalStructScanner[T any]() Scanner[*T] {
	return newAddrScanner[T](newPositionalStructScanner[T]())
}

// RowToStructByPos scans a row into a T.
// T must be a struct. T must have the same number of public fields as row has fields.
// The row and T fields will be matched by position.
// If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByPos[T any]() rowSpecRes[T] {
	return rowSpecRes[T]{fn: newPositionalStructScanner[T]}
}

// RowToStructByPos scans a row into a *T.
// T must be a struct. T must have the same number of public fields as row has fields.
// The row and T fields will be matched by position.
// If the "db" struct tag is "-" then the field will be ignored.
func RowToAddrOfStructByPos[T any]() rowSpecRes[*T] {
	return rowSpecRes[*T]{fn: newAddrOfPositionalStructScanner[T]}
}

func (rs *positionalStructScanner[T]) Initialize(rows pgx.Rows) error {
	typ := typeFor[T]()
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("generic type '%s' is not a struct", typ.Name())
	}
	fldDescs := rows.FieldDescriptions()
	var err error
	rs.fields, err = getPositionalStructRowFields(typ, fldDescs)
	return err
}

var positionalStructRowFieldsMap sync.Map

func getPositionalStructRowFields(
	typ reflect.Type,
	fldDescs []pgconn.FieldDescription,
) ([]structRowField, error) {
	fieldsIface, ok := positionalStructRowFieldsMap.Load(typ)
	if !ok {
		namedFields := lookupNamedStructRowFields(typ)
		fields := make([]structRowField, len(namedFields))
		for i := range namedFields {
			fields[i] = namedFields[i].field
		}
		fieldsIface, _ = positionalStructRowFieldsMap.LoadOrStore(typ, fields)
	}
	fields := fieldsIface.([]structRowField)
	if len(fields) != len(fldDescs) {
		return nil, fmt.Errorf(
			"got %d values, but dst struct has only %d fields",
			len(fldDescs),
			len(fields),
		)
	}
	return fields, nil
}

type namedStructScanner[T any] struct {
	structScanner[T]
}

type strictNamedStructScanner[T any] struct {
	namedStructScanner[T]
}

type laxNamedStructScanner[T any] struct {
	namedStructScanner[T]
}

var (
	_ Scanner[struct{}] = (*strictNamedStructScanner[struct{}])(nil)
	_ Scanner[struct{}] = (*laxNamedStructScanner[struct{}])(nil)
)

// newNamedStructScanner returns Scanner that scans a row into a T.
// T must be a struct. T must have the same number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func newNamedStructScanner[T any]() Scanner[T] {
	return &strictNamedStructScanner[T]{}
}

// newLaxNamedStructScanner returns Scanner that scans a row into a T.
// T must be a struct. T must have greater than or equal number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func newLaxNamedStructScanner[T any]() Scanner[T] {
	return &laxNamedStructScanner[T]{}
}

// newAddrOfNamedStructScanner returns Scanner that scans a row into a *T.
// T must be a struct. T must have the same number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func newAddrOfNamedStructScanner[T any]() Scanner[*T] {
	return newAddrScanner[T](newNamedStructScanner[T]())
}

// newAddrOfLaxNamedStructScanner returns Scanner that scans a row into a *T.
// T must be a struct. T must have greater than or equal number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func newAddrOfLaxNamedStructScanner[T any]() Scanner[*T] {
	return newAddrScanner[T](newLaxNamedStructScanner[T]())
}

// RowToStructByName scans a row into a T.
// T must be a struct. T must have the same number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByName[T any]() rowSpecRes[T] {
	return rowSpecRes[T]{fn: newNamedStructScanner[T]}
}

// RowToAddrOfStructByName scans a row into a *T.
// T must be a struct. T must have the same number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func RowToAddrOfStructByName[T any]() rowSpecRes[*T] {
	return rowSpecRes[*T]{fn: newAddrOfNamedStructScanner[T]}
}

// RowToStructByNameLax scans a row into a T.
// T must be a struct. T must have greater than or equal number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func RowToStructByNameLax[T any]() rowSpecRes[T] {
	return rowSpecRes[T]{fn: newLaxNamedStructScanner[T]}
}

// RowToAddrOfStructByNameLax scans a row into a *T.
// T must be a struct. T must have greater than or equal number of named public fields as row has fields.
// The row and T fields will be matched by name. The match is case-insensitive.
// The database column name can be overridden with a "db" struct tag.
// If the "db" struct tag is "-" then the field will be ignored.
func RowToAddrOfStructByNameLax[T any]() rowSpecRes[*T] {
	return rowSpecRes[*T]{fn: newAddrOfLaxNamedStructScanner[T]}
}

func (rs *strictNamedStructScanner[T]) Initialize(rows pgx.Rows) error {
	return rs.initialize(rows, false)
}

func (rs *laxNamedStructScanner[T]) Initialize(rows pgx.Rows) error {
	return rs.initialize(rows, true)
}

func (rs *namedStructScanner[T]) initialize(rows pgx.Rows, lax bool) error {
	typ := typeFor[T]()
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("generic type '%s' is not a struct", typ.Name())
	}
	fldDescs := rows.FieldDescriptions()
	var missingField string
	var err error
	rs.fields, missingField, err = getNamedStructRowFields(typ, fldDescs)
	if !lax && missingField != "" {
		return fmt.Errorf("cannot find field %s in returned row", missingField)
	} else if err != nil {
		return err
	}

	return nil
}

var namedStructRowFieldsMap sync.Map

type namedStructRowFieldsKey struct {
	typ      reflect.Type
	colNames string
}

type namedStructRowFieldsEntry struct {
	fields       []structRowField
	missingField string
}

func getNamedStructRowFields(
	typ reflect.Type,
	fldDescs []pgconn.FieldDescription,
) ([]structRowField, string, error) {
	key := namedStructRowFieldsKey{
		typ:      typ,
		colNames: joinColNames(fldDescs),
	}
	entryIface, ok := namedStructRowFieldsMap.Load(key)
	if !ok {
		namedFields := lookupNamedStructRowFields(typ)
		fields := make([]structRowField, len(fldDescs))
		var missingField string
		for i := range namedFields {
			f := &namedFields[i]
			fpos := fieldPosByName(fldDescs, f.name)
			if fpos == -1 {
				if missingField == "" {
					missingField = f.name
				}
				continue
			}
			fields[fpos] = f.field
		}
		for i, f := range fields {
			if !f.isSet() {
				return nil, missingField, fmt.Errorf(
					"struct doesn't have corresponding row field %s",
					fldDescs[i].Name,
				)
			}
		}
		entry := &namedStructRowFieldsEntry{
			fields:       fields,
			missingField: missingField,
		}
		entryIface, _ = namedStructRowFieldsMap.LoadOrStore(key, entry)
	}

	entry := entryIface.(*namedStructRowFieldsEntry)
	return entry.fields, entry.missingField, nil
}

func joinColNames(fldDescs []pgconn.FieldDescription) string {
	switch len(fldDescs) {
	case 0:
		return ""
	case 1:
		return fldDescs[0].Name
	}

	totalSize := len(fldDescs) - 1 // Space for separator bytes.
	for _, d := range fldDescs {
		totalSize += len(d.Name)
	}
	var b strings.Builder
	b.Grow(totalSize)
	b.WriteString(fldDescs[0].Name)
	for _, d := range fldDescs[1:] {
		b.WriteByte(0) // Join with NUL byte as it's (presumably) not a valid column character.
		b.WriteString(d.Name)
	}
	return b.String()
}

func fieldPosByName(fldDescs []pgconn.FieldDescription, field string) (i int) {
	i = -1
	for i, desc := range fldDescs {

		// Snake case support.
		field = strings.ReplaceAll(field, "_", "")
		descName := strings.ReplaceAll(desc.Name, "_", "")

		if strings.EqualFold(descName, field) {
			return i
		}
	}
	return
}

func typeFor[T any]() reflect.Type {
	// Definition copied from reflect.TypeFor.
	// TODO: Use reflect.TypeFor when we support for go versions < 1.22 is dropped.
	return reflect.TypeOf((*T)(nil)).Elem()
}

// structScanner encapsulates the logic to scan a row into fields of a struct.
type structScanner[T any] struct {
	fields      []structRowField
	scanTargets []any
}

func (rs *structScanner[T]) ScanRowInto(receiver *T, rows pgx.Rows) error {
	rs.setupScanTargets(receiver)
	return rows.Scan(rs.scanTargets...)
}

func (rs *structScanner[T]) setupScanTargets(receiver *T) {
	r := receiverFromPointer(receiver)
	if rs.scanTargets == nil {
		rs.scanTargets = make([]any, len(rs.fields))
	}
	for i, f := range rs.fields {
		rs.scanTargets[i] = r.getField(f)
	}
}

// addrScannerInfo wraps a Scanner[T] into a Scanner[*T].
type addrScanner[T any] struct {
	wrapped Scanner[T]
}

// newAddrScanner returns a Scanner that wraps a Scanner to scan into a pointer.
func newAddrScanner[T any](wrapped Scanner[T]) Scanner[*T] {
	return &addrScanner[T]{wrapped: wrapped}
}

var _ Scanner[*struct{}] = (*addrScanner[struct{}])(nil)

func (rs *addrScanner[T]) Initialize(rows pgx.Rows) error {
	return rs.wrapped.Initialize(rows)
}

func (rs *addrScanner[T]) ScanRowInto(receiver **T, rows pgx.Rows) error {
	*receiver = new(T)
	return rs.wrapped.ScanRowInto(*receiver, rows)
}

type mapScanner struct{}

var _ Scanner[map[string]any] = (*mapScanner)(nil)

// newMapScanner returns a Scanner that scans a row into a map.
func newMapScanner() Scanner[map[string]any] {
	return &mapScanner{}
}

// RowToMap scans a row into a map.
func RowToMap() rowSpecRes[map[string]any] {
	return rowSpecRes[map[string]any]{fn: newMapScanner}
}

func (*mapScanner) Initialize(rows pgx.Rows) error {
	return nil
}

func (*mapScanner) ScanRowInto(receiver *map[string]any, rows pgx.Rows) error {
	values, err := rows.Values()
	if err != nil {
		return err
	}

	*receiver = make(map[string]any, len(values))

	for i := range values {
		(*receiver)[rows.FieldDescriptions()[i].Name] = values[i]
	}

	return nil
}

type adapterScanner[T any] pgx.RowToFunc[T]

var _ Scanner[struct{}] = (adapterScanner[struct{}])(nil)

// Adapt adapts a RowToFunc (the input to pgx.CollectRows, etc.) into a RowSpec.
//
// This simplifies migration from pgx.CollectRows to pgx_collect.CollectRows by
// allowing code-bases with custom RowToFunc implementations to replace
//
//	pgx.CollectRows(rows, myRowToFunc)
//
// with:
//
//	pgxc.CollectRows(rows, pgxc.Adapt(myRowToFunc))
//
// This is only recommendation for custom implementations of RowToFunc during
// a migration process. Implementations of RowToFunc in pgx have native
// pgx_collect implementations that are more concise and efficient. Custom
// implementations can likely be refactored to work with RowToCustom which
// should be somewhat more efficient for queries over multiple rows.
func Adapt[T any](rowTo pgx.RowToFunc[T]) RowSpec[T] {
	return func() rowSpecRes[T] {
		return rowSpecRes[T]{
			fn: func() Scanner[T] { return adapterScanner[T](rowTo) },
		}
	}
}

func (adapterScanner[T]) Initialize(pgx.Rows) error {
	return nil
}

func (rs adapterScanner[T]) ScanRowInto(receiver *T, rows pgx.Rows) error {
	var err error
	*receiver, err = rs(rows)
	return err
}
