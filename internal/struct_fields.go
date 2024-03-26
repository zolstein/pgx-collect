package pgx_collect

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"github.com/jackc/pgx/v5/pgconn"
)

const structTagKey = "db"

type StructRowFields []structRowField

func (fs StructRowFields) NumFields() int {
	return len(fs)
}

func (fs StructRowFields) Populate(r StructRowFieldReceiver, scanTargets []any) {
	for i, f := range fs {
		scanTargets[i] = r.getField(f)
	}
}

// structRowField describes a field of a struct.
type structRowField struct {
	// TODO: It would be a bit more efficient to track the path using the pointer
	// offset within the (outermost) struct and use unsafe.Pointer arithmetic to
	// construct references when scanning rows. However, it's not clear it's worth
	// using unsafe for this.
	path []int
}

func (f structRowField) isSet() bool {
	return f.path != nil
}

type StructRowFieldReceiver reflect.Value

func ReceiverFromPointer[T any](ptr *T) StructRowFieldReceiver {
	return StructRowFieldReceiver(reflect.ValueOf(ptr).Elem())
}

func (r StructRowFieldReceiver) getField(f structRowField) any {
	return reflect.Value(r).FieldByIndex(f.path).Addr().Interface()
}

type namedStructRowField struct {
	field structRowField
	name  string
}

var namedStructRowFieldMap sync.Map

func lookupNamedStructRowFields(t reflect.Type) []namedStructRowField {
	if resultIface, ok := namedStructRowFieldMap.Load(t); ok {
		return resultIface.([]namedStructRowField)
	}
	result := computeStructFieldNames(t)
	resultIface, _ := namedStructRowFieldMap.LoadOrStore(t, result)
	return resultIface.([]namedStructRowField)
}

func computeStructFieldNames(t reflect.Type) []namedStructRowField {
	fields := make([]namedStructRowField, 0, t.NumField())
	fieldStack := make([]int, 0, 1)

	var helper func(t reflect.Type)
	helper = func(t reflect.Type) {
		tail := len(fieldStack)
		fieldStack = append(fieldStack, 0)
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			fieldStack[tail] = i
			// Handle anonymous struct embedding, but do not try to handle embedded pointers.
			if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
				helper(sf.Type)
			} else if sf.PkgPath == "" {
				dbTag, dbTagPresent := sf.Tag.Lookup(structTagKey)
				if dbTagPresent {
					dbTag, _, _ = strings.Cut(dbTag, ",")
				}
				if dbTag == "-" {
					// Field is ignored, skip it.
					continue
				}
				colName := dbTag
				if !dbTagPresent {
					colName = sf.Name
				}
				fields = append(fields, namedStructRowField{
					field: structRowField{
						path: append([]int(nil), fieldStack...),
					},
					name: colName,
				})
			}
		}
		fieldStack = fieldStack[:tail]
	}
	helper(t)
	return fields
}

// Map from reflect.Type -> []structRowField
var structRowFieldsByPosMap sync.Map

func GetStructRowFieldsByPos(
	typ reflect.Type,
	fldDescs []pgconn.FieldDescription,
) (StructRowFields, error) {
	fieldsIface, ok := structRowFieldsByPosMap.Load(typ)
	if !ok {
		namedFields := lookupNamedStructRowFields(typ)
		fields := make([]structRowField, len(namedFields))
		for i := range namedFields {
			fields[i] = namedFields[i].field
		}
		fieldsIface, _ = structRowFieldsByPosMap.LoadOrStore(typ, fields)
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

// Map from structRowFieldsByNameKey -> *[]structRowFieldsByNameEntry
// The types and method of managing this cache are funky for the following reasons:
// Different sets / orders of field keys will produce different []structRowField results,
// so the column names from the fldDesc list needs to be included as part of the cache key.
// However, slices can't be used in the field of a map - we need an immutable representation.
// One solution is to create a string joining the column names. However, this requires
// allocating space for the string in order to look up an existing cache value.
// It's faster to instead hash the strings and include the hash in the key. However, this
// introduces a risk that two key sets will collide. To address this, we store a pointer to
// as slice of entries, and use some somewhat complicated logic to manage updating the list
// on a collision. The overhead on collisions is probably bad, but this should be exeedingly
// rare in practice.
// N.b. Plausibly, we should use an implicit linked list rather than a slice. I.e. store a
// *structRowFieldsByNameEntry, which contains a *structRowFieldsByNameEntry as a field.
// This would avoid needing to copy the slice on each collision and _might_ have slightly
// lower overhead otherwise.
var structRowFieldsByNameMap sync.Map

type structRowFieldsByNameKey struct {
	typ          reflect.Type
	hashColNames uint64
}

type structRowFieldsByNameEntry struct {
	cols   []string
	fields []structRowField
	// missingField is used to report errors when non-lax mappers don't include every field.
	// It must be the _first_ field in column order in the type, to be consistent with pgx.
	missingField string
}

func GetStructRowFieldsByName(
	typ reflect.Type,
	fldDescs []pgconn.FieldDescription,
) (StructRowFields, string, error) {
	key := structRowFieldsByNameKey{
		typ:          typ,
		hashColNames: hashColNames(fldDescs),
	}
	var entries []structRowFieldsByNameEntry
	entriesIface, ok := structRowFieldsByNameMap.Load(key)
	if !ok {
		// Ensure the map contains an entry for the key, so we can compare-and-swap later.
		var entriesBox []structRowFieldsByNameEntry
		entriesIface, ok = structRowFieldsByNameMap.LoadOrStore(key, &entriesBox)
	}
	if ok {
		// Make sure one of the entries actually matches this field-set.
		entries = *(entriesIface.(*[]structRowFieldsByNameEntry))
		for _, e := range entries {
			if colsMatch(fldDescs, e.cols) {
				return e.fields, e.missingField, nil
			}
		}
	}
	newEntry := buildNamedStructRowFieldsEntry(typ, fldDescs)
	for i, f := range newEntry.fields {
		if !f.isSet() {
			// In this case, this mapping is never valid. We could cache the failure, but
			// for now it doesn't seem worthwhile to optimize error paths, so we just error out.
			// This could be expensive if a broken query is issued repeatedly.
			return nil, newEntry.missingField, fmt.Errorf(
				"struct doesn't have corresponding row field %s",
				fldDescs[i].Name,
			)
		}
	}

	// Copy existing entries to a new slice, adding the newEntry. Loop to compare-and-swap in
	// the slice, to make sure we actually cache our result but don't clobber anyone else's
	// result that occurred since we last checked.
	for {
		newEntries := make([]structRowFieldsByNameEntry, len(entries)+1)
		newEntries[0] = newEntry
		copy(newEntries[1:], entries)

		// TODO: CompareAndSwap doesn't exist in 1.19, whih is the main reason support is dropped.
		// We could probably work around this by storing an atomic.Pointer in the map and doing
		// the CAS on the pointer rather than the map key.
		if structRowFieldsByNameMap.CompareAndSwap(key, entriesIface, &newEntries) {
			return newEntry.fields, newEntry.missingField, nil
		}
		entriesIface, _ = structRowFieldsByNameMap.Load(key)
		entries = *(entriesIface.(*[]structRowFieldsByNameEntry))

		// It's possible (likely?) that if the CAS failed, we conflicted with another operation
		// inserting the same field-set. Check if the entries we just read includes our field-set.
		//
		// We could probably optimize this loop to only look at a subset of the entries,
		// since some will be repeated from earlier loops. However, it's very unlikely
		// we get here anyway.
		for _, e := range entries {
			if colsMatch(fldDescs, e.cols) {
				return e.fields, e.missingField, nil
			}
		}
	}
}

func buildNamedStructRowFieldsEntry(
	typ reflect.Type,
	fldDescs []pgconn.FieldDescription,
) structRowFieldsByNameEntry {
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
	cols := make([]string, len(fldDescs))
	for i := range fldDescs {
		cols[i] = fldDescs[i].Name
	}
	entry := structRowFieldsByNameEntry{
		cols:         cols,
		fields:       fields,
		missingField: missingField,
	}
	return entry
}

func colsMatch(fldDescs []pgconn.FieldDescription, colNames []string) bool {
	if len(fldDescs) != len(colNames) {
		return false
	}
	for i, s := range colNames {
		if fldDescs[i].Name != s {
			return false
		}
	}
	return true
}

func hashColNames(fldDescs []pgconn.FieldDescription) uint64 {
	hasher := fnv.New64a()
	var zeroByte [1]byte
	_ = zeroByte
	for _, f := range fldDescs {
		// bs := unsafe.Slice(unsafe.StringData(f.Name), len(f.Name))
		bs := unsafeStringToBytes(f.Name)
		hasher.Write(bs)
		// Writing zero bytes between field names reduces the likelihood of collisions.
		// E.g. "aa","a" hash the same as "a","aa" without zeroes, but different with them.
		hasher.Write(zeroByte[:])
	}
	return hasher.Sum64()
}

// TODO: If we reduce the version requirement back down to 1.19, we need to conditionally
// compile a version of this that works.
func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
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

// ClearCaches clears all caches of struct field information. This is intended only for testing.
// This is not synchronized with access of the caches, and so must not be called
// simultaneously with any active use of pgx-collect.
func ClearStructFieldCaches() {
	namedStructRowFieldMap = sync.Map{}
	structRowFieldsByPosMap = sync.Map{}
	structRowFieldsByNameMap = sync.Map{}
}

// CollidingFieldSets returns all field-sets for a type that collided in the hash-table with
// the given keys. (Including the provided one, assuming it's in the table.)
func CollidingFieldSets(typ reflect.Type, fields []string) [][]string {
	fldDescs := make([]pgconn.FieldDescription, len(fields))
	for i, f := range fields {
		fldDescs[i].Name = f
	}
	key := structRowFieldsByNameKey{
		typ:          typ,
		hashColNames: hashColNames(fldDescs),
	}
	entriesIface, ok := structRowFieldsByNameMap.Load(key)
	if !ok {
		return nil
	}
	entries := *(entriesIface.(*[]structRowFieldsByNameEntry))
	fieldSets := make([][]string, len(entries))
	for i, e := range entries {
		fieldSets[i] = e.cols
	}
	return fieldSets
}
