package pgx_collect

import (
	"reflect"
	"strings"
	"sync"
)

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

type structRowFieldReceiver reflect.Value

func receiverFromPointer[T any](ptr *T) structRowFieldReceiver {
	return structRowFieldReceiver(reflect.ValueOf(ptr).Elem())
}

func (r structRowFieldReceiver) getField(f structRowField) any {
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
