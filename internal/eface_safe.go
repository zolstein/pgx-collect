//go:build pgx_collect_safe

package pgx_collect

import (
	"reflect"
	"unsafe"
)

// typeHandle is an opaque handle to a type, used to construct scan targets.
type typeHandle reflect.Type

type eface struct {
	typ typeHandle
	ptr unsafe.Pointer
}

// anyFromParts creates an any pointing at a typed value according to the provided
// typeHandle, backed by ptr.
//
// Note: If the type handle was constructed from type T, anyFromParts requires ptr
// to point at a T, and the returned value will represent a *T, not a T.
func anyFromParts(typ typeHandle, ptr unsafe.Pointer) any {
	return reflect.NewAt(typ, ptr).Interface()
}

// typeToHandle returns a typeHandle for Type t.
func typeToHandle(t reflect.Type) typeHandle {
	return t
}
