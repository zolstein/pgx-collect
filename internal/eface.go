//go:build !pgx_collect_safe

package pgx_collect

import (
	"fmt"
	"reflect"
	"runtime"
	"unsafe"
)

func init() {
	const errorStr = `pgx-collect: runtime validation: %s

pgx-collect relies on internals of the Go runtime that are violated by %s.
To resolve this issue:
	- Ensure that you have the latest version of pgx-collect.
	- Please file an issue at github.com/zolstein/pgx-collect.
	- As a temporary workaround, you can build with '-tags pgx_collect_safe'.
`
	if err := validateUnsafeRuntime(); err != nil {
		panic(fmt.Sprintf(errorStr, err, runtime.Version()))
	}
}

func validateUnsafeRuntime() error {
	if unsafe.Sizeof(any(nil)) != unsafe.Sizeof(eface{}) {
		return fmt.Errorf("interface size assertion failed")
	}

	type S struct {
		X int
	}

	h := typeToHandle(typeFor[S]())

	s := S{X: 123}

	v := anyFromParts(h, unsafe.Pointer(&s))

	p, ok := v.(*S)
	if !ok {
		return fmt.Errorf("type assertion failed")
	}

	if p != &s {
		return fmt.Errorf("value assertion failed")
	}

	v = anyFromParts(h, nil)

	p, ok = v.(*S)
	if !ok {
		return fmt.Errorf("nil pointer type assertion failed")
	}

	if p != nil {
		return fmt.Errorf("nil pointer value assertion failed")
	}

	return nil
}

func typeFor[T any]() reflect.Type {
	// Definition copied from reflect.TypeFor.
	// TODO: Use reflect.TypeFor when we support for go versions < 1.22 is dropped.
	return reflect.TypeOf((*T)(nil)).Elem()
}

// typeHandle is an opaque handle to a type, used to construct scan targets.
type typeHandle unsafe.Pointer

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
	e := eface{typ, ptr}
	return *(*any)(unsafe.Pointer(&e))
}

// typeToHandle returns a typeHandle for Type t.
func typeToHandle(t reflect.Type) typeHandle {
	e := reflect.Zero(reflect.PointerTo(t)).Interface()
	return (*(*eface)(unsafe.Pointer(&e))).typ
}
