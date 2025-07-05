package core

import (
	"reflect"
	"testing"
)

func TestValue(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		v := Value([]byte("hello"))
		assertEqual(t, v.IsZero(), false)
		assertEqual(t, v.Bytes(), []byte("hello"))
		assertEqual(t, v.String(), "hello")
		_, err := v.Bool()
		assertEqual(t, err.Error(), `strconv.ParseBool: parsing "hello": invalid syntax`)
		_, err = v.Int()
		assertEqual(t, err.Error(), `strconv.Atoi: parsing "hello": invalid syntax`)
		_, err = v.Float()
		assertEqual(t, err.Error(), `strconv.ParseFloat: parsing "hello": invalid syntax`)
	})
	t.Run("string", func(t *testing.T) {
		v := Value("hello")
		assertEqual(t, v.IsZero(), false)
		assertEqual(t, v.Bytes(), []byte("hello"))
		assertEqual(t, v.String(), "hello")
		_, err := v.Bool()
		assertEqual(t, err.Error(), `strconv.ParseBool: parsing "hello": invalid syntax`)
		_, err = v.Int()
		assertEqual(t, err.Error(), `strconv.Atoi: parsing "hello": invalid syntax`)
		_, err = v.Float()
		assertEqual(t, err.Error(), `strconv.ParseFloat: parsing "hello": invalid syntax`)
	})
	t.Run("bool true", func(t *testing.T) {
		v := Value("1")
		assertEqual(t, v.IsZero(), false)
		assertEqual(t, v.Bytes(), []byte("1"))
		assertEqual(t, v.String(), "1")
		vbool, err := v.Bool()
		assertNoErr(t, err)
		assertEqual(t, vbool, true)
		vint, err := v.Int()
		assertNoErr(t, err)
		assertEqual(t, vint, 1)
		vfloat, err := v.Float()
		assertNoErr(t, err)
		assertEqual(t, vfloat, 1.0)
	})
	t.Run("bool false", func(t *testing.T) {
		v := Value("0")
		assertEqual(t, v.IsZero(), false)
		assertEqual(t, v.Bytes(), []byte("0"))
		assertEqual(t, v.String(), "0")
		vbool, err := v.Bool()
		assertNoErr(t, err)
		assertEqual(t, vbool, false)
		vint, err := v.Int()
		assertNoErr(t, err)
		assertEqual(t, vint, 0)
		vfloat, err := v.Float()
		assertNoErr(t, err)
		assertEqual(t, vfloat, 0.0)
	})
	t.Run("int", func(t *testing.T) {
		v := Value("42")
		assertEqual(t, v.IsZero(), false)
		assertEqual(t, v.Bytes(), []byte("42"))
		assertEqual(t, v.String(), "42")
		_, err := v.Bool()
		assertEqual(t, err.Error(), `strconv.ParseBool: parsing "42": invalid syntax`)
		vint, err := v.Int()
		assertNoErr(t, err)
		assertEqual(t, vint, 42)
		vfloat, err := v.Float()
		assertNoErr(t, err)
		assertEqual(t, vfloat, 42.0)
	})
	t.Run("float", func(t *testing.T) {
		v := Value("42.5")
		assertEqual(t, v.IsZero(), false)
		assertEqual(t, v.Bytes(), []byte("42.5"))
		assertEqual(t, v.String(), "42.5")
		_, err := v.Bool()
		assertEqual(t, err.Error(), `strconv.ParseBool: parsing "42.5": invalid syntax`)
		_, err = v.Int()
		assertEqual(t, err.Error(), `strconv.Atoi: parsing "42.5": invalid syntax`)
		vfloat, err := v.Float()
		assertNoErr(t, err)
		assertEqual(t, vfloat, 42.5)
	})
	t.Run("empty string", func(t *testing.T) {
		v := Value("")
		assertEqual(t, v.IsZero(), true)
		assertEqual(t, v.Bytes(), []byte{})
		assertEqual(t, v.String(), "")
		vbool, err := v.Bool()
		assertNoErr(t, err)
		assertEqual(t, vbool, false)
		vint, err := v.Int()
		assertNoErr(t, err)
		assertEqual(t, vint, 0)
		vfloat, err := v.Float()
		assertNoErr(t, err)
		assertEqual(t, vfloat, 0.0)
	})
	t.Run("nil", func(t *testing.T) {
		v := Value(nil)
		assertEqual(t, v.IsZero(), true)
		assertEqual(t, v.Bytes(), []byte(nil))
		assertEqual(t, v.String(), "")
		vbool, err := v.Bool()
		assertNoErr(t, err)
		assertEqual(t, vbool, false)
		vint, err := v.Int()
		assertNoErr(t, err)
		assertEqual(t, vint, 0)
		vfloat, err := v.Float()
		assertNoErr(t, err)
		assertEqual(t, vfloat, 0.0)
	})
}

func assertEqual(tb testing.TB, got, want any) {
	tb.Helper()
	if !reflect.DeepEqual(got, want) {
		tb.Errorf("want %#v, got %#v", want, got)
	}
}

func assertNoErr(tb testing.TB, got error) {
	tb.Helper()
	if got != nil {
		tb.Errorf("unexpected error %T (%v)", got, got)
	}
}
