package main

import "github.com/pingcap/tidb/parser/test_driver"

type Datum struct {
	test_driver.Datum
}

func DatumSetInt64(i int64) (d Datum) {
	d.SetInt64(i)
	return
}

func DatumSetUint64(i uint64) (d Datum) {
	d.SetUint64(i)
	return
}

func DatumSetFloat64(f float64) (d Datum) {
	d.SetFloat64(f)
	return
}

func DatumSetFloat32(f float32) (d Datum) {
	d.SetFloat32(f)
	return
}

func DatumSetString(s string) (d Datum) {
	d.SetString(s)
	return
}

func DatumSetBytes(b []byte) (d Datum) {
	d.SetBytes(b)
	return
}

func DatumSetBytesAsString(b []byte) (d Datum) {
	d.SetBytesAsString(b)
	return
}

func DatumSetInterface(x interface{}) (d Datum) {
	d.SetInterface(x)
	return
}

func DatumSetNull() (d Datum) {
	d.SetNull()
	return
}

func DatumSetBinaryLiteral(b test_driver.BinaryLiteral) (d Datum) {
	d.SetBinaryLiteral(b)
	return
}

func DatumSetMysqlDecimal(b *test_driver.MyDecimal) (d Datum) {
	d.SetMysqlDecimal(b)
	return
}

func InitSetValue(val interface{}) (d Datum) {
	d.SetValue(val)
	return
}
