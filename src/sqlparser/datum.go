package main

import (
	"github.com/pingcap/tidb/parser/test_driver"
	"strconv"
)

type Datum struct {
	test_driver.Datum
	Args []Expression
}

func (d *Datum) print() string {
	if d.Args == nil {
		switch d.Datum.Kind() {
		case test_driver.KindString:
			return d.Datum.GetString()
		case test_driver.KindInt64:
			return strconv.FormatInt(d.GetInt64(), 10)
		case test_driver.KindUint64:
			return strconv.FormatUint(d.GetUint64(), 10)
		case test_driver.KindFloat32:
			return strconv.FormatFloat(float64(d.GetFloat32()), 'e', -1, 32)
		case test_driver.KindFloat64:
			return strconv.FormatFloat(d.GetFloat64(), 'e', -1, 64)
		default:
			panic("Error Datum")
		}
	} else {
		str := ""
		for _, arg := range d.Args {
			str = str + arg.print()
		}
		return d.GetString() + "(" + str + ")"
	}
}

func InitSetValue(val interface{}) (d Datum) {
	d.SetValue(val)
	return
}

type MyOp int

// List operators.
const (
	LogicAnd MyOp = iota + 1
	LeftShift
	RightShift
	LogicOr
	GE
	LE
	EQ
	NE
	LT
	GT
	Plus
	Minus
	And
	Or
	Mod
	Xor
	Div
	Mul
	Not
	Not2
	BitNeg
	IntDiv
	LogicXor
	NullEQ
	In
	Like
	Case
	Regexp
	IsNull
	IsTruth
	IsFalsity
)

var Ops = [...]struct {
	Name      string
	Literal   string
	isKeyword bool
}{
	LogicAnd: {
		Name:      "and",
		Literal:   "AND",
		isKeyword: true,
	},
	LogicOr: {
		Name:      "or",
		Literal:   "OR",
		isKeyword: true,
	},
	LogicXor: {
		Name:      "xor",
		Literal:   "XOR",
		isKeyword: true,
	},
	LeftShift: {
		Name:      "leftshift",
		Literal:   "<<",
		isKeyword: false,
	},
	RightShift: {
		Name:      "rightshift",
		Literal:   ">>",
		isKeyword: false,
	},
	GE: {
		Name:      "ge",
		Literal:   ">=",
		isKeyword: false,
	},
	LE: {
		Name:      "le",
		Literal:   "<=",
		isKeyword: false,
	},
	EQ: {
		Name:      "eq",
		Literal:   "=",
		isKeyword: false,
	},
	NE: {
		Name:      "ne",
		Literal:   "!=", // perhaps should use `<>` here
		isKeyword: false,
	},
	LT: {
		Name:      "lt",
		Literal:   "<",
		isKeyword: false,
	},
	GT: {
		Name:      "gt",
		Literal:   ">",
		isKeyword: false,
	},
	Plus: {
		Name:      "plus",
		Literal:   "+",
		isKeyword: false,
	},
	Minus: {
		Name:      "minus",
		Literal:   "-",
		isKeyword: false,
	},
	And: {
		Name:      "bitand",
		Literal:   "&",
		isKeyword: false,
	},
	Or: {
		Name:      "bitor",
		Literal:   "|",
		isKeyword: false,
	},
	Mod: {
		Name:      "mod",
		Literal:   "%",
		isKeyword: false,
	},
	Xor: {
		Name:      "bitxor",
		Literal:   "^",
		isKeyword: false,
	},
	Div: {
		Name:      "div",
		Literal:   "/",
		isKeyword: false,
	},
	Mul: {
		Name:      "mul",
		Literal:   "*",
		isKeyword: false,
	},
	Not: {
		Name:      "not",
		Literal:   "not ",
		isKeyword: true,
	},
	Not2: {
		Name:      "!",
		Literal:   "!",
		isKeyword: false,
	},
	BitNeg: {
		Name:      "bitneg",
		Literal:   "~",
		isKeyword: false,
	},
	IntDiv: {
		Name:      "intdiv",
		Literal:   "DIV",
		isKeyword: true,
	},
	NullEQ: {
		Name:      "nulleq",
		Literal:   "<=>",
		isKeyword: false,
	},
	In: {
		Name:      "in",
		Literal:   "IN",
		isKeyword: true,
	},
	Like: {
		Name:      "like",
		Literal:   "LIKE",
		isKeyword: true,
	},
	Case: {
		Name:      "case",
		Literal:   "CASE",
		isKeyword: true,
	},
	Regexp: {
		Name:      "regexp",
		Literal:   "REGEXP",
		isKeyword: true,
	},
	IsNull: {
		Name:      "isnull",
		Literal:   "IS NULL",
		isKeyword: true,
	},
	IsTruth: {
		Name:      "istrue",
		Literal:   "IS TRUE",
		isKeyword: true,
	},
	IsFalsity: {
		Name:      "isfalse",
		Literal:   "IS FALSE",
		isKeyword: true,
	},
}

func (o MyOp) String() string {
	return Ops[o].Name
}

func StrToOp(str string) MyOp {
	for i, v := range Ops {
		if v.Name == str {
			return MyOp(i)
		}
	}
	return -1
}
