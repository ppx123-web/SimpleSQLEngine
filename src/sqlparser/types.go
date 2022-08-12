package main

import (
	"fmt"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/test_driver"
)

type OpType int

const (
	Project      OpType = iota + 1 //Select Fields
	Join                           //Join Table
	Table                          //Scan Table
	GroupBy                        //GroupBy
	HavingFilter                   //HavingFilter
	Filter                         //Where Filter
	OrderBy                        //OrderBy
	Limit                          //Limit
)

type Stack struct {
	size int
	data []*LogicalPlan
}

func (s *Stack) Push(value *LogicalPlan) {
	s.data = append(s.data, value)
	s.size++
	return
}

func (s *Stack) Pop() (ret *LogicalPlan) {
	if s.size == 0 {
		panic("Pop From Empty Stack")
	}
	ret = s.data[s.size-1]
	s.data = s.data[:s.size-1]
	s.size--
	return
}

func (s *Stack) top() (ret *LogicalPlan) {
	ret = s.data[s.size-1]
	return
}

func (s *Stack) Empty() bool {
	if s.size == 0 {
		return true
	} else {
		return false
	}
}

func (s *Stack) Size() int {
	return s.size
}

type LogicalPlan struct {
	Tp      OpType
	Content interface{}
	child   []LogicalPlan
	parent  *LogicalPlan
}

type ColumnName struct {
	OrigTblName string
	OrigColName string
	DBName      string
	TblName     string
	ColName     string
}

type Expression struct {
	expr   []Datum
	Fields map[string]ColumnName
	AsName string
}

type ProjectionNode struct {
	cols []Expression
}

func (n ProjectionNode) print() {
	for _, v := range n.cols {
		if v.AsName != "" {
			fmt.Printf("%v AS %v, ", v.print(), v.AsName)
		} else {
			fmt.Printf("%v, ", v.print())
		}
	}
}

type JoinNode struct {
	Tp ast.JoinType
	On []Expression
}

func (n JoinNode) print() {
	switch n.Tp {
	case ast.CrossJoin:
		fmt.Printf("CrossJoin")
	case ast.LeftJoin:
		fmt.Printf("LeftJoin")
	case ast.RightJoin:
		fmt.Printf("RightJoin")
	case 0:
		fmt.Printf("Single Table")
		return
	}
	fmt.Printf(" ON ( ")
	for _, v := range n.On {
		if v.AsName != "" {
			fmt.Printf("%v AS %v, ", v.print(), v.AsName)
		} else {
			fmt.Printf("%v, ", v.print())
		}
	}
	fmt.Printf(" )")
}

// TableNode : if a sub query, table TblName means the query's AsName, OrigXXName unused
//
//	if a table ,    table TblName means the table's AsName, OrigXXName is resolved in Analyzer
type TableNode struct {
	Table ColumnName
}

func (n TableNode) print() {
	if n.Table.TblName != "" {
		fmt.Printf("%v AS %v", n.Table.OrigTblName, n.Table.TblName)
	} else {
		fmt.Printf("%v", n.Table.OrigTblName)
	}
}

type HavingFilterNode struct {
	Expr []Expression
}

func (n HavingFilterNode) print() {
	for _, v := range n.Expr {
		if v.AsName != "" {
			fmt.Printf("%v AS %v, ", v.print(), v.AsName)
		} else {
			fmt.Printf("%v, ", v.print())
		}
	}
}

type WhereFilterNode struct {
	Expr []Expression
}

func (n WhereFilterNode) print() {
	for _, v := range n.Expr {
		if v.AsName != "" {
			fmt.Printf("%v AS %v, ", v.print(), v.AsName)
		} else {
			fmt.Printf("%v, ", v.print())
		}
	}
}

type GroupByNode struct {
	Items []Expression
}

func (n GroupByNode) print() {
	for _, v := range n.Items {
		if v.AsName != "" {
			fmt.Printf("%v AS %v, ", v.print(), v.AsName)
		} else {
			fmt.Printf("%v, ", v.print())
		}
	}
}

type ByItem struct {
	Item Expression
	Desc bool
}

type OrderByNode struct {
	Items []ByItem
}

func (n OrderByNode) print() {
	for _, v := range n.Items {
		fmt.Printf("%v ", v.Item.print())
		if v.Desc {
			fmt.Printf("Desc")
		}
	}
}

type LimitNode struct {
	Count  Expression
	Offset Expression
}

func (n LimitNode) print() {
	fmt.Printf("Count: %v", n.Count.print())
	if len(n.Offset.expr) > 1 {
		fmt.Printf("Offset: %v", n.Offset.print())
	}
}

func (expr *Expression) print() string {
	var s []Datum
	for _, d := range expr.expr {
		switch d.Kind() {
		case test_driver.KindString:
			if d.Args != nil {
				s = append(s, d)
			} else {
				t := StrToOp(d.GetString())
				if t != -1 {
					op1, op2 := s[len(s)-2], s[len(s)-1]
					str := "(" + op1.print() + Ops[t].Literal + op2.print() + ")"
					s = s[:len(s)-2]
					s = append(s, InitSetValue(str))
				} else {
					s = append(s, d)
				}
			}
		default:
			s = append(s, d)
		}
	}
	return s[0].print()
}

func LogicalPlanNodeEqual(p1 *LogicalPlan, p2 *LogicalPlan) bool {
	if p1 == p2 {
		return true
	}
	return p1.Tp == p2.Tp && p1.Content == p2.Content
}

func OpNodeInit(tp OpType, op interface{}) *LogicalPlan {
	node := new(LogicalPlan)
	node.Tp = tp
	node.Content = op
	node.child = []LogicalPlan{}
	return node
}

func (plan *LogicalPlan) LogicalPlanInsert(newPlan *LogicalPlan) {
	newPlan.child = plan.child
	plan.child = []LogicalPlan{*newPlan}
	newPlan.parent = plan
	for _, child := range newPlan.child {
		child.parent = newPlan
	}
}

func (plan *LogicalPlan) LogicalPlanDelete() {
	par := plan.parent
	if par == nil && len(plan.child) >= 2 {
		LogFuncName()
		panic("Wrong Node Delete")
	}
	if par != nil {
		if len(par.child) == 1 {
			par.child = plan.child
		} else {
			for i, child := range par.child {
				if &child == plan {
					par.child = append(par.child[:i], plan.child...)
					break
				}
			}
		}
		plan.parent = nil
	} else {
		plan.child[0].parent = nil
	}
	plan.child = []LogicalPlan{}
}

func (plan *LogicalPlan) LogicalPlanFindRoot() *LogicalPlan {
	root := plan
	for {
		if root.parent != nil {
			root = root.parent
		} else {
			return root
		}
	}
}

func GetExpressionColName(expr Expression) []ColumnName {
	var str []ColumnName
	for _, v := range expr.Fields {
		str = append(str, v)
	}
	for _, exp := range expr.expr {
		if exp.Args != nil {
			for _, arg := range exp.Args {
				str = append(str, GetExpressionColName(arg)...)
			}
		}
	}
	return RemoveRepeatedElement(str)
}

// RemoveRepeatedElement 通过map键的唯一性去重
func RemoveRepeatedElement(s []ColumnName) []ColumnName {
	result := make([]ColumnName, 0)
	m := make(map[ColumnName]bool) //map的值不重要
	for _, v := range s {
		if _, ok := m[v]; !ok {
			result = append(result, v)
			m[v] = true
		}
	}
	return result
}

func TableInSubLogicalPlan(root *LogicalPlan, table string) bool {
	switch root.Tp {
	case Project:
		for _, v := range root.Content.(ProjectionNode).cols {
			if len(v.AsName) > 0 {
				if v.AsName == table {
					return true
				}
			}
		}
	case Table:
		if table == root.Content.(TableNode).Table.OrigTblName ||
			table == root.Content.(TableNode).Table.TblName {
			return true
		}
	}
	for _, child := range root.child {
		if TableInSubLogicalPlan(&child, table) {
			return true
		}
	}
	return false
}
