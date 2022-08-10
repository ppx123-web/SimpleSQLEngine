package main

import (
	"github.com/pingcap/tidb/parser/ast"
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

//type OpNodeVisitor interface {
//	OpNodeEnter(node *LogicalPlan) (skip bool)
//	OpNodeLeave(node *LogicalPlan)
//}
//
//func (node *LogicalPlan) traverse(s OpNodeVisitor) {
//	if node == nil {
//		return
//	}
//	skip := s.OpNodeEnter(node)
//	if !skip {
//		for _, child := range node.child {
//			child.traverse(s)
//		}
//	}
//	s.OpNodeLeave(node)
//	return
//}

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

type JoinNode struct {
	Tp ast.JoinType
	On []Expression
}

// TableNode : if a sub query, table TblName means the query's AsName, OrigXXName unused
//
//	if a table ,    table TblName means the table's AsName, OrigXXName is resolved in Analyzer
type TableNode struct {
	Table ColumnName
}

type HavingFilterNode struct {
	Expr []Expression
}

type WhereFilterNode struct {
	Expr []Expression
}

type ByItem struct {
	Item Expression
	Desc bool
}

type OrderByNode struct {
	Items []ByItem
}

type LimitNode struct {
	Count  Expression
	Offset Expression
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
