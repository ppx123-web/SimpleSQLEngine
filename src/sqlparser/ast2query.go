package main

import (
	"fmt"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"runtime"
)

var LogFuncNameFlag bool = false

func LogFuncName() {
	if LogFuncNameFlag {
		funcName, _, _, _ := runtime.Caller(1)
		fmt.Println("func name: " + runtime.FuncForPC(funcName).Name())
	}
}

type OpType int

const (
	SELECT       OpType = iota + 1 //Select Fields
	Join                           //Join Table
	Table                          //Scan Table
	GroupBy                        //GroupBy
	HavingFilter                   //Having
	WhereFilter                    // Where
	OrderBy                        //OrderBy
	Limit                          //Limit
)

type Stack struct {
	size int
	data []*OpNode
}

func (s *Stack) Push(value *OpNode) {
	s.data = append(s.data, value)
	s.size++
	return
}

func (s *Stack) Pop() (ret *OpNode) {
	if s.size == 0 {
		panic("Pop From Empty Stack")
	}
	ret = s.data[s.size-1]
	s.data = s.data[:s.size-1]
	s.size--
	return
}

func (s *Stack) top() (ret *OpNode) {
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

type OpNode struct {
	Vis                 int
	Tp                  OpType
	Op                  interface{}
	left, right, parent *OpNode
}

type OpNodeVisitor interface {
	OpNodeEnter(node *OpNode) (skip bool)
	OpNodeLeave(node *OpNode)
}

func (node *OpNode) traverse(s OpNodeVisitor) {
	if node == nil {
		return
	}
	skip := s.OpNodeEnter(node)
	if !skip {
		node.left.traverse(s)
		node.right.traverse(s)
	}
	s.OpNodeLeave(node)
	return
}

type OpJoin struct {
	Tp ast.JoinType
	On *ast.OnCondition
}

type OpTable struct {
	Table *ast.TableSource
}

type OpHavingFilter struct {
	Expr *ast.ExprNode
}

type OpWhereFilter struct {
	BiOpExpr *ast.BinaryOperationExpr
}

type OpOrderBy struct {
	Order *ast.OrderByClause
}

type OpLimit struct {
	Limit *ast.Limit
}

func OpNodeInit(tp OpType, op interface{}) *OpNode {
	node := new(OpNode)
	node.Tp = tp
	node.Op = op
	return node
}

func (s *Stack) Enter(in ast.Node) (ast.Node, bool) {
	switch in := in.(type) {
	case *ast.BinaryOperationExpr, *ast.Limit, *ast.FieldList,
		*ast.HavingClause, *ast.OrderByClause, *ast.GroupByClause,
		*ast.OnCondition:
		return in, true
	default:
		return in, false
	}
}

func (s *Stack) Leave(in ast.Node) (ast.Node, bool) {
	switch in := in.(type) {
	case *ast.Join:
		s.sddJoin(in)
	case *ast.TableSource:
		s.sddTableSource(in)
	case *ast.SelectStmt:
		s.sddSelectStmt(in)
	case *ast.TableRefsClause:
		s.sddFrom(in)
	case *ast.BinaryOperationExpr:
		s.sddWhere(in)
	case *ast.GroupByClause:
		s.sddGroupBy(in)
	case *ast.OrderByClause:
		s.sddOrderBy(in)
	case *ast.HavingClause:
		s.sddHaving(in)
	case *ast.Limit:
		s.sddLimit(in)
	case *ast.FieldList:
		s.sddFieldList(in)
	}
	return in, true
}

func GetQuery(root *ast.StmtNode) *OpNode {
	OpStack := new(Stack)
	if _, ok := (*root).(*ast.SelectStmt); ok {
		(*root).Accept(OpStack)
	}
	TableNode := OpStack.Pop()
	SelectNode := OpStack.Pop()
	SelectNode.left = TableNode
	TableNode.parent = SelectNode
	return SelectNode
}

func (s *Stack) sddSelectStmt(root *ast.SelectStmt) {
	LogFuncName()
	newNode := OpNodeInit(Table, &OpTable{nil})
	newNode.left = s.Pop()
	newNode.left.parent = newNode
	s.Push(newNode)
}

func (s *Stack) sddFrom(root *ast.TableRefsClause) {
	LogFuncName()
}

func (s *Stack) sddFieldList(root *ast.FieldList) {
	LogFuncName()
	newNode := OpNodeInit(SELECT, root.Fields)
	if !s.Empty() {
		newNode.left = s.Pop()
		newNode.left.parent = newNode
	}
	s.Push(newNode)
}

func (s *Stack) sddWhere(root *ast.BinaryOperationExpr) {
	LogFuncName()
	newNode := OpNodeInit(WhereFilter, &OpWhereFilter{root})
	newNode.left = s.Pop()
	newNode.left.parent = newNode
	s.Push(newNode)
}

func (s *Stack) sddGroupBy(root *ast.GroupByClause) {
	LogFuncName()
	newNode := OpNodeInit(GroupBy, root.Items)
	newNode.left = s.Pop()
	newNode.left.parent = newNode
	s.Push(newNode)
}

func (s *Stack) sddHaving(root *ast.HavingClause) {
	LogFuncName()
	newNode := OpNodeInit(HavingFilter, &OpHavingFilter{&root.Expr})
	newNode.left = s.Pop()
	newNode.left.parent = newNode
	s.Push(newNode)
}

func (s *Stack) sddOrderBy(root *ast.OrderByClause) {
	LogFuncName()
	newNode := OpNodeInit(OrderBy, &OpOrderBy{root})
	newNode.left = s.Pop()
	newNode.left.parent = newNode
	s.Push(newNode)
}

func (s *Stack) sddLimit(root *ast.Limit) {
	LogFuncName()
	newNode := OpNodeInit(Limit, &OpLimit{root})
	newNode.left = s.Pop()
	newNode.left.parent = newNode
	s.Push(newNode)
}

func (s *Stack) sddJoin(root *ast.Join) {
	LogFuncName()
	newNode := OpNodeInit(Join, &OpJoin{root.Tp, root.On})
	if root.Right != nil {
		newNode.right = s.Pop()
		newNode.right.parent = newNode
	}
	newNode.left = s.Pop()
	newNode.left.parent = newNode
	s.Push(newNode)
}

func (s *Stack) sddTableSource(root *ast.TableSource) {
	LogFuncName()
	newNode := OpNodeInit(Table, &OpTable{root})
	switch root.Source.(type) {
	case *ast.SelectStmt:
	case *ast.TableName:
		s.Push(newNode)
	default:
		panic("TableSource Error Type")
	}
}

func BinaryExpr(root *ast.ExprNode) []*ast.BinaryOperationExpr {
	if root, ok := (*root).(*ast.BinaryOperationExpr); ok {
		switch root.Op {
		case opcode.LogicAnd, opcode.LogicOr, opcode.LogicXor:
			ret := append(BinaryExpr(&root.L), BinaryExpr(&root.R)...)
			return ret
		default:
			return nil
		}
	} else {
		return nil
	}
}
