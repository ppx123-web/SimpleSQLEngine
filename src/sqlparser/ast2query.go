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
	Project      OpType = iota + 1 //Select Fields
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

type OpField struct {
	Fields []*ast.SelectField
}

type OpJoin struct {
	Tp ast.JoinType
	On *ast.OnCondition
}

type OpTable struct {
	Table *ast.TableSource
}

type OpHavingFilter struct {
	Expr []*ast.BinaryOperationExpr
}

type OpWhereFilter struct {
	BiOpExpr []*ast.BinaryOperationExpr
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

func (cur *OpNode)OpNodeInsert(node *OpNode) {
	left := cur.left
	right := cur.right
	cur.left = node
	node.parent = cur
	node.left = left
	node.right = right
	if left != nil {
		left.parent = node
	}
	if right != nil {
		right.parent = node
	}
}

func (cur *OpNode)OpNodeDelete() {
	left := cur.left
	right := cur.right
	par := cur.parent
	if par == nil && right != nil {
		LogFuncName()
		panic("Wrong Node Delete")
	}
	if par != nil {
		par.left = left
		par.right = right
		left.parent = par
		if right != nil {
			right.parent = par
		}
	} else {
		left.parent = nil
	}
	cur.left, cur.right, cur.parent = nil, nil, nil

}


func (s *Stack) Enter(in ast.Node) (ast.Node, bool) {
	switch in := in.(type) {
	case *ast.BinaryOperationExpr, *ast.Limit, *ast.FieldList,
		*ast.HavingClause, *ast.OrderByClause, *ast.GroupByClause,
		*ast.OnCondition, ast.ExprNode:
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
	case ast.ExprNode:
		s.sddWhere(&in)
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
	return OpStack.Pop()
}

func OutputQuery(root *OpNode, deep int) {
	if root == nil {
		return
	}
	for i := 0; i < deep; i++ {
		fmt.Printf("    ")
	}
	fmt.Printf(" ")
	switch root.Tp {
	case Project:
		fmt.Printf("Project")
	case Join:
		fmt.Printf("Join")
	case Table:
		fmt.Printf("Table")
	case GroupBy:
		fmt.Printf("GroupBy")
	case HavingFilter:
		fmt.Printf("HavingFilter")
	case WhereFilter:
		fmt.Printf("WhereFilter")
	case OrderBy:
		fmt.Printf("OrderBy")
	case Limit:
		fmt.Printf("Limit")
	}
	fmt.Printf("  %+v\n", root)
	OutputQuery(root.left, deep+1)
	OutputQuery(root.right, deep+1)
}

func (s *Stack) sddSelectStmt(root *ast.SelectStmt) {
	LogFuncName()
	TableNode := s.Pop()
	SelectNode := s.Pop()
	SelectNode.left = TableNode
	TableNode.parent = SelectNode
	s.Push(SelectNode)
}

func (s *Stack) sddFrom(root *ast.TableRefsClause) {
	LogFuncName()
}

func (s *Stack) sddFieldList(root *ast.FieldList) {
	LogFuncName()
	newNode := OpNodeInit(Project, &OpField{root.Fields})
	s.Push(newNode)
}

func (s *Stack) sddWhere(root *ast.ExprNode) {
	LogFuncName()
	newNode := OpNodeInit(WhereFilter, &OpWhereFilter{BinaryExpr(root)})
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
	newNode := OpNodeInit(HavingFilter, &OpHavingFilter{BinaryExpr(&root.Expr)})
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
		newNode.left = s.Pop()
		newNode.left.parent = newNode
	case *ast.TableName:
	default:
		panic("TableSource Error Type")
	}
	s.Push(newNode)
}

func BinaryExpr(root *ast.ExprNode) []*ast.BinaryOperationExpr {
	if root, ok := (*root).(*ast.BinaryOperationExpr); ok {
		switch root.Op {
		case opcode.LogicAnd:
			ret := append(BinaryExpr(&root.L), BinaryExpr(&root.R)...)
			return ret
		default:
			ret := []*ast.BinaryOperationExpr{root}
			return ret
		}
	} else {
		return nil
	}
}
