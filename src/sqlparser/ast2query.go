package main

import (
	"fmt"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/test_driver"
	"runtime"
)

var LogFuncNameFlag = false

func LogFuncName() {
	if LogFuncNameFlag {
		funcName, _, _, _ := runtime.Caller(1)
		fmt.Println("func name: " + runtime.FuncForPC(funcName).Name())
	}
}

func GetQuery(root *ast.StmtNode) *LogicalPlan {
	OpStack := new(Stack)
	if _, ok := (*root).(*ast.SelectStmt); ok {
		(*root).Accept(OpStack)
	}
	return OpStack.Pop()
}

func OutputQuery(root *LogicalPlan, deep int) {
	if root == nil {
		return
	}
	for i := 0; i < deep; i++ {
		fmt.Printf("    ")
	}
	fmt.Printf(" ")
	switch root.Tp {
	case Project:
		fmt.Printf("Project %v", root.Content.(*ProjectionNode).cols)
	case Join:
		fmt.Printf("Join %v", root.Content.(*JoinNode).On)
	case Table:
		fmt.Printf("Table %v", root.Content.(*TableNode).Table)
	case GroupBy:
		fmt.Printf("GroupBy")
	case HavingFilter:
		fmt.Printf("HavingFilter")
	case WhereFilter:
		fmt.Printf("WhereFilter %v", root.Content.(*WhereFilterNode).BiOpExpr)
	case OrderBy:
		fmt.Printf("OrderBy")
	case Limit:
		fmt.Printf("Limit %v", root.Content.(*LimitNode))
	}
	fmt.Printf("\n")
	//fmt.Printf("  %+v\n", root)
	for _, child := range root.child {
		OutputQuery(&child, deep+1)
	}
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
		s.Join(in)
	case *ast.TableSource:
		s.TableSource(in)
	case *ast.SelectStmt:
		s.SelectStmt()
	case ast.ExprNode:
		s.Where(&in)
	case *ast.GroupByClause:
		s.GroupBy(in)
	case *ast.OrderByClause:
		s.OrderBy(in)
	case *ast.HavingClause:
		s.Having(in)
	case *ast.Limit:
		s.Limit(in)
	case *ast.FieldList:
		s.FieldList(in)
	}
	return in, true
}

func (s *Stack) SelectStmt() {
	LogFuncName()
	TableNode := s.Pop()
	SelectNode := s.Pop()
	SelectNode.child = append(SelectNode.child, *TableNode)
	TableNode.parent = SelectNode
	s.Push(SelectNode)
}

//func (s *Stack) From(root *ast.TableRefsClause) {
//	LogFuncName()
//}

func (s *Stack) FieldList(root *ast.FieldList) {
	LogFuncName()
	newNode := OpNodeInit(Project, &ProjectionNode{AnalyzeColumns(root.Fields)})
	s.Push(newNode)
}

func (s *Stack) Where(root *ast.ExprNode) {
	LogFuncName()
	newNode := OpNodeInit(WhereFilter, &WhereFilterNode{AnalyzeLogicalAndExpr(root)})
	newNode.child = append(newNode.child, *s.Pop())
	newNode.child[len(newNode.child)-1].parent = newNode
	s.Push(newNode)
}

func (s *Stack) GroupBy(root *ast.GroupByClause) {
	LogFuncName()
	newNode := OpNodeInit(GroupBy, root.Items)
	newNode.child = append(newNode.child, *s.Pop())
	newNode.child[len(newNode.child)-1].parent = newNode
	s.Push(newNode)
}

func (s *Stack) Having(root *ast.HavingClause) {
	LogFuncName()
	newNode := OpNodeInit(HavingFilter, &HavingFilterNode{AnalyzeLogicalAndExpr(&root.Expr)})
	newNode.child = append(newNode.child, *s.Pop())
	newNode.child[len(newNode.child)-1].parent = newNode
	s.Push(newNode)
}

func (s *Stack) OrderBy(root *ast.OrderByClause) {
	LogFuncName()
	newNode := OpNodeInit(OrderBy, &OrderByNode{AnalyzeOrderByNode(root)})
	newNode.child = append(newNode.child, *s.Pop())
	newNode.child[len(newNode.child)-1].parent = newNode
	s.Push(newNode)
}

func (s *Stack) Limit(root *ast.Limit) {
	LogFuncName()
	e1, e2 := AnalyzeLimitNode(root)
	newNode := OpNodeInit(Limit, &LimitNode{e1, e2})
	newNode.child = append(newNode.child, *s.Pop())
	newNode.child[len(newNode.child)-1].parent = newNode
	s.Push(newNode)
}

func (s *Stack) Join(root *ast.Join) {
	LogFuncName()
	newNode := OpNodeInit(Join, &JoinNode{root.Tp, AnalyzeJoinNode(root)})
	if root.Right != nil {
		newNode.child = append(newNode.child, *s.Pop())
		newNode.child[len(newNode.child)-1].parent = newNode
	}
	newNode.child = append(newNode.child, *s.Pop())
	newNode.child[len(newNode.child)-1].parent = newNode
	s.Push(newNode)
}

func (s *Stack) TableSource(root *ast.TableSource) {
	LogFuncName()
	switch root.Source.(type) {
	case *ast.SelectStmt:
		newNode := OpNodeInit(Table, &TableNode{ColumnName{TblName: root.AsName.String()}})
		newNode.child = append(newNode.child, *s.Pop())
		newNode.child[len(newNode.child)-1].parent = newNode
		s.Push(newNode)
	case *ast.TableName:
		newNode := OpNodeInit(Table, &TableNode{
			ColumnName{TblName: root.AsName.String(),
				OrigTblName: root.Source.(*ast.TableName).Name.String()}})
		s.Push(newNode)
	default:
		panic("TableSource Error Type")
	}

}

func AnalyzeColumns(root []*ast.SelectField) []Expression {
	var ret []Expression
	for _, field := range root {
		if field.WildCard != nil {
			expr := Expression{
				expr:   []Datum{InitSetValue("*")},
				Fields: make(map[string]ColumnName),
			}
			ret = append(ret, expr)
		} else {
			expr := AnalyzeExprNode(&field.Expr)
			ret = append(ret, expr)
		}
	}
	return ret
}

func AnalyzeOrderByNode(root *ast.OrderByClause) []ByItem {
	var ret []ByItem
	for _, expr := range root.Items {
		var item ByItem
		item.Desc = expr.Desc
		expr.Expr.Accept(&item.Item)
		ret = append(ret, item)
	}
	return ret
}

func AnalyzeJoinNode(root *ast.Join) []Expression {
	if root.On == nil {
		return nil
	} else {
		return AnalyzeLogicalAndExpr(&root.On.Expr)
	}
}

func AnalyzeLimitNode(root *ast.Limit) (Expression, Expression) {
	var Count, Offset Expression
	Count.Fields = make(map[string]ColumnName)
	Offset.Fields = make(map[string]ColumnName)
	root.Count.Accept(&Count)
	root.Offset.Accept(&Offset)
	return Count, Offset
}

func AnalyzeExprNode(root *ast.ExprNode) Expression {
	var ret Expression
	ret.Fields = make(map[string]ColumnName)
	(*root).Accept(&ret)
	return ret
}

func AnalyzeLogicalAndExpr(root *ast.ExprNode) []Expression {
	var ret []Expression
	BiOpExpr := BinaryExpr(root)
	for _, node := range BiOpExpr {
		var tempExpr Expression
		tempExpr.Fields = make(map[string]ColumnName)
		(*node).Accept(&tempExpr)
		ret = append(ret, tempExpr)
	}
	return ret
}

func (expr *Expression) Enter(in ast.Node) (ast.Node, bool) {
	switch root := in.(type) {
	case *ast.BinaryOperationExpr:
		expr.expr = append(expr.expr, InitSetValue("("))
	default:
		_ = root
	}
	return in, false
}

func (expr *Expression) Leave(in ast.Node) (ast.Node, bool) {
	switch root := in.(type) {
	case *test_driver.ValueExpr:
		expr.expr = append(expr.expr, Datum{root.Datum})
	case *ast.BinaryOperationExpr:
		expr.expr = append(expr.expr, InitSetValue(root.Op.String()), InitSetValue(")"))
	case *ast.ColumnNameExpr:
		colName := root.Name.Table.String() + "." + root.Name.Name.String()
		expr.expr = append(expr.expr, InitSetValue(colName))
		expr.Fields[colName] = ColumnName{
			//OrigTblName: root.Refer.Table.Name.String(),
			//OrigColName: root.Refer.Column.Name.String(),
			TblName: root.Name.Table.String(),
			ColName: root.Name.Name.String(),
		}
	}
	return in, true
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
