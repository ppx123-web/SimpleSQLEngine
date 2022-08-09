package main

//import (
//	"github.com/pingcap/tidb/parser/ast"
//	_ "github.com/pingcap/tidb/parser/test_driver"
//)
//
//
//
//// FindOpNode return the first found OpType OpNode, if node has left and right, first search left
//func FindOpNode(root *LogicalPlan, op OpType) (*LogicalPlan, bool) {
//	if root == nil {
//		return nil, false
//	}
//	if root.Tp == op {
//		return root, true
//	}
//	if left, ok := FindOpNode(root.left, op); ok {
//		return left, true
//	}
//	if right, ok := FindOpNode(root.right, op); ok {
//		return right, true
//	}
//	return nil, false
//}
//
//func (plan *LogicalPlan) SingleParNode() bool {
//	if plan.right != nil || (plan.parent != nil && plan.parent.right != nil) {
//		return false
//	}
//	return true
//}
//
//func (plan *LogicalPlan) SingleChildNode() bool {
//	if plan.right != nil || (plan.left != nil && plan.left.right != nil) {
//		return false
//	}
//	return true
//}
//
//func (plan *LogicalPlan) Push2(dst *LogicalPlan) {
//	if !plan.SingleParNode() || !dst.SingleChildNode() {
//		LogFuncName()
//		panic("Wrong Push Down Node: Filter plan has right child")
//	}
//	child := plan.left
//	par := plan.parent
//	if par != nil {
//		par.left = child
//	}
//	child.parent = par
//
//	plan.left = dst.left
//	plan.left.parent = plan
//	dst.left = plan
//	plan.parent = dst
//}
//
//// QueryOptimizer 认为analyzed阶段将columnNameExpr解析到了对应的Refer中
//func QueryOptimizer(root *LogicalPlan) {
//	PredicatePush2Project(root)
//}
//
//func PredicatePush2Project(root *LogicalPlan) {
//	curNode := root
//	var where *LogicalPlan
//	where = nil
//	for {
//		if curNode.Tp == WhereFilter {
//			where = curNode
//		}
//		if curNode.Tp == Project {
//			if CanPredicatePush2Project(where, curNode) {
//				where.OpNodeDelete()
//				curNode.OpNodeInsert(where)
//			}
//		}
//		if curNode.right != nil {
//			PredicatePush2Project(root.right)
//			PredicatePush2Project(root.left)
//			break
//		}
//	}
//
//}
//
//func CheckFieldsDeterministic(list *ast.FieldList) bool {
//	//TODO NOT JUDGE ALL Condition
//	for _, field := range list.Fields {
//		switch expr := field.Expr.(type) {
//		case *ast.ColumnNameExpr:
//		case *ast.AggregateFuncExpr:
//			_ = expr
//			return false
//		}
//	}
//	return true
//}
//
//// CanPredicatePush2Project 下推WhereFilter到距离最近的Project
//func CanPredicatePush2Project(where *LogicalPlan, project *LogicalPlan) bool {
//	return CheckFieldsDeterministic(project.Op.(*ast.FieldList))
//}
//
//func PredicatePush2Aggregate(root *LogicalPlan) {
//
//}
//
//func PredicatePush2Join() {
//
//}
//
//func LimitPush2Project() {
//
//}
//
//func LimitPush2Join() {
//
//}
