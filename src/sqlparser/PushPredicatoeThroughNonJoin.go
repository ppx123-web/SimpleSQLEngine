package main

import "github.com/pingcap/tidb/parser/ast"

func (plan *LogicalPlan) PushPredicateThroughNonJoin() {

}

func PredicatePush2Project(root *LogicalPlan) bool {

}

// CheckFieldsDeterministic checks the *ast.FieldList whether is deterministic
func CheckFieldsDeterministic(list *ast.FieldList) bool {
	//TODO NOT JUDGE ALL Condition
	for _, field := range list.Fields {
		switch expr := field.Expr.(type) {
		case *ast.ColumnNameExpr:
		case *ast.AggregateFuncExpr:
			_ = expr
			return false
		}
	}
	return true
}

func PredicatePush2Aggregate(root *LogicalPlan) bool {

}

func PredicatePush2Join() bool {

}

func LimitPush2Project() bool {

}

func LimitPush2Join() bool {

}
