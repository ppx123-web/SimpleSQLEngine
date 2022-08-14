package main

import (
	"fmt"
	"github.com/pingcap/tidb/parser/test_driver"
)

// PushPredicateThroughNonJoin : return true means push down successfully
func (plan *LogicalPlan) PushPredicateThroughNonJoin() bool {
	var ret = false
	for {
		var modify = false
		modify = modify || PredicatePush2Project(plan)
		modify = modify || PredicatePush2Aggregate(plan)
		if !modify {
			break
		} else {
			ret = true
		}
		plan.CombineFilters()
	}
	return ret
}

func PredicatePush2Project(root *LogicalPlan) bool {
	if root == nil || len(root.child) < 1 {
		return false
	}
	cur := root
	var modify = false
	for {
		tmp := &cur.child[0]
		if cur.Tp == Filter && len(cur.child[0].child) == 1 && cur.child[0].child[0].Tp == Project {
			if CanPredicatePush2Project(cur) {
				PredicatePush2ProjectForInstance(cur)
				modify = true
			}
		}
		cur = tmp
		if len(cur.child) != 1 {
			break
		}
	}
	for _, child := range cur.child {
		modify = modify || PredicatePush2Project(&child)
	}
	return modify
}

func CanPredicatePush2Project(root *LogicalPlan) bool {
	if CheckFieldsDeterministic(root.child[0].child[0].Content.(ProjectionNode).cols) {
		return true
	} else {
		return false
	}
}

// PredicatePush2ProjectForInstance : root.Tp = Filter, root.child[0].Child[0].Tp = Project
func PredicatePush2ProjectForInstance(root *LogicalPlan) {
	if root.Tp != Filter || root.child[0].child[0].Tp != Project {
		LogFuncName()
		panic("Error Root Node When Predicate push down")
	}
	child := &root.child[0].child[0]
	root.LogicalPlanDelete()
	child.LogicalPlanInsert(root)

	fmt.Printf("Predicate Push Down to Project\n")
	OutputQuery(treeRoot, 0)

}

// CheckFieldsDeterministic checks the exprs whether is deterministic
func CheckFieldsDeterministic(exprs []Expression) bool {
	//TODO NOT JUDGE ALL Conditions
	for _, expr := range exprs {
		if !CheckExprDeterministic(expr) {
			return false
		}
	}
	return true
}

func CheckExprDeterministic(expr Expression) bool {
	for _, datum := range expr.expr {
		if datum.Kind() == test_driver.KindString {
			str := datum.GetString()
			if _, ok := expr.Fields[str]; !ok {
				//Datum is a Function
				if !CheckFieldsDeterministic(datum.Args) {
					return false
				}
			}
		}
	}
	return true
}

func PredicatePush2Aggregate(root *LogicalPlan) bool {
	//聚合函数的字段必须是确定的且必须要有GroupBY
	//将Filter以是否确定性分成可以下推candidates的和可以保留的nonDeterministic
	//将candidates和聚合的字段比较，获得可以下推的字段pushDown，剩余字段rest
	//rest和nonDeterministic合并
	//push down
	if root == nil || len(root.child) < 1 {
		return false
	}
	cur := root
	var modify = false
	for {
		tmp := &cur.child[0]
		if cur.Tp == Filter {
			//聚合函数的字段必须是确定的且必须要有GroupBY
			if dst, ok := FindLogicalPlanInSingleChain(cur, Aggregate); ok {
				if CanPush2Aggregator(dst) {
					modify = modify || PredicatePush2AggregatorForInstance(cur, dst)
				}
			}
		}
		cur = tmp
		if len(cur.child) != 1 {
			break
		}
	}
	for _, child := range cur.child {
		modify = modify || PredicatePush2Aggregate(&child)
	}
	return modify
}

func CanPush2Aggregator(aggregate *LogicalPlan) bool {
	return CheckFieldsDeterministic(aggregate.Content.(AggregateNode).ProjectionNode.cols)
}

func PredicatePush2AggregatorForInstance(filter, aggregate *LogicalPlan) bool {
	Exprs := filter.Content.(WhereFilterNode).Expr
	var candidates, nonDeterministic, rest, pushDown []Expression
	//将Filter以是否确定性分成可以下推candidates的和可以保留的nonDeterministic
	for _, expr := range Exprs {
		if CheckExprDeterministic(expr) {
			candidates = append(candidates, expr)
		} else {
			nonDeterministic = append(nonDeterministic, expr)
		}
	}
	//将candidates和聚合的字段比较，获得可以下推的字段pushDown，剩余字段rest
	attributes := GetAggregateFunctions(aggregate.Content.(AggregateNode).cols)
	for _, expr := range candidates {
		var flag = true
		for _, s := range attributes {
			if _, ok := expr.Fields[s]; ok {
				flag = false
				break
			}
		}
		if flag {
			pushDown = append(pushDown, expr)
		} else {
			rest = append(rest, expr)
		}
	}
	//rest和nonDeterministic合并
	remained := append(nonDeterministic, rest...)

	if len(pushDown) > 0 {
		filter.Content = WhereFilterNode{Expr: remained}
		//push down
		newNode := OpNodeInit(Filter, WhereFilterNode{Expr: pushDown})
		aggregate.LogicalPlanInsert(newNode)

		fmt.Printf("Predicate Push Down to Aggregator\n")
		OutputQuery(treeRoot, 0)
		return true
	} else {
		return false
	}
}

func AggregatorInExpression(expr Expression) bool {
	for _, e := range expr.expr {
		if e.Args != nil && e.Kind() == test_driver.KindString {
			if IsAggregateFunction(e.GetString()) {
				return true
			}
		}
	}
	return false
}

func GetAggregateFunctions(aggregate []Expression) []string {
	var ret []string
	for _, expr := range aggregate {
		if AggregatorInExpression(expr) {
			ret = append(ret, expr.AsName)
		}
	}
	return ret
}

func IsAggregateFunction(f string) bool {
	switch f {
	case "count", "sum", "avg", "max", "min",
		"COUNT", "SUM", "AVG", "MAX", "MIN":
		return true
	}
	return false
}
