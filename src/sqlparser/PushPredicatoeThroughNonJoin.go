package main

import (
	"github.com/pingcap/tidb/parser/test_driver"
)

// PushPredicateThroughNonJoin : return true means push down successfully
func (plan *LogicalPlan) PushPredicateThroughNonJoin() bool {
	var ret = false
	for {
		var modify = false
		modify = modify || PredicatePush2Project(plan)
		//modify = modify || PredicatePush2Aggregate(plan)
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
			if CheckFieldsDeterministic(cur.child[0].child[0].Content.(ProjectionNode).cols) {
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

// PredicatePush2ProjectForInstance : root.Tp = Filter, root.child[0].Tp = Project
func PredicatePush2ProjectForInstance(root *LogicalPlan) {
	if root.Tp != Filter || root.child[0].child[0].Tp != Project {
		LogFuncName()
		panic("Error Root Node When Predicate push down")
	}
	child := &root.child[0].child[0]
	root.LogicalPlanDelete()
	child.LogicalPlanInsert(root)
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
	if root == nil {
		return false
	}
	cur := root
	var modify = false
	for {
		tmp := &cur.child[0]
		if cur.Tp == Filter {
			//聚合函数的字段必须是确定的且必须要有GroupBY
			if dst, ok := FindLogicalPlanInSingleChain(cur, GroupBy); ok {
				if CanPush2Aggregator(cur, dst) {
					PredicatePush2AggregatorForInstance(cur, dst)
					modify = true
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

func CanPush2Aggregator(filter, aggregate *LogicalPlan) bool {
	proj := aggregate.parent
	if proj.Tp != Project {
		proj = proj.parent
	}
	if proj.Tp != Project {
		LogFuncName()
		panic("Error Push to Aggregator")
	}
	return CheckFieldsDeterministic(proj.Content.(ProjectionNode).cols)
}

func PredicatePush2AggregatorForInstance(filter, aggregate *LogicalPlan) {
	proj := aggregate.parent
	if proj.Tp != Project {
		proj = proj.parent
	}
	if proj.Tp != Project {
		LogFuncName()
		panic("Error Push to Aggregator")
	}
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
	attributes := GetAggregateFunctions(proj.Content.(ProjectionNode).cols)
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
	filter.Content = WhereFilterNode{Expr: remained}
	//push down
	newNode := OpNodeInit(Filter, WhereFilterNode{Expr: pushDown})
	aggregate.LogicalPlanInsert(newNode)
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
