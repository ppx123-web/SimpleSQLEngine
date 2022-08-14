package main

import (
	_ "github.com/pingcap/tidb/parser/test_driver"
)

func (plan *LogicalPlan) QueryOptimizer() {
	plan.PushDownPredicate()
}

func (plan *LogicalPlan) PushDownPredicate() {
	plan.CombineFilters()
	plan.PushPredicateThroughNonJoin()
	plan.PushPredicateThroughJoin()
	plan.LimitPushDown()
}

func (plan *LogicalPlan) CombineFilters() {
	cur := plan
	for {
		if len(cur.child) != 1 {
			break
		}
		if cur.Tp == Filter && cur.child[0].Tp == Filter {
			cur.Content = WhereFilterNode{
				Expr: append(cur.Content.(WhereFilterNode).Expr, cur.child[0].Content.(WhereFilterNode).Expr...),
			}
			cur.child[0].LogicalPlanDelete()
		}
		cur = &cur.child[0]
	}
	for _, child := range cur.child {
		child.CombineFilters()
	}
}

// FindLogicalPlanInSingleChain return the first found OpType OpNode in the single chain of LogicalPlan
func FindLogicalPlanInSingleChain(root *LogicalPlan, op OpType) (*LogicalPlan, bool) {
	if root == nil {
		return nil, false
	}
	if root.Tp == op {
		return root, true
	}
	cur := root
	for {
		if len(cur.child) == 1 {
			cur = &cur.child[0]
			if cur.Tp == op {
				return cur, true
			}
		} else {
			break
		}
	}
	return nil, false
}

// SingleParNode check plan and plan's parent both have single child
func (plan *LogicalPlan) SingleParNode() bool {
	if len(plan.child) > 1 || (plan.parent != nil && len(plan.parent.child) > 1) {
		return false
	}
	return true
}

// SingleChildNode check plan and plan's child both have single child
func (plan *LogicalPlan) SingleChildNode() bool {
	if len(plan.child) > 1 || (len(plan.child) > 0 && len(plan.child[0].child) > 1) {
		return false
	}
	return true
}

// IsNextType return whether the plan's child is OpType
func (plan *LogicalPlan) IsNextType(Tp OpType) bool {
	if len(plan.child) != 1 {
		return false
	} else {
		if plan.child[0].Tp == Tp {
			if len(plan.child[0].child) != 1 {
				panic("Wrong Filter Node")
			}
			return true
		} else {
			return false
		}
	}
}

func LimitPush2Project() bool {
	return true
}

func LimitPush2Join() bool {
	return true
}
