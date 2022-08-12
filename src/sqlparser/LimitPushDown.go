package main

import "github.com/pingcap/tidb/parser/ast"

func (plan *LogicalPlan) LimitPushDown() (bool, *LogicalPlan) {
	var ret = false
	for {
		var modify = false
		modify = modify || LimitPushDownToProject(plan)
		modify = modify || LimitPushDownToJoin(plan)
		if !modify {
			break
		} else {
			ret = true
		}
	}
	return ret, plan.LogicalPlanFindRoot()
}

func LimitPushDownToProject(root *LogicalPlan) bool {
	if root == nil {
		return false
	}
	cur := root
	var modify = false
	for {
		tmp := &cur.child[0]
		if cur.Tp == Filter {
			if dst, ok := FindLogicalPlanInSingleChain(cur, Project); ok {
				LimitPush2ProjectForInstance(cur, dst)
				modify = true
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

func LimitPush2ProjectForInstance(limit, proj *LogicalPlan) {
	if limit.child[0].Tp == OrderBy {
		order := &limit.child[0]
		order.LogicalPlanDelete()
		proj.LogicalPlanInsert(order)
	}
	limit.LogicalPlanDelete()
	proj.LogicalPlanInsert(limit)
}

func LimitPushDownToJoin(root *LogicalPlan) bool {
	if root == nil {
		return false
	}
	cur := root
	var modify = false
	for {
		tmp := &cur.child[0]
		if cur.Tp == Filter {
			if dst, ok := FindLogicalPlanInSingleChain(cur, Join); ok {
				if flag, place := CanLimitPush2Join(cur, dst); flag {
					LimitPush2JoinForInstance(cur, dst, place)
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

func CanLimitPush2Join(limit, join *LogicalPlan) (bool, int) {
	if limit.child[0].Tp != OrderBy {
		return false, 0
	}
	order := limit.child[0]
	j := join.Content.(JoinNode)
	attributes := order.Content.(OrderByNode).Items
	switch j.Tp {
	case ast.CrossJoin:
		return false, 0
	case ast.LeftJoin:
		for _, item := range attributes {
			for _, v := range item.Item.Fields {
				if !TableInSubLogicalPlan(&join.child[0], v.OrigTblName) {
					return false, 0
				}
			}
		}
		return true, 1
	case ast.RightJoin:
		for _, item := range attributes {
			for _, v := range item.Item.Fields {
				if !TableInSubLogicalPlan(&join.child[1], v.OrigTblName) {
					return false, 0
				}
			}
		}
		return true, 2
	default:
		LogFuncName()
		panic("Error")
	}
}

func LimitPush2JoinForInstance(limit, join *LogicalPlan, choice int) {
	prev := &join.child[choice-1]
	if limit.child[0].Tp == OrderBy {
		newNode := OpNodeInit(OrderBy, limit.child[0].Content)
		newNode.child = append(newNode.child, *prev)
		prev.parent = newNode
		prev = prev.parent
	}
	newNode := OpNodeInit(Limit, limit.Content)
	newNode.child = append(newNode.child, *prev)
	prev.parent = newNode
	join.child[choice-1] = *newNode
	newNode.parent = join
}
