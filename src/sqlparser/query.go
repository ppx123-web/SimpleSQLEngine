package main

import (
	"fmt"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"io/ioutil"
	"log"
)

const (
	dir                 = "test/"
	f1                  = "sql1.mdf"
	f2                  = "sql2.mdf"
	PredPushToProject   = "PredPushToProject.mdf"
	PredPushToAggregate = "PredPushToAggregate.mdf"
	LimitPushToProject  = "LimitPushToProject.mdf"
	LimitPushToJoin     = "LimitPushToJoin.mdf"
)

var treeRoot *LogicalPlan

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	return &stmtNodes[0], nil
}

func main() {
	bytes, err := ioutil.ReadFile(dir + LimitPushToJoin)
	if err != nil {
		log.Fatal("Failed to read file")
	}
	astNode, err := parse(string(bytes))
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	query := GetQuery(astNode)
	OutputQuery(query, 0)
	treeRoot = query
	query.QueryOptimizer()
	OutputQuery(treeRoot, 0)
}
