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
	filepath          = "test/sql1.mdf"
	PredPushToProject = "test/PredPushToProject.mdf"
)

var tree_root *LogicalPlan

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	return &stmtNodes[0], nil
}

func main() {
	bytes, err := ioutil.ReadFile(PredPushToProject)
	if err != nil {
		log.Fatal("Failed to read file: " + filepath)
	}
	astNode, err := parse(string(bytes))
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	query := GetQuery(astNode)
	OutputQuery(query, 0)
	tree_root = query
	query.QueryOptimizer()
	OutputQuery(query, 0)
}

/*
 Project: a, b, id,
     Filter: (b<1),
         Table:  AS tmp
             Project: A, B,
                 Filter: (a>2),
                     Table: testdata2

*/
