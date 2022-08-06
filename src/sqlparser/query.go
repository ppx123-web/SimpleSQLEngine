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
	filepath = "sql2.mdf"
)

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	return &stmtNodes[0], nil
}

func main() {
	bytes, err := ioutil.ReadFile(filepath)
	if err != nil {
		log.Fatal("Failed to read file: " + filepath)
	}
	astNode, err := parse(string(bytes))
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	query := GetQuery(astNode)
	_ = query
	fmt.Printf("%v\n", query)
}
