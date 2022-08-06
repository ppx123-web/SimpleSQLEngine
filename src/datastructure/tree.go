package datastructure

// 邻接表
type Node struct {
	right  *Node
	left   *Node
	parent *Node
	value  interface{}
}

type Tree struct {
	nodes []*Node
}

func (n *Node) init(v interface{}) *Node {
	n.value = v
	return n
}

func (dag *Tree) AddVertex(v *Node) {
	dag.nodes = append(dag.nodes, v)
}

func (dag *Tree) AddEdge(parent, child *Node) {
	if parent.left == nil {
		parent.left = child
		child.parent = parent
		return
	} else if parent.right == nil {
		parent.right = child
		child.parent = parent
	} else {
		panic("Too many children\n")
	}
}
