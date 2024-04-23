package kqlfilter

// HasMustEqual will determine if the field that is provided MUST match for equality at the top level of the AST,
// and if so, will return the associated value(s). If not, it will return an empty string.
// This is useful e.g. for determining if a KQL query contains a field that directly corresponds to an Elastic index,
// and as such make it possible to reduce the space of ElasticSearch indexes to query.
func HasMustEqual(ast Node, field string) []string {
	if ast == nil {
		return nil
	}
	switch n := ast.(type) {
	case *AndNode:
		return hasMustEqualAndNode(n, field)
	case *IsNode:
		return hasMustEqualIsNode(n, field)
	case *OrNode:
		return hasMustEqualOrNode(n, field)
	default:
		return nil
	}
}

func hasMustEqualAndNode(ast *AndNode, field string) []string {
	var values []string
	for _, node := range ast.Nodes {
		var values_ []string
		switch n := node.(type) {
		case *IsNode:
			values_ = hasMustEqualIsNode(n, field)
		default:
			continue
		}
		values = append(values, values_...)
	}
	return values
}

func hasMustEqualOrNode(ast *OrNode, field string) []string {
	var values []string
	for _, node := range ast.Nodes {
		var values_ []string
		switch n := node.(type) {
		case *IsNode:
			values_ = hasMustEqualIsNode(n, field)
			if len(values_) == 0 {
				return nil
			}
		default:
			continue
		}
		values = append(values, values_...)
	}
	return values
}

func hasMustEqualIsNode(ast *IsNode, field string) []string {
	if ast.Identifier != field {
		return nil
	}

	var values []string
	switch n := ast.Value.(type) {
	case *LiteralNode:
		values = append(values, n.Value)
	case *OrNode:
		for _, node := range n.Nodes {
			literalNode, ok := node.(*LiteralNode)
			if ok {
				values = append(values, literalNode.Value)
			}
		}
	default:
		break
	}
	return values
}
