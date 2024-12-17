package kqlfilter

import (
	"fmt"
	"slices"
	"strings"
)

type Filter struct {
	Clauses []Clause
}

type Clause struct {
	Field string
	// One of the following: `=`, `<`, `<=`, `>`, `>=`, `IN`
	Operator string
	// List of values for the clause.
	// For `IN` operator, this is a list of values to match against.
	// For other operators, this is a list of one string.
	Values []string
}

// Parse parses a filter string into a Filter struct.
// The filter string must not contain any boolean operators, parentheses or nested queries.
// The filter string must contain only simple clauses of the form "field:value", where all clauses are AND'ed.
// If you need to parse a more complex filter string, use ParseAST instead.
func Parse(input string) (Filter, error) {
	if strings.TrimSpace(input) == "" {
		return Filter{}, nil
	}
	ast, err := ParseAST(input, WithMaxDepth(2))
	if err != nil {
		return Filter{}, err
	}
	return convertToFilter(ast)
}

// ParseAST parses a filter string into an AST.
// The filter string must be a valid Kibana query language filter string.
func ParseAST(input string, options ...ParserOption) (n Node, err error) {
	p := &parser{
		maxDepth:      20,
		maxComplexity: 20,
	}
	for _, option := range options {
		option(p)
	}
	p.text = input

	defer p.recover(&err)
	p.lex = lex(input)
	p.parse()
	p.lex = nil // release lexer for garbage collection

	return p.Root, err
}

// ParserOption is a function that configures a parser.
type ParserOption func(*parser)

// DisableComplexExpressions disables complex expressions.
func DisableComplexExpressions() ParserOption {
	return func(p *parser) {
		p.disableComplexExpressions = true
	}
}

// WithMaxDepth sets limit to maximum number of nesting.
func WithMaxDepth(depth int) ParserOption {
	return func(p *parser) {
		p.maxDepth = depth
	}
}

// WithMaxComplexity sets limit to maximum number of individual clauses separated by boolean operators.
func WithMaxComplexity(complexity int) ParserOption {
	return func(p *parser) {
		p.maxComplexity = complexity
	}
}

func convertToFilter(ast Node) (Filter, error) {
	if ast == nil {
		return Filter{}, nil
	}
	switch n := ast.(type) {
	case *AndNode:
		return convertAndNode(n)
	case *IsNode:
		return convertIsNode(n)
	case *RangeNode:
		return convertRangeNode(n)
	case *NotNode:
		return convertNotNode(n)
	case *LiteralNode:
		return convertLiteralNode(n)
	default:
		return Filter{}, fmt.Errorf("unsupported node type %T", ast)
	}
}

func convertLiteralNode(ast *LiteralNode) (Filter, error) {
	if !slices.Contains([]string{"true", "false"}, ast.Value) {
		return Filter{}, fmt.Errorf("only boolean literals are supported; %s", ast.Value)
	}

	if ast.Value == "true" {
		return Filter{
			Clauses: []Clause{
				{
					Field:    "1",
					Operator: "=",
					Values:   []string{"1"},
				},
			},
		}, nil
	} else {
		return Filter{
			Clauses: []Clause{
				{
					Field:    "1",
					Operator: "=",
					Values:   []string{"0"},
				},
			},
		}, nil
	}
}

func convertAndNode(ast *AndNode) (Filter, error) {
	var filter Filter
	fieldCounts := make(map[string]int)
	for _, node := range ast.Nodes {
		var f Filter
		var err error
		switch n := node.(type) {
		case *IsNode:
			f, err = convertIsNode(n)
		case *NotNode:
			f, err = convertNotNode(n)
		case *RangeNode:
			f, err = convertRangeNode(n)
		case *LiteralNode:
			f, err = convertLiteralNode(n)
		default:
			return Filter{}, fmt.Errorf("unsupported node type %T", ast)
		}
		if err != nil {
			return Filter{}, err
		}
		filter.Clauses = append(filter.Clauses, f.Clauses...)
	}
	for _, clause := range filter.Clauses {
		fieldCounts[clause.Field]++
		if fieldCounts[clause.Field] > 2 {
			return Filter{}, fmt.Errorf("field count maximum in filter exceeded")
		}
	}
	return filter, nil
}

func convertIsNode(ast *IsNode) (Filter, error) {
	clause := Clause{
		Field:    ast.Identifier,
		Operator: "=",
	}
	switch n := ast.Value.(type) {
	case *LiteralNode:
		clause.Values = []string{n.Value}
	case *OrNode:
		clause.Operator = "IN"
		for _, node := range n.Nodes {
			literalNode, ok := node.(*LiteralNode)
			if !ok {
				return Filter{}, fmt.Errorf("unsupported node type %T", node)
			}
			clause.Values = append(clause.Values, literalNode.Value)
		}
	default:
		return Filter{}, fmt.Errorf("unsupported node type %T", ast.Value)
	}
	return Filter{
		Clauses: []Clause{clause},
	}, nil
}

func convertNotNode(ast *NotNode) (Filter, error) {
	var err error
	var filter Filter
	switch n := ast.Expr.(type) {
	case *IsNode:
		filter, err = convertIsNode(n)
	default:
		return Filter{}, fmt.Errorf("unsupported node type %T", ast.Expr)
	}

	if err != nil {
		return Filter{}, err
	}

	for i := range filter.Clauses {
		if filter.Clauses[i].Operator == "=" {
			filter.Clauses[i].Operator = "!="
		} else {
			return Filter{}, fmt.Errorf("cannot support negation on operator %s", filter.Clauses[i].Operator)
		}
	}

	return filter, nil
}

func convertRangeNode(ast *RangeNode) (Filter, error) {
	var value string
	switch n := ast.Value.(type) {
	case *LiteralNode:
		value = n.Value
	default:
		return Filter{}, fmt.Errorf("unsupported node type %T", ast.Value)
	}
	operator := ast.Operator.String()
	if operator == "???" {
		return Filter{}, fmt.Errorf("unsupported operator %s", operator)
	}
	return Filter{
		Clauses: []Clause{
			{
				Field:    ast.Identifier,
				Operator: operator,
				Values:   []string{value},
			},
		},
	}, nil
}
