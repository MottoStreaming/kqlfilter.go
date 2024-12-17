package elastic

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/MottoStreaming/kqlfilter.go"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

type QueryGenerator struct {
	mapFieldName  func(name string) (string, error)
	mapFieldValue func(name, value string) (string, error)
}

func NewQueryGenerator(options ...Option) *QueryGenerator {
	g := &QueryGenerator{mapFieldName: defaultFieldNameMapper, mapFieldValue: defaultFieldValueMapper}

	for _, option := range options {
		option(g)
	}

	return g
}

// Option is a function that configures a query generator.
type Option func(*QueryGenerator)

// WithFieldMapper allows validating incoming field names, and mapping them to internally defined ones.
// This can be used to prevent users from requiring knowledge about implementation details.
// Example usage:
//
//	WithFieldMapper(func(name string) (string, error) {
//		if !allowedFields[name] {
//			return "", fmt.Errorf("field %s is not allowed", name)
//		}
//		if name == "release_time" {
//			return "time", nil
//		}
//		return name, nil
//	})
func WithFieldMapper(fieldMapper func(name string) (string, error)) Option {
	return func(g *QueryGenerator) {
		g.mapFieldName = fieldMapper
	}
}

// WithFieldValueMapper allows mapping incoming values for a field, or returning an error on invalid values.
// Example usage:
//
//	WithFieldValueMapper(func(name, value string) (string, error) {
//		if name == "user_id" and !isValidUserID(value) {
//			return "", fmt.Errorf("user id %s is not allowed", value)
//		}
//		return fmt.Sprintf("uid_%s", value), nil
//	})
func WithFieldValueMapper(fieldValueMapper func(name, value string) (string, error)) Option {
	return func(g *QueryGenerator) {
		g.mapFieldValue = fieldValueMapper
	}
}

// ConvertAST converts a KQL AST to an Elasticsearch query.
func (q *QueryGenerator) ConvertAST(root kqlfilter.Node) (types.Query, error) {
	return q.convertNodeToQuery(root, "")
}

func (q *QueryGenerator) convertNodeToQuery(node kqlfilter.Node, prefix string) (types.Query, error) {
	switch n := node.(type) {
	case *kqlfilter.AndNode:
		var clauses []types.Query
		for _, child := range n.Nodes {
			q, err := q.convertNodeToQuery(child, prefix)
			if err != nil {
				return types.Query{}, err
			}
			clauses = append(clauses, q)
		}
		return types.Query{
			Bool: &types.BoolQuery{
				Must: clauses,
			},
		}, nil
	case *kqlfilter.OrNode:
		var clauses []types.Query
		for _, child := range n.Nodes {
			q, err := q.convertNodeToQuery(child, prefix)
			if err != nil {
				return types.Query{}, err
			}
			clauses = append(clauses, q)
		}
		return types.Query{
			Bool: &types.BoolQuery{
				Should: clauses,
			},
		}, nil
	case *kqlfilter.NotNode:
		q, err := q.convertNodeToQuery(n.Expr, prefix)
		if err != nil {
			return types.Query{}, err
		}
		return types.Query{
			Bool: &types.BoolQuery{
				MustNot: []types.Query{q},
			},
		}, nil
	case *kqlfilter.IsNode:
		id, err := q.mapFieldName(prefix + n.Identifier)
		if err != nil {
			return types.Query{}, fmt.Errorf("%s: %w", id, err)
		}

		nested, ok := n.Value.(*kqlfilter.NestedNode)
		if ok {
			// Transform x:{y:z} syntax.
			// Prefix all identifiers with the identifier of the parent node,
			// so it becomes x.y:z
			return q.convertNodeToQuery(nested.Expr, id+".")
		}

		or, ok := n.Value.(*kqlfilter.OrNode)
		if ok {
			// Transform x:(y or z) syntax.
			var vals []types.FieldValue
			// Check that all children are literals
			for _, child := range or.Nodes {
				if _, ok := child.(*kqlfilter.LiteralNode); !ok {
					return types.Query{}, fmt.Errorf("%s: invalid syntax", id)
				}
				lit := child.(*kqlfilter.LiteralNode)
				lit.Value, err = q.mapFieldValue(id, lit.Value)
				if err != nil {
					return types.Query{}, fmt.Errorf("%s: %w", id, err)
				}
				vals = append(vals, lit.Value)
			}

			return types.Query{
				Terms: &types.TermsQuery{
					TermsQuery: map[string]types.TermsQueryField{
						id: vals,
					},
				},
			}, nil

		}

		lit, ok := n.Value.(*kqlfilter.LiteralNode)
		if !ok {
			return types.Query{}, fmt.Errorf("%s: expected literal node", id)
		}

		lit.Value, err = q.mapFieldValue(id, lit.Value)
		if err != nil {
			return types.Query{}, fmt.Errorf("%s: %w", id, err)
		}

		return types.Query{
			Term: map[string]types.TermQuery{
				id: {
					Value: lit.Value,
				},
			},
		}, nil
	case *kqlfilter.RangeNode:
		id, err := q.mapFieldName(prefix + n.Identifier)
		if err != nil {
			return types.Query{}, err
		}

		lit, ok := n.Value.(*kqlfilter.LiteralNode)
		if !ok {
			return types.Query{}, fmt.Errorf("%s: expected literal node", id)
		}

		lit.Value, err = q.mapFieldValue(id, lit.Value)
		if err != nil {
			return types.Query{}, fmt.Errorf("%s: %w", id, err)
		}

		rq, err := convertRangeNode(n.Operator, lit)
		if err != nil {
			return types.Query{}, fmt.Errorf("%s: %w", id, err)
		}
		return types.Query{
			Range: map[string]types.RangeQuery{
				id: rq,
			},
		}, nil
	case *kqlfilter.LiteralNode:
		if !slices.Contains([]string{"true", "false"}, n.Value) {
			return types.Query{}, fmt.Errorf("only boolean literals are supported; %s", n.Value)
		}
		if n.Value == "true" {
			return types.Query{
				MatchAll: &types.MatchAllQuery{},
			}, nil
		} else {
			return types.Query{
				MatchNone: &types.MatchNoneQuery{},
			}, nil
		}
	default:
		return types.Query{}, fmt.Errorf("unexpected node type: %T", n)
	}
}

func convertRangeNode(op kqlfilter.RangeOperator, lit *kqlfilter.LiteralNode) (types.RangeQuery, error) {
	// Here we check the type of the literal node, and then we can create the correct range query.
	fVal, err := strconv.ParseFloat(lit.Value, 64)
	if err == nil {
		// it is an int
		esFVal := types.Float64(fVal)
		rq := &types.NumberRangeQuery{}
		switch op {
		case kqlfilter.RangeOperatorLt:
			rq.Lt = &esFVal
		case kqlfilter.RangeOperatorLte:
			rq.Lte = &esFVal
		case kqlfilter.RangeOperatorGt:
			rq.Gt = &esFVal
		case kqlfilter.RangeOperatorGte:
			rq.Gte = &esFVal
		}
		return rq, nil
	}

	// It is not a number, so we check if it is a date.
	_, err = time.Parse(time.RFC3339, lit.Value)
	if err != nil {
		return nil, errors.New("expected number or date literal")
	}

	rq := &types.DateRangeQuery{}
	switch op {
	case kqlfilter.RangeOperatorLt:
		rq.Lt = &lit.Value
	case kqlfilter.RangeOperatorLte:
		rq.Lte = &lit.Value
	case kqlfilter.RangeOperatorGt:
		rq.Gt = &lit.Value
	case kqlfilter.RangeOperatorGte:
		rq.Gte = &lit.Value
	}

	return rq, nil
}

func defaultFieldNameMapper(name string) (string, error) {
	return name, nil
}

func defaultFieldValueMapper(_, value string) (string, error) {
	return value, nil
}
