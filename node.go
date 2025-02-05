package kqlfilter

import (
	"strings"
)

// A Node is an element in the parse tree.
type Node interface {
	Type() NodeType
	String() string
	Position() Pos // byte position of start of node in full original input string
	// writeTo writes the String output to the builder.
	writeTo(*strings.Builder)
}

// NodeType identifies the type of parse tree node.
type NodeType int

// Pos represents a byte position in the original input text from which
// this template was parsed.
type Pos int

func (p Pos) Position() Pos {
	return p
}

// Type returns itself and provides an easy default implementation
// for embedding in a Node. Embedded in all non-trivial Nodes.
func (t NodeType) Type() NodeType {
	return t
}

const (
	NodeOr NodeType = iota // Plain text.
	NodeAnd
	NodeNot
	NodeIs
	NodeRange
	NodeNested
	NodeLiteral
	NodeContainment
)

// Nodes.

// OrNode holds multiple sub queries.
type OrNode struct {
	NodeType
	Pos
	p     *parser
	Nodes []Node // The clauses nodes in lexical order.
}

func (p *parser) newOrNode(pos Pos) *OrNode {
	return &OrNode{p: p, NodeType: NodeOr, Pos: pos}
}

func (q *OrNode) append(n Node) {
	q.Nodes = append(q.Nodes, n)
}

func (q *OrNode) String() string {
	var sb strings.Builder
	q.writeTo(&sb)
	return sb.String()
}

func (q *OrNode) writeTo(sb *strings.Builder) {
	sb.WriteString("(")
	for i, n := range q.Nodes {
		if i > 0 {
			sb.WriteString(" OR ")
		}
		n.writeTo(sb)
	}
	sb.WriteString(")")
}

// AndNode holds multiple sub queries.
type AndNode struct {
	NodeType
	Pos
	p     *parser
	Nodes []Node // The clauses nodes in lexical order.
}

func (p *parser) newAndNode(pos Pos) *AndNode {
	return &AndNode{p: p, NodeType: NodeAnd, Pos: pos}
}

func (q *AndNode) append(n Node) {
	q.Nodes = append(q.Nodes, n)
}

func (q *AndNode) tree() *parser {
	return q.p
}

func (q *AndNode) String() string {
	var sb strings.Builder
	q.writeTo(&sb)
	return sb.String()
}

func (q *AndNode) writeTo(sb *strings.Builder) {
	sb.WriteString("(")
	for i, n := range q.Nodes {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		n.writeTo(sb)
	}
	sb.WriteString(")")
}

// NotNode holds a negated sub query.
type NotNode struct {
	NodeType
	Pos
	p    *parser
	Expr Node // Negated node.
}

func (p *parser) newNotNode(pos Pos, expr Node) *NotNode {
	return &NotNode{p: p, NodeType: NodeNot, Pos: pos, Expr: expr}
}

func (q *NotNode) String() string {
	var sb strings.Builder
	q.writeTo(&sb)
	return sb.String()
}

func (q *NotNode) writeTo(sb *strings.Builder) {
	sb.WriteString("NOT ")
	q.Expr.writeTo(sb)
}

// IsNode holds equality check.
type IsNode struct {
	NodeType
	Pos
	p          *parser
	Identifier string
	Value      Node // The clauses nodes in lexical order.
}

func (p *parser) newIsNode(pos Pos, identifier string, value Node) *IsNode {
	return &IsNode{p: p, NodeType: NodeIs, Pos: pos, Identifier: identifier, Value: value}
}

func (q *IsNode) String() string {
	var sb strings.Builder
	q.writeTo(&sb)
	return sb.String()
}

func (q *IsNode) writeTo(sb *strings.Builder) {
	sb.WriteString(q.Identifier)
	sb.WriteString("=")
	q.Value.writeTo(sb)
}

// RangeNode holds range check.
type RangeNode struct {
	NodeType
	Pos
	p          *parser
	Identifier string
	Operator   RangeOperator
	Value      Node // The clauses nodes in lexical order.
}

type RangeOperator int

const (
	RangeOperatorGt RangeOperator = iota
	RangeOperatorGte
	RangeOperatorLt
	RangeOperatorLte
)

func (o RangeOperator) String() string {
	switch o {
	case RangeOperatorGt:
		return ">"
	case RangeOperatorGte:
		return ">="
	case RangeOperatorLt:
		return "<"
	case RangeOperatorLte:
		return "<="
	default:
		return "???"
	}
}

func (p *parser) newRangeNode(pos Pos, id string, op RangeOperator, value Node) *RangeNode {
	return &RangeNode{p: p, NodeType: NodeRange, Pos: pos, Identifier: id, Operator: op, Value: value}
}

func (q *RangeNode) String() string {
	var sb strings.Builder
	q.writeTo(&sb)
	return sb.String()
}

func (q *RangeNode) writeTo(sb *strings.Builder) {
	sb.WriteString(q.Identifier)
	sb.WriteString(q.Operator.String())
	q.Value.writeTo(sb)
}

// ContainmentNode holds containment check.
type ContainmentNode struct {
	NodeType
	Pos
	p          *parser
	Identifier string
	Operator   ContainmentOperator
	Value      Node // The clauses nodes in lexical order.
}

type ContainmentOperator int

const (
	ContainmentOperatorContains ContainmentOperator = iota
	ContainmentOperatorContainedBy
)

func (o ContainmentOperator) String() string {
	switch o {
	case ContainmentOperatorContains:
		return ">@"
	case ContainmentOperatorContainedBy:
		return "<@"
	default:
		return "???"
	}
}

func (p *parser) newContainmentNode(pos Pos, id string, op ContainmentOperator, value Node) *ContainmentNode {
	return &ContainmentNode{p: p, NodeType: NodeContainment, Pos: pos, Identifier: id, Operator: op, Value: value}
}

func (q *ContainmentNode) String() string {
	var sb strings.Builder
	q.writeTo(&sb)
	return sb.String()
}

func (q *ContainmentNode) writeTo(sb *strings.Builder) {
	sb.WriteString(q.Identifier)
	sb.WriteString(q.Operator.String())
	q.Value.writeTo(sb)
}

// NestedNode holds nested sub query.
type NestedNode struct {
	NodeType
	Pos
	p    *parser
	Expr Node // The clauses nodes in lexical order.
}

func (p *parser) newNestedNode(pos Pos, value Node) *NestedNode {
	return &NestedNode{p: p, NodeType: NodeNested, Pos: pos, Expr: value}
}

func (q *NestedNode) String() string {
	var sb strings.Builder
	q.writeTo(&sb)
	return sb.String()
}

func (q *NestedNode) writeTo(sb *strings.Builder) {
	sb.WriteString("{")
	q.Expr.writeTo(sb)
	sb.WriteString("}")
}

// LiteralNode holds literal value.
type LiteralNode struct {
	NodeType
	Pos
	p     *parser
	Value string
}

func (p *parser) newLiteralNode(pos Pos, value string) *LiteralNode {
	return &LiteralNode{p: p, NodeType: NodeLiteral, Pos: pos, Value: value}
}

func (q *LiteralNode) String() string {
	var sb strings.Builder
	q.writeTo(&sb)
	return sb.String()
}

func (q *LiteralNode) writeTo(sb *strings.Builder) {
	sb.WriteString(q.Value)
}
