package kqlfilter

import (
	"fmt"
	"runtime"
	"strings"
)

// parser is the representation of a single parsed filter.
type parser struct {
	Root Node   // top-level root of the tree.
	text string // text parsed to create the filter
	// Parsing only; cleared after parse.
	lex       *lexer
	token     [3]item // three-token lookahead for parser.
	peekCount int
	// Disallow complex expressions:
	// OR, AND, NOT, grouping parentheses or nested queries.
	disableComplexExpressions bool
	maxDepth                  int
	currentDepth              int
	maxComplexity             int
	currentComplexity         int
}

// next returns the next token.
func (p *parser) next() item {
	if p.peekCount > 0 {
		p.peekCount--
	} else {
		p.token[0] = p.lex.nextItem()
	}
	return p.token[p.peekCount]
}

// backup backs the input stream up one token.
func (p *parser) backup() {
	p.peekCount++
}

// peek returns but does not consume the next token.
func (p *parser) peek() item {
	if p.peekCount > 0 {
		return p.token[p.peekCount-1]
	}
	p.peekCount = 1
	p.token[0] = p.lex.nextItem()
	return p.token[0]
}

func (p *parser) eatSpace() {
	for p.peek().typ == itemSpace {
		p.next()
	}
}

// Parsing.

// errorf formats the error and terminates processing.
func (p *parser) errorf(format string, args ...any) {
	p.Root = nil
	format = fmt.Sprintf("parser error: %s at pos %d", format, p.token[0].pos)
	panic(fmt.Errorf(format, args...))
}

// expect consumes the next token and guarantees it has the required type.
func (p *parser) expect(expected itemType, context string) item {
	token := p.next()
	if token.typ != expected {
		p.unexpected(token, context)
	}
	return token
}

// expectOneOf consumes the next token and guarantees it has one of the required types.
func (p *parser) expectOneOf(expected []itemType, context string) item {
	token := p.next()
	for _, expectedType := range expected {
		if token.typ == expectedType {
			return token
		}
	}
	p.unexpected(token, context)
	return token
}

// unexpected complains about the token and terminates processing.
func (p *parser) unexpected(token item, context string) {
	if token.typ == itemError {
		extra := ""
		p.errorf("%s%s", token, extra)
	}
	p.errorf("unexpected %s in %s", token, context)
}

// recover is the handler that turns panics into returns from the top level of Parse.
func (p *parser) recover(errp *error) {
	e := recover()
	if e != nil {
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		*errp = e.(error)
	}
}

// parse is the top-level parser for the KQL query
func (p *parser) parse() {
	p.currentDepth = 0
	p.eatSpace()

	head := p.parseOr()
	// Handle implicit AND
	if p.peek().typ != itemEOF {
		andN := p.newAndNode(0)
		andN.append(head)
		for p.peek().typ != itemEOF {
			p.eatSpace()
			andN.append(p.parseOr())
		}
		p.Root = andN
		return
	}
	p.Root = head
}

func (p *parser) parseOr() Node {
	n := p.newOrNode(p.peek().pos)
	and := p.parseAnd(false)
	n.append(and)
	// optional space before OR
	p.eatSpace()
	for p.peek().typ == itemOr {
		if p.disableComplexExpressions {
			p.errorf("complex expressions are not allowed")
		}
		p.currentComplexity++

		if p.currentComplexity > p.maxComplexity {
			p.errorf("maximum complexity exceeded")
		}

		p.next()
		p.eatSpace()

		and = p.parseAnd(false)
		n.append(and)
	}
	// simplify if only one node
	if len(n.Nodes) == 1 {
		return n.Nodes[0]
	}
	return n
}

func (p *parser) parseAnd(requireLiterals bool) Node {
	n := p.newAndNode(p.peek().pos)

	var nn Node
	if requireLiterals {
		nn = p.parseValue()
	} else {
		nn = p.parseNot()
	}

	n.append(nn)
	p.eatSpace()
	for p.peek().typ == itemAnd {
		p.currentComplexity++

		if p.currentComplexity > p.maxComplexity {
			p.errorf("maximum complexity exceeded")
		}

		p.next()
		p.eatSpace()
		if requireLiterals {
			nn = p.parseValue()
		} else {
			nn = p.parseNot()
		}
		p.eatSpace()
		n.append(nn)
	}
	// simplify if only one node
	if len(n.Nodes) == 1 {
		return n.Nodes[0]
	}
	return n
}

func (p *parser) parseNot() Node {
	if p.peek().typ == itemNot {
		pos := p.peek().pos
		p.next()
		p.eatSpace()

		expr := p.parseSubQuery()
		return p.newNotNode(pos, expr)
	}
	return p.parseSubQuery()
}

func (p *parser) parseSubQuery() Node {
	if p.peek().typ == itemLeftParen {
		p.next()
		p.eatSpace()
		p.currentDepth++

		if p.maxDepth > 0 && p.currentDepth+1 > p.maxDepth {
			p.errorf("maximum nesting depth exceeded")
		}

		n := p.parseOr()
		p.eatSpace()

		p.expect(itemRightParen, "subquery")

		p.currentDepth--
		return n
	}
	return p.parseExpression()
}

func (p *parser) parseExpression() Node {
	switch p.peek().typ {
	case itemString:
		idItem := p.next()
		p.eatSpace()

		op := p.next()

		switch op.typ {
		case itemColon:
			p.eatSpace()
			value := p.parseListOfValues()
			return p.newIsNode(idItem.pos, idItem.val, value)
		case itemRangeOperator:
			p.eatSpace()
			value := p.parseValue()
			var rop RangeOperator
			switch op.val {
			case "<":
				rop = RangeOperatorLt
			case "<=":
				rop = RangeOperatorLte
			case ">":
				rop = RangeOperatorGt
			case ">=":
				rop = RangeOperatorGte
			}
			return p.newRangeNode(idItem.pos, idItem.val, rop, value)
		case itemContainmentOperator:
			p.eatSpace()
			value := p.parseListOfAndValues()
			var cop ContainmentOperator
			switch op.val {
			case ">@":
				cop = ContainmentOperatorContains
			case "<@":
				cop = ContainmentOperatorContainedBy
			}
			return p.newContainmentNode(idItem.pos, idItem.val, cop, value)
		default:
			p.backup()
			// Strip the quotes
			if strings.HasPrefix(idItem.val, `"`) {
				idItem.val = idItem.val[1 : len(idItem.val)-1]
			}
			return p.newLiteralNode(idItem.pos, idItem.val)
		}

	case itemBool:
		value := p.next()
		return p.newLiteralNode(value.pos, value.val)

	default:
		p.unexpected(p.peek(), "expression")
		return nil
	}
}

func (p *parser) parseListOfValues() Node {
	peeked := p.peek()
	if peeked.typ == itemLeftBrace {
		if p.disableComplexExpressions {
			p.errorf("complex expressions are not allowed")
		}
		p.currentDepth++

		if p.maxDepth > 0 && p.currentDepth+1 > p.maxDepth {
			p.errorf("maximum nesting depth exceeded")
		}

		p.next()
		p.eatSpace()

		n := p.parseOr()
		p.eatSpace()

		p.expect(itemRightBrace, "list of values")

		p.currentDepth--
		return p.newNestedNode(peeked.pos, n)
	}
	if peeked.typ == itemLeftParen {
		if p.disableComplexExpressions {
			p.errorf("complex expressions are not allowed")
		}

		p.currentDepth++

		if p.maxDepth > 0 && p.currentDepth+1 > p.maxDepth {
			p.errorf("maximum nesting depth exceeded")
		}

		p.next()
		p.eatSpace()

		n := p.parseOr()
		p.eatSpace()
		p.expect(itemRightParen, "list of values")

		p.currentDepth--
		return n
	}
	return p.parseValue()
}

func (p *parser) parseListOfAndValues() Node {
	peeked := p.peek()
	if peeked.typ == itemLeftParen {
		if p.disableComplexExpressions {
			p.errorf("complex expressions are not allowed")
		}

		p.currentDepth++

		if p.maxDepth > 0 && p.currentDepth+1 > p.maxDepth {
			p.errorf("maximum nesting depth exceeded")
		}

		p.next()
		p.eatSpace()

		n := p.parseAnd(true)
		p.eatSpace()
		p.expect(itemRightParen, "list of values")

		p.currentDepth--
		return n
	}
	return p.parseValue()
}

func (p *parser) parseValue() Node {
	var value string
	pos := p.peek().pos

	valueCount := 0
	for {
		if p.atTerminator() {
			break
		}
		valueCount++
		item := p.expectOneOf([]itemType{
			itemString,
			itemBool,
			itemWildcard,
		}, "value")
		if item.typ == itemString && strings.HasPrefix(item.val, `"`) {
			// Strip the quotes
			item.val = item.val[1 : len(item.val)-1]
		}
		value += item.val
	}

	if valueCount == 0 {
		p.errorf("value expected")
	}

	return p.newLiteralNode(pos, value)
}

func (p *parser) atTerminator() bool {
	item := p.peek()
	switch item.typ {
	case itemEOF, itemSpace, itemLeftBrace, itemLeftParen, itemRightParen, itemRightBrace:
		return true
	default:
		return false
	}
}
