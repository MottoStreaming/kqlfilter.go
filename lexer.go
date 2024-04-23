package kqlfilter

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// item represents a token or text string returned from the scanner.
type item struct {
	typ  itemType // The type of this item.
	pos  Pos      // The starting position, in bytes, of this item in the input string.
	val  string   // The value of this item.
	line int      // The line number at the start of this item.
}

func (i item) String() string {
	switch {
	case i.typ == itemEOF:
		return "EOF"
	case i.typ == itemError:
		return i.val
	case len(i.val) > 10:
		return fmt.Sprintf("%.10q...", i.val)
	}
	return fmt.Sprintf("%q", i.val)
}

// itemType identifies the type of lex items.
type itemType int

const (
	itemError itemType = iota // error occurred; value is text of error
	itemEOF
	itemSpace         // run of spaces
	itemBool          // boolean constant
	itemString        // string (includes quotes)
	itemOr            // 'or'
	itemAnd           // 'and'
	itemNot           // 'not'
	itemLeftParen     // '('
	itemRightParen    // ')'
	itemLeftBrace     // '{'
	itemRightBrace    // '{'
	itemColon         // ':'
	itemWildcard      // '*'
	itemRangeOperator // '<=' or '<' or '>=' or '>'
)

// Make the types pretty printable.
var itemName = map[itemType]string{
	itemError:         "error",
	itemEOF:           "EOF",
	itemSpace:         "space",
	itemBool:          "bool",
	itemString:        "string",
	itemOr:            "or",
	itemAnd:           "and",
	itemNot:           "not",
	itemLeftParen:     "(",
	itemRightParen:    ")",
	itemLeftBrace:     "{",
	itemRightBrace:    "}",
	itemColon:         ":",
	itemRangeOperator: "range",
}

func (i itemType) String() string {
	s := itemName[i]
	if s == "" {
		return fmt.Sprintf("item%d", int(i))
	}
	return s
}

var key = map[string]itemType{
	"or":    itemOr,
	"and":   itemAnd,
	"not":   itemNot,
	"true":  itemBool,
	"false": itemBool,
}

const eof = -1

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*lexer) stateFn

// lexer holds the state of the scanner.
type lexer struct {
	input      string // the string being scanned
	pos        Pos    // current position in the input
	start      Pos    // start position of this item
	atEOF      bool   // we have hit the end of input and returned eof
	parenDepth int    // nesting depth of ( ) exprs
	braceDepth int    // nesting depth of { } exprs
	line       int    // 1+number of newlines seen
	startLine  int    // start line of this item
	item       item   // item to return to parser
}

// next returns the next rune in the input.
func (l *lexer) next() rune {
	if int(l.pos) >= len(l.input) {
		l.atEOF = true
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += Pos(w)
	if r == '\n' {
		l.line++
	}
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// backup steps back one rune.
func (l *lexer) backup() {
	if !l.atEOF && l.pos > 0 {
		r, w := utf8.DecodeLastRuneInString(l.input[:l.pos])
		l.pos -= Pos(w)
		// Correct newline count.
		if r == '\n' {
			l.line--
		}
	}
}

// thisItem returns the item at the current input point with the specified type
// and advances the input.
func (l *lexer) thisItem(t itemType) item {
	i := item{t, l.start, l.input[l.start:l.pos], l.startLine}
	l.start = l.pos
	l.startLine = l.line
	return i
}

// emit passes the trailing text as an item back to the parser.
func (l *lexer) emit(t itemType) stateFn {
	return l.emitItem(l.thisItem(t))
}

// emitItem passes the specified item to the parser.
func (l *lexer) emitItem(i item) stateFn {
	l.item = i
	return nil
}

// ignore skips over the pending input before this point.
// It tracks newlines in the ignored text, so use it only
// for text that is skipped without calling l.next.
func (l *lexer) ignore() {
	l.line += strings.Count(l.input[l.start:l.pos], "\n")
	l.start = l.pos
	l.startLine = l.line
}

// accept consumes the next rune if it's from the valid set.
func (l *lexer) accept(valid string) bool {
	if strings.ContainsRune(valid, l.next()) {
		return true
	}
	l.backup()
	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *lexer) acceptRun(valid string) {
	for strings.ContainsRune(valid, l.next()) {
	}
	l.backup()
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *lexer) errorf(format string, args ...any) stateFn {
	l.item = item{itemError, l.start, fmt.Sprintf(format, args...), l.startLine}
	l.start = 0
	l.pos = 0
	l.input = l.input[:0]
	return nil
}

// nextItem returns the next item from the input.
// Called by the parser, not in the lexing goroutine.
func (l *lexer) nextItem() item {
	l.item = item{itemEOF, l.pos, "EOF", l.startLine}
	state := lexExpression
	for {
		state = state(l)
		if state == nil {
			return l.item
		}
	}
}

// lex creates a new scanner for the input string.
func lex(input string) *lexer {
	l := &lexer{
		input:     input,
		line:      1,
		startLine: 1,
	}
	return l
}

// state functions

// lexExpression scans the elements.
func lexExpression(l *lexer) stateFn {
	// Either number, quoted string, or identifier.
	// Spaces separate arguments; runs of spaces turn into itemSpace.
	// Pipe symbols separate and are emitted.
	switch r := l.next(); {
	case r == eof:
		if l.parenDepth != 0 {
			return l.errorf("unclosed left parenthesis")
		}
		if l.braceDepth != 0 {
			return l.errorf("unclosed left brace")
		}
		return l.emit(itemEOF)
	case isSpace(r):
		return lexSpace
	case r == ':':
		return l.emit(itemColon)
	case r == '"':
		return lexQuote
	case r == '<' || r == '>':
		return lexRangeOperator
	case r == '*':
		return l.emit(itemWildcard)
	case r == '(':
		l.parenDepth++
		return l.emit(itemLeftParen)
	case r == ')':
		l.parenDepth--
		if l.parenDepth < 0 {
			return l.errorf("unexpected right parenthesis")
		}
		return l.emit(itemRightParen)
	case r == '{':
		l.braceDepth++
		return l.emit(itemLeftBrace)
	case r == '}':
		l.braceDepth--
		if l.braceDepth < 0 {
			return l.errorf("unexpected right brace")
		}
		return l.emit(itemRightBrace)
	default:
		return lexString
	}
}

// lexSpace scans a run of space characters.
// We have not consumed the first space, which is known to be present.
func lexSpace(l *lexer) stateFn {
	for {
		r := l.peek()
		if !isSpace(r) {
			break
		}
		l.next()
	}
	return l.emit(itemSpace)
}

// lexQuote scans a quoted string.
func lexQuote(l *lexer) stateFn {
Loop:
	for {
		switch l.next() {
		case '\\':
			if r := l.next(); r != eof && r != '\n' {
				break
			}
			fallthrough
		case eof, '\n':
			return l.errorf("unterminated quoted string")
		case '"':
			break Loop
		}
	}
	// Replace escaped characters.

	item := item{
		typ:  itemString,
		pos:  l.start,
		val:  replaceEscapes(l.input[l.start:l.pos]),
		line: l.startLine,
	}
	l.emitItem(item)
	l.start = l.pos
	l.startLine = l.line
	return nil
}

// lexString scans continuous string until it finds a special symbol
func lexString(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		case !isSpecialSymbol(r) && r != eof && !isSpace(r):
		// absorb.
		case r == '\\':
			switch l.next() {
			case '\\', '(', ')', '{', '}', ':', '<', '>', '"', '*':
				// absorb.
			case 'a':
				// escaped 'and'
				if !l.accept("n") {
					return l.errorf("invalid escape sequence")
				}
				if !l.accept("d") {
					return l.errorf("invalid escape sequence")
				}
				// absorb.
			case 'o':
				// escaped 'or'
				if !l.accept("r") {
					return l.errorf("invalid escape sequence")
				}
				// absorb.
			case 'n':
				// escaped 'not'
				if !l.accept("o") {
					return l.errorf("invalid escape sequence")
				}
				if !l.accept("t") {
					return l.errorf("invalid escape sequence")
				}
				// absorb.
			default:
				return l.errorf("invalid escape sequence")
			}
		default:
			l.backup()
			word := strings.ToLower(l.input[l.start:l.pos])
			if !l.atTerminator() {
				return l.errorf("bad character %#U", r)
			}
			switch {
			case key[word] > 0:
				item := key[word]
				return l.emit(item)
			default:
				// Replace escaped characters.

				item := item{
					typ:  itemString,
					pos:  l.start,
					val:  replaceEscapes(l.input[l.start:l.pos]),
					line: l.startLine,
				}
				l.emitItem(item)
				l.start = l.pos
				l.startLine = l.line
				return nil
			}
		}
	}
}

// replaceEscapes replaces escaped characters in the input string.
func replaceEscapes(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			switch s[i] {
			case '\\', '(', ')', '{', '}', ':', '<', '>', '"', '*':
				b.WriteByte(s[i])
			case 'a':
				b.WriteString("and")
				i += 2
			case 'o':
				b.WriteString("or")
				i += 1
			case 'n':
				b.WriteString("not")
				i += 2
			}
		} else {
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// atTerminator reports whether the input is at valid termination character to
// appear after an identifier.
func (l *lexer) atTerminator() bool {
	r := l.peek()
	if isSpace(r) {
		return true
	}
	switch r {
	case eof, '*', '>', '<', ':', ')', '(', '}', '{':
		return true
	}
	return false
}

// lexRangeOperator scans a range operator.
func lexRangeOperator(l *lexer) stateFn {
	// we already consumed > or <, so check for optional =
	l.accept("=")
	return l.emit(itemRangeOperator)
}

// isSpace reports whether r is a space character.
func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\r' || r == '\n'
}

// isSpecialSymbol reports whether r is a special symbol.
func isSpecialSymbol(r rune) bool {
	switch r {
	case '\\', '(', ')', '{', '}', ':', '<', '>', '"', '*':
		return true
	default:
		return false
	}
}
