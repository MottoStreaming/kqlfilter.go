package kqlfilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newItem(typ itemType, text string) item {
	return item{
		typ: typ,
		val: text,
	}
}

var (
	tEOF      = newItem(itemEOF, "")
	tSpace    = newItem(itemSpace, " ")
	tLparen   = newItem(itemLeftParen, "(")
	tRparen   = newItem(itemRightParen, ")")
	tLbrace   = newItem(itemLeftBrace, "{")
	tRbrace   = newItem(itemRightBrace, "}")
	tColon    = newItem(itemColon, ":")
	tWildcard = newItem(itemWildcard, "*")
)

func iterate(l *lexer) []item {
	var items []item
	for {
		item := l.nextItem()
		items = append(items, item)
		if item.typ == itemEOF || item.typ == itemError {
			break
		}
	}
	return items
}

func compareItems(t *testing.T, items []item, expected []item, comparePos bool) {
	assert.Len(t, items, len(expected), "number of items should match")
	for i, item := range items {
		assert.Equalf(t, expected[i].typ, item.typ, "item type should match %s!=%s", expected[i].typ, item.typ)
		assert.Equal(t, expected[i].val, item.val, "item value should match")
		if comparePos {
			assert.Equal(t, expected[i].pos, item.pos, "item position should match")
		}
	}
}

func TestLexer(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []item
	}{
		{
			"empty input",
			"",
			[]item{tEOF},
		},
		{
			"white space",
			" \n\t\r",
			[]item{
				newItem(itemSpace, " \n\t\r"),
				tEOF,
			},
		},
		{
			"simple filter",
			"field: value*",
			[]item{
				newItem(itemString, "field"),
				tColon,
				tSpace,
				newItem(itemString, "value"),
				tWildcard,
				tEOF,
			},
		},
		{
			"number range",
			"price>125.25",
			[]item{
				newItem(itemString, "price"),
				newItem(itemRangeOperator, ">"),
				newItem(itemString, "125.25"),
				tEOF,
			},
		},
		{
			"number range 2",
			"temp <=-20",
			[]item{
				newItem(itemString, "temp"),
				tSpace,
				newItem(itemRangeOperator, "<="),
				newItem(itemString, "-20"),
				tEOF,
			},
		},
		{
			"quoted filter",
			"field: \"value two\"*",
			[]item{
				newItem(itemString, "field"),
				tColon,
				tSpace,
				newItem(itemString, `"value two"`),
				tWildcard,
				tEOF,
			},
		},
		{
			"quoted escape filter",
			`field: "value \" two"`,
			[]item{
				newItem(itemString, "field"),
				tColon,
				tSpace,
				newItem(itemString, `"value " two"`),
				tEOF,
			},
		},
		{
			"unicode filter",
			"field: \"Lūgēte, ō Venerēs Cupīdinēsque\"",
			[]item{
				newItem(itemString, "field"),
				tColon,
				tSpace,
				newItem(itemString, `"Lūgēte, ō Venerēs Cupīdinēsque"`),
				tEOF,
			},
		},
		{
			"unicode filter 2",
			"field:Lūgēte",
			[]item{
				newItem(itemString, "field"),
				tColon,
				newItem(itemString, `Lūgēte`),
				tEOF,
			},
		},
		{
			"escapes",
			"field\\(x\\):separated\\:value",
			[]item{
				newItem(itemString, "field(x)"),
				tColon,
				newItem(itemString, "separated:value"),
				tEOF,
			},
		},
		{
			"parenthesis",
			"field: (one  OR two)",
			[]item{
				newItem(itemString, "field"),
				tColon,
				tSpace,
				tLparen,
				newItem(itemString, "one"),
				newItem(itemSpace, "  "),
				newItem(itemOr, "OR"),
				tSpace,
				newItem(itemString, "two"),
				tRparen,
				tEOF,
			},
		},
		{
			"unclosed parenthesis",
			"field: (",
			[]item{
				newItem(itemString, "field"),
				tColon,
				tSpace,
				tLparen,
				newItem(itemError, "unclosed left parenthesis"),
			},
		},
		{
			"unbalanced parenthesis",
			"field: (one OR two))",
			[]item{
				newItem(itemString, "field"),
				tColon,
				tSpace,
				tLparen,
				newItem(itemString, "one"),
				tSpace,
				newItem(itemOr, "OR"),
				tSpace,
				newItem(itemString, "two"),
				tRparen,
				newItem(itemError, "unexpected right parenthesis"),
			},
		},
		{
			"unbalanced braces",
			"field: {one:x}}",
			[]item{
				newItem(itemString, "field"),
				tColon,
				tSpace,
				tLbrace,
				newItem(itemString, "one"),
				tColon,
				newItem(itemString, "x"),
				tRbrace,
				newItem(itemError, "unexpected right brace"),
			},
		},
		{
			"braces",
			"field: {nested:{ field:\"value\"}}",
			[]item{
				newItem(itemString, "field"),
				tColon,
				tSpace,
				tLbrace,
				newItem(itemString, "nested"),
				tColon,
				tLbrace,
				tSpace,
				newItem(itemString, "field"),
				tColon,
				newItem(itemString, `"value"`),
				tRbrace,
				tRbrace,
				tEOF,
			},
		},
		{
			"dotted",
			"some.nested.field:value",
			[]item{
				newItem(itemString, "some.nested.field"),
				tColon,
				newItem(itemString, `value`),
				tEOF,
			},
		},
		{
			"bad number",
			"field: -23d",
			[]item{
				newItem(itemString, "field"),
				tColon,
				tSpace,
				newItem(itemString, `-23d`),
				tEOF,
			},
		},
		{
			"or values",
			"field:(x OR y AND z)",
			[]item{
				newItem(itemString, "field"),
				tColon,
				tLparen,
				newItem(itemString, "x"),
				tSpace,
				newItem(itemOr, "OR"),
				tSpace,
				newItem(itemString, "y"),
				tSpace,
				newItem(itemAnd, "AND"),
				tSpace,
				newItem(itemString, "z"),
				tRparen,
				tEOF,
			},
		},
		{
			"syntax that includes percentage and wildcard",
			"discount_string:70%*",
			[]item{
				newItem(itemString, "discount_string"),
				tColon,
				newItem(itemString, "70%"),
				tWildcard,
				tEOF,
			},
		},
		{
			"bool",
			"suspended: true",
			[]item{
				newItem(itemString, "suspended"),
				tColon,
				tSpace,
				newItem(itemBool, "true"),
				tEOF,
			},
		},
		{
			"multivalue bool",
			"suspended: (true OR false)",
			[]item{
				newItem(itemString, "suspended"),
				tColon,
				tSpace,
				tLparen,
				newItem(itemBool, "true"),
				tSpace,
				newItem(itemOr, "OR"),
				tSpace,
				newItem(itemBool, "false"),
				tRparen,
				tEOF,
			},
		},
		{
			"false bool without value",
			"false",
			[]item{
				newItem(itemBool, "false"),
				tEOF,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			l := lex(test.input)
			items := iterate(l)
			compareItems(t, items, test.expected, false)
		})
	}
}
