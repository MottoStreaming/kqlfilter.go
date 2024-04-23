package kqlfilter

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNodeTransformer(t *testing.T) {
	transformer := NewNodeMapper()
	transformer.TransformIdentifierFunc = func(s string) string {
		if s == "a" {
			return "z"
		}

		if s == "c" {
			return "y"
		}
		return s
	}

	transformer.TransformValueFunc = func(s string) string {
		if s == "3" {
			return "99"
		}
		return s
	}

	n, err := ParseAST("a:1 and b:2 and c:3 and d:4 e:6")
	require.NoError(t, err)

	err = transformer.Map(n)
	require.NoError(t, err)

	require.Equal(t, "((z=1 AND b=2 AND y=99 AND d=4) AND e=6)", n.String())

	n, err = ParseAST("a:1 and b:2 and not c:3 and d:4 e:6")
	require.NoError(t, err)

	err = transformer.Map(n)
	require.NoError(t, err)

	require.Equal(t, "((z=1 AND b=2 AND NOT y=99 AND d=4) AND e=6)", n.String())

	n, err = ParseAST("a>1 and b:2 and not c<3 and d:4 e:6")
	require.NoError(t, err)

	err = transformer.Map(n)
	require.NoError(t, err)

	require.Equal(t, "((z>1 AND b=2 AND NOT y<99 AND d=4) AND e=6)", n.String())
}
