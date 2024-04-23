package kqlfilter

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHasMustEqual(t *testing.T) {
	testCases := []struct {
		name           string
		input          string
		expectedValues []string
	}{
		{
			name:           "single value",
			input:          "type_id:team",
			expectedValues: []string{"team"},
		},
		{
			name:           "two values at a top-level and with another field",
			input:          "type_id:team and disabled:true",
			expectedValues: []string{"team"},
		},
		{
			name:           "three values at a top-level and with another field",
			input:          "type_id:team and disabled:true and active:true",
			expectedValues: []string{"team"},
		},
		{
			name:           "two values on the same field with an and (broken conceptually)",
			input:          "type_id:team and type_id:player",
			expectedValues: []string{"team", "player"},
		},
		{
			name:           "two values",
			input:          "type_id:(team or player)",
			expectedValues: []string{"team", "player"},
		},
		{
			name:           "two values at a top-level or on the same field",
			input:          "type_id:team or type_id:player",
			expectedValues: []string{"team", "player"},
		},
		{
			name:           "two values at a top-level or with another field",
			input:          "type_id:team or disabled:true",
			expectedValues: nil,
		},
		{
			name:           "three values at a top-level or on the same field and another one",
			input:          "type_id:team or type_id:player or disabled:true",
			expectedValues: nil,
		},
		{
			name:           "nested query",
			input:          "type_id:team and (active:false or disabled:true)",
			expectedValues: []string{"team"},
		},
		{
			name:           "range query",
			input:          "type_id>=team and type_id<=player",
			expectedValues: nil,
		},
		{
			name:           "not query",
			input:          "not type_id:team",
			expectedValues: nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			n, err := ParseAST(test.input)
			require.NoError(t, err)

			values := HasMustEqual(n, "type_id")
			assert.Equal(t, test.expectedValues, values)
		})
	}
}
