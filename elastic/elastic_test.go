package elastic

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MottoStreaming/kqlfilter.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertNodeToQuery(t *testing.T) {
	testCases := []struct {
		name              string
		input             string
		expectedError     error
		expectedQueryJSON string
	}{
		{
			name:              "simple equality",
			input:             "type_id:team",
			expectedError:     nil,
			expectedQueryJSON: `{"term":{"type_id":{"value":"team"}}}`,
		},
		{
			name:              "renamed field",
			input:             `start_time:"2000-01-01T00:00:00.000Z"`,
			expectedError:     nil,
			expectedQueryJSON: `{"term":{"time":{"value":"2000-01-01T00:00:00.000Z"}}}`,
		},
		{
			name:              "do not rename field when it's nested",
			input:             "fields.start_time:foo",
			expectedError:     nil,
			expectedQueryJSON: `{"term":{"fields.start_time":{"value":"foo"}}}`,
		},
		{
			name:              "multiple values for same field",
			input:             "type_id:(team OR player)",
			expectedError:     nil,
			expectedQueryJSON: `{"terms":{"type_id":["team","player"]}}`,
		},
		{
			name:          "multiple fields",
			input:         "type_id:team fields.active:true",
			expectedError: nil,
			expectedQueryJSON: `{
  "bool": {
    "must": [
      {
        "term": {
          "type_id": {
            "value": "team"
          }
        }
      },
	  {
        "term": {
          "fields.active": {
            "value": "true"
          }
        }
      }
    ]
  }
}`,
		},
		{
			name:          "or",
			input:         "(fields.home_team.id:abc OR fields.away_team.id:abc)",
			expectedError: nil,
			expectedQueryJSON: `{
  "bool": {
    "should": [
      {
        "term": {
          "fields.home_team.id": {
            "value": "abc"
          }
        }
      },
      {
        "term": {
          "fields.away_team.id": {
            "value": "abc"
          }
        }
      }
    ]
  }
}`,
		},
		{
			name:          "and/or",
			input:         "type_id:team fields.active:true or fields.established_year < 2000",
			expectedError: nil,
			expectedQueryJSON: `{
  "bool": {
    "must": [
      {
        "term": {
          "type_id": {
            "value": "team"
          }
        }
      },
      {
        "bool": {
          "should": [
            {
              "term": {
                "fields.active": {
                  "value": "true"
                }
              }
            },
            {
              "range": {
                "fields.established_year": {
                  "lt": 2000
                }
              }
            }
          ]
        }
      }
    ]
  }
}`,
		},
		{
			name:          "not",
			input:         "not type_id:team",
			expectedError: nil,
			expectedQueryJSON: `{
  "bool": {
    "must_not": [
      {
        "term": {
          "type_id": {
            "value": "team"
          }
        }
      }
    ]
  }
}`,
		},
		{
			name:          "nested",
			input:         "type_id:player fields:{position:(goalkeeper OR defender)}",
			expectedError: nil,
			expectedQueryJSON: `{
  "bool": {
    "must": [
      {
        "term": {
          "type_id": {
            "value": "player"
          }
        }
      },
      {
        "terms": {
          "fields.position": [
            "goalkeeper",
            "defender"
          ]
        }
      }
    ]
  }
}`,
		},
		{
			name:          "range date",
			input:         `type_id:player fields.birthday >= "2000-01-01T00:00:00.000Z"`,
			expectedError: nil,
			expectedQueryJSON: `{
	  "bool": {
		"must": [	
		  {
			"term": {
			  "type_id": {	
				"value": "player"	
			  }
			}
},
		  {
			"range": {
			  "fields.birthday": {
				"gte": "2000-01-01T00:00:00.000Z"
			  }
			}
		  }
		]
	  }
}`,
		},
		{
			name:          "field validator works",
			input:         `time:"2000-01-01T00:00:04.123Z"`,
			expectedError: errors.New("time: please round time fields to the nearest 5 minutes for improved cachability"),
		},
		{
			name:          "range invalid",
			input:         `type_id:player fields.birthday>=true`,
			expectedError: errors.New("fields.birthday: expected number or date literal"),
		},
		{
			name:          "nesting invalid",
			input:         `type_id:player fields.birthday:(value:"2000-01-01T00:00:00.000Z")`,
			expectedError: errors.New("fields.birthday: expected literal node"),
		},
		{
			name:          "invalid field",
			input:         `type:player`,
			expectedError: errors.New("type: invalid field"),
		},
		{
			name:          "invalid multiple values",
			input:         `type_id:(player OR team OR (club OR organization))`,
			expectedError: errors.New("type_id: invalid syntax"),
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			n, err := kqlfilter.ParseAST(test.input)
			require.NoError(t, err)

			g := NewQueryGenerator(
				WithFieldMapper(
					func(field string) (string, error) {
						if field == "start_time" || field == "time" {
							return "time", nil
						}
						if field == "type_id" || field == "fields" {
							return field, nil
						}
						if strings.HasPrefix(field, "fields.") && strings.Count(field, ".") < 3 {
							return field, nil
						}
						return field, errors.New("invalid field")
					}),
				WithFieldValueMapper(
					func(k, v string) (string, error) {
						if k == "time" {
							t, err := time.Parse(time.RFC3339, v)
							if err != nil {
								return v, fmt.Errorf("invalid TIMESTAMP values: %w", err)
							}
							if t.Second() > 0 || t.Nanosecond() > 0 || t.Minute()%5 > 0 {
								return v, fmt.Errorf("please round time fields to the nearest 5 minutes for improved cachability")
							}
						}
						return v, nil
					}))

			q, err := g.ConvertAST(n)
			if test.expectedError != nil {
				require.EqualError(t, err, test.expectedError.Error())
				return
			}
			require.NoError(t, err)

			data, err := json.Marshal(q)
			require.NoError(t, err)

			assert.JSONEq(t, test.expectedQueryJSON, string(data))
		})
	}
}
