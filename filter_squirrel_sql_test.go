package kqlfilter

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
)

func TestToSquirrelSql(t *testing.T) {
	// All of those should return an error.
	testCases := []struct {
		name          string
		input         string
		columnMap     map[string]FilterToSquirrelSqlFieldConfig
		expectedError error
		expectedSQL   string
		expectedArgs  []any
	}{
		{
			"one string field",
			"name:Beau", map[string]FilterToSquirrelSqlFieldConfig{
				"name": {
					ColumnName: "name",
					ColumnType: FilterToSpannerFieldColumnTypeString,
				},
			},
			nil,
			"SELECT * FROM users WHERE name = ?",
			[]any{"Beau"},
		},
		{
			"one integer field",
			"age:30", map[string]FilterToSquirrelSqlFieldConfig{
				"age": {
					ColumnName: "age",
					ColumnType: FilterToSpannerFieldColumnTypeInt64,
				},
			},
			nil,
			"SELECT * FROM users WHERE age = ?",
			[]any{int64(30)},
		},
		{
			"one boolean field",
			"local:false",
			map[string]FilterToSquirrelSqlFieldConfig{
				"local": {
					ColumnName: "local",
					ColumnType: FilterToSpannerFieldColumnTypeBool,
				},
			},
			nil,
			"SELECT * FROM users WHERE local = ?",
			[]any{false},
		},
		{
			"one float field",
			"weight:70.7",
			map[string]FilterToSquirrelSqlFieldConfig{
				"weight": {
					ColumnName: "weight",
					ColumnType: FilterToSpannerFieldColumnTypeFloat64,
				},
			},
			nil,
			"SELECT * FROM users WHERE weight = ?",
			[]any{70.7},
		},
		{
			"one timestamp field",
			"birthdate>\"1993-11-26T07:00:00Z\"",
			map[string]FilterToSquirrelSqlFieldConfig{
				"birthdate": {
					ColumnName:  "birthdate",
					ColumnType:  FilterToSpannerFieldColumnTypeTimestamp,
					AllowRanges: true,
				},
			},
			nil,
			"SELECT * FROM users WHERE birthdate > ?",
			[]any{time.Date(1993, 11, 26, 7, 0, 0, 0, time.UTC)},
		},
		{
			"disallowed range operator",
			"userId>12345 and birthdate>\"1993-11-26T07:00:00Z\"",
			map[string]FilterToSquirrelSqlFieldConfig{
				"userId": {
					ColumnName:  "userId",
					ColumnType:  FilterToSpannerFieldColumnTypeInt64,
					AllowRanges: false,
				},
				"birthdate": {
					ColumnName:  "birthdate",
					ColumnType:  FilterToSpannerFieldColumnTypeTimestamp,
					AllowRanges: true,
				},
			},
			errors.New("failed to parse clause 0 to squirrel sql statement: failed to build statement by operator: operator > not supported: unsupported operator"),
			"",
			[]any{},
		},
		{
			"all type of values together",
			"name:Beau age:30 weight:70.7 local:false favorite_day: (Monday OR Tuesday)",
			map[string]FilterToSquirrelSqlFieldConfig{
				"name": {
					ColumnName: "name",
					ColumnType: FilterToSpannerFieldColumnTypeString,
				},
				"age": {
					ColumnName: "age",
					ColumnType: FilterToSpannerFieldColumnTypeInt64,
				},
				"local": {
					ColumnName: "local",
					ColumnType: FilterToSpannerFieldColumnTypeBool,
				},
				"weight": {
					ColumnName: "weight",
					ColumnType: FilterToSpannerFieldColumnTypeFloat64,
				},
				"favorite_day": {
					ColumnName:          "favorite_day",
					ColumnType:          FilterToSpannerFieldColumnTypeString,
					AllowMultipleValues: true,
				},
			},
			nil,
			"SELECT * FROM users WHERE name = ? AND age = ? AND weight = ? AND local = ? AND favorite_day IN (?,?)",
			[]any{"Beau", int64(30), 70.7, false, "Monday", "Tuesday"},
		},
		{
			"one string field with IN operator",
			"favorite_day: (Monday OR Tuesday)",
			map[string]FilterToSquirrelSqlFieldConfig{
				"favorite_day": {
					ColumnName:          "favorite_day",
					ColumnType:          FilterToSpannerFieldColumnTypeString,
					AllowMultipleValues: true,
				},
			},
			nil,
			"SELECT * FROM users WHERE favorite_day IN (?,?)",
			[]any{"Monday", "Tuesday"},
		},
		{
			"one string field with prefix matching",
			`self_intro:"Monday_%a\\_\\%\\**"`,
			map[string]FilterToSquirrelSqlFieldConfig{
				"self_intro": {
					ColumnName:       "self_intro",
					ColumnType:       FilterToSpannerFieldColumnTypeString,
					AllowPrefixMatch: true,
				},
			},
			nil,
			"SELECT * FROM users WHERE self_intro LIKE ?",
			[]any{`Monday\_\%a\\\_\\\%\\*%`},
		},
		{
			"one string field with values map 1",
			"favorite_day:(Monday OR Tuesday)",
			map[string]FilterToSquirrelSqlFieldConfig{
				"favorite_day": {
					ColumnName:          "favorite_day",
					ColumnType:          FilterToSpannerFieldColumnTypeString,
					AllowMultipleValues: true,
					MapValue: func(s string) (any, error) {
						switch s {
						case "Monday":
							return "monday", nil
						case "Tuesday":
							return "tuesday", nil
						default:
							return "", fmt.Errorf("wrong day")
						}
					},
				},
			},
			nil,
			"SELECT * FROM users WHERE favorite_day IN (?,?)",
			[]any{"monday", "tuesday"},
		},
		{
			"one string field with values map 2",
			"before< now",
			map[string]FilterToSquirrelSqlFieldConfig{
				"before": {
					ColumnName:          "create_time",
					ColumnType:          FilterToSpannerFieldColumnTypeTimestamp,
					AllowMultipleValues: true,
					AllowRanges:         true,
					MapValue: func(s string) (any, error) {
						switch s {
						case "now":
							return time.Parse(time.RFC3339Nano, "2023-01-01T00:00:00.000000000Z")
						default:
							return "", fmt.Errorf("wrong value")
						}
					},
				},
			},
			nil,
			"SELECT * FROM users WHERE create_time < ?",
			[]any{time.Date(2023, 01, 01, 00, 00, 00, 00, time.UTC)},
		},
		{
			"unknown field",
			"name:Beau age:30",
			map[string]FilterToSquirrelSqlFieldConfig{
				"age": {
					ColumnName: "age",
					ColumnType: FilterToSpannerFieldColumnTypeInt64,
				},
			},
			unknownFieldErr,
			"",
			nil,
		},
		{
			"wrong value type",
			"age:Beau",
			map[string]FilterToSquirrelSqlFieldConfig{
				"age": {
					ColumnName: "age",
					ColumnType: FilterToSpannerFieldColumnTypeInt64,
				},
			},
			valueConvertErr,
			"",
			nil,
		},
		{
			"wrong values number",
			"age: (1 OR 2)",
			map[string]FilterToSquirrelSqlFieldConfig{
				"age": {
					ColumnName: "age",
					ColumnType: FilterToSpannerFieldColumnTypeInt64,
				},
			},
			valuesNumError,
			"",
			nil,
		},
		{
			"custom parser",
			"age: (1 OR 2)",
			map[string]FilterToSquirrelSqlFieldConfig{
				"age": {
					ColumnName: "age",
					CustomBuilder: func(stmt sq.SelectBuilder, operator string, values []string) (sq.SelectBuilder, error) {
						for i := range values {
							vInt64, err := strconv.ParseInt(values[i], 10, 64)
							if err != nil {
								return stmt, err
							}
							stmt = stmt.Where(sq.Gt{"age": vInt64})
						}
						return stmt, nil
					},
				},
			},
			nil,
			"SELECT * FROM users WHERE age > ? AND age > ?",
			[]any{int64(1), int64(2)},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			f, errParse := Parse(test.input)
			require.NoError(t, errParse)
			stmt, err := f.ToSquirrelSql(sq.Select("*").From("users"), test.columnMap)
			if test.expectedError != nil {
				require.Error(t, err, test.expectedError)
			} else {
				sql, args, err := stmt.ToSql()
				require.NoError(t, err)
				require.Equal(t, test.expectedSQL, sql)
				require.Equal(t, test.expectedArgs, args)
			}
		})
	}
}

func TestAny2Int(t *testing.T) {
	successCases := []any{
		"1",
		int(1),
		int64(1),
		int32(1),
		int16(1),
		int8(1),
		uint(1),
		uint64(1),
		uint32(1),
		uint16(1),
		uint8(1),
		float64(1),
		float32(1),
	}
	for index, c := range successCases {
		i, err := any2Int64(c)
		require.NoError(t, err)
		require.Equalf(t, int64(1), i, "%d: %+v\n", index, reflect.TypeOf(c))
	}
	convertErrorCases := []any{
		"asdf",
		"1.1.1.1",
		"1.1",
	}
	for _, c := range convertErrorCases {
		_, err := any2Int64(c)
		require.ErrorIs(t, err, valueConvertErr)
	}
	unexpectedValueTypeErrorCases := []any{
		os.File{},
		strings.Builder{},
		time.Time{},
	}
	for _, c := range unexpectedValueTypeErrorCases {
		_, err := any2Int64(c)
		require.ErrorIs(t, err, unexpectedValueTypeErr)
	}
}

func TestAny2Float(t *testing.T) {
	successCases := []any{
		"1",
		int(1),
		int64(1),
		int32(1),
		int16(1),
		int8(1),
		uint(1),
		uint64(1),
		uint32(1),
		uint16(1),
		uint8(1),
		float64(1),
		float32(1),
	}
	for index, c := range successCases {
		i, err := any2Float64(c)
		require.NoError(t, err)
		require.Equalf(t, float64(1), i, "%d: %+v\n", index, reflect.TypeOf(c))
	}
	convertErrorCases := []any{
		"asdf",
		"1.1.1.1",
		"1-1",
	}
	for i, c := range convertErrorCases {
		_, err := any2Float64(c)
		require.ErrorIs(t, err, valueConvertErr, "case index: %d", i)
	}
	unexpectedValueTypeErrorCases := []any{
		os.File{},
		strings.Builder{},
		time.Time{},
	}
	for _, c := range unexpectedValueTypeErrorCases {
		_, err := any2Float64(c)
		require.ErrorIs(t, err, unexpectedValueTypeErr)
	}
}

func TestAny2Bool(t *testing.T) {
	successCases := []any{
		true,
		"true",
		"1",
		"True",
		"TRUE",
		"T",
	}
	for index, c := range successCases {
		i, err := any2Bool(c)
		require.NoError(t, err)
		require.Equalf(t, true, i, "%d: %+v\n", index, reflect.TypeOf(c))
	}
	convertErrorCases := []any{
		"fALsE",
		"tRuE",
		"2",
	}
	for i, c := range convertErrorCases {
		v, err := any2Bool(c)
		require.ErrorIs(t, err, valueConvertErr, "index: %d, v: %+v", i, v)
	}
	unexpectedValueTypeErrorCases := []any{
		1,
		int64(1),
		int32(1),
		int16(1),
		int8(1),
		uint(1),
		uint64(1),
		uint32(1),
		uint16(1),
		uint8(1),
		float64(1),
		float32(1),
		os.File{},
		strings.Builder{},
		time.Time{},
	}
	for _, c := range unexpectedValueTypeErrorCases {
		_, err := any2Bool(c)
		require.ErrorIs(t, err, unexpectedValueTypeErr)
	}
}

func TestAny2Time(t *testing.T) {
	now := time.Now().UTC()
	successCases := []any{
		now,
		now.Format(time.RFC3339Nano),
	}
	for index, c := range successCases {
		i, err := any2Time(c)
		require.NoError(t, err)
		require.Equalf(t, now, i, "%d: %+v\n", index, reflect.TypeOf(c))
	}
}

func TestAny2Str(t *testing.T) {
	successCases := []any{
		"1",
		1,
		int64(1),
		int32(1),
		int16(1),
		int8(1),
		uint(1),
		uint64(1),
		uint32(1),
		uint16(1),
		uint8(1),
		float64(1),
		float32(1),
	}
	for index, c := range successCases {
		i := any2Str(c)
		require.Equalf(t, "1", i, "%d: %+v\n", index, reflect.TypeOf(c))
	}
}
