package kqlfilter

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/pkg/errors"
)

type FilterToSquirrelSqlFieldColumnType int

const (
	FilterToSquirrelSqlFieldColumnTypeString = iota
	FilterToSquirrelSqlFieldColumnTypeInt
	FilterToSquirrelSqlFieldColumnTypeFloat
	FilterToSquirrelSqlFieldColumnTypeBool
	FilterToSquirrelSqlFieldColumnTypeTimestamp
)

type FilterToSquirrelSqlFieldConfig struct {
	// SQL table column name. Can be omitted if the column name is equal to the key in the fieldConfigs map.
	ColumnName string
	// SQL column type. Defaults to FilterToSquirrelSqlFieldColumnTypeString.
	ColumnType FilterToSquirrelSqlFieldColumnType
	// Allow prefix matching when a wildcard (`*`) is present at the end of a string.
	// Only applicable for FilterToSpannerFieldColumnTypeString. Defaults to false.
	AllowPrefixMatch bool
	// Allow multiple values for this field. Defaults to false.
	AllowMultipleValues bool
	// Allow this field to be queried with one or more range operators. Defaults to false.
	AllowRanges bool
	// A function that takes a string value as provided by the user and converts it to string result that matches how it
	// should be as users' input. This should return an error when the user is providing a value that is illegal or unexpected
	// for this particular field. Defaults to using the provided value as-is.
	MapValue func(string) (any, error)
	// A function that handle parsing the sql statement by itself.
	// If set, all other fields in the config will be ignored
	CustomBuilder func(stmt sq.SelectBuilder, operator string, values []string) (sq.SelectBuilder, error)
}

// ToSquirrelSql parses a Filter and attach the result the given squirrel sql select builder.
//
// It takes a map of fields that are allowed to be queried via this filter (as a user should not be able to query all
// db columns via a filter). It attaches the parsed result to a squirrel select builder as one or more where clauses.
// An example follows.
//
// Given a Filter that looks like this:
//
//	[(Field: "userId", Operator: "=", Values: []string{"12345"}), (Field: "status", Operator: "IN", Values: []string{"active", "frozen", "deleted})]
//
// and fieldConfigs that looks like this:
//
//	{
//		"userId": (ColumnName: "user_id", ColumnType: FilterToSquirrelSqlFieldColumnTypeInt64),
//		"status":  (ColumnName: "status",   ColumnType: FilterToSquirrelSqlFieldColumnTypeString, AllowMultipleValues: true)
//	}
//
// The given squirrel select builder will be attached with condition clauses as below by its Where() method:
//
//		stmt = stmt.Where(sq.Eq{"user_id":123456})
//		stmt = stmt.Where(sq.Eq{"status": []string{"active", "frozen", "deleted}})
//	 return stmt
//
// which will let you get the final sql where conditions like:
//
// ...... WHERE user_id = 123456 AND status in ("active","frozen","deleted") .....
//
// Note: the input timestamp format should always be time.RFC3339Nano
var unknownFieldErr = errors.Errorf("unknown field")

func (f Filter) ToSquirrelSql(stmt sq.SelectBuilder, fieldConfigs map[string]FilterToSquirrelSqlFieldConfig) (sq.SelectBuilder, error) {
	var err error

	for i, clause := range f.Clauses {
		fieldConfig, ok := fieldConfigs[clause.Field]
		if !ok {
			return stmt, errors.Wrapf(unknownFieldErr, "unknown field: %s", clause.Field)
		}

		stmt, err = clause.ToSquirrelSql(stmt, fieldConfig)
		if err != nil {
			return stmt, errors.Wrapf(err, "failed to parse clause %d to squirrel sql statement", i)
		}
	}
	return stmt, nil
}

func (c *Clause) ToSquirrelSql(stmt sq.SelectBuilder, config FilterToSquirrelSqlFieldConfig) (sq.SelectBuilder, error) {
	var err error
	// use customer parser if provided
	if config.CustomBuilder != nil {
		stmt, err = config.CustomBuilder(stmt, c.Operator, c.Values)
		if err != nil {
			return stmt, err
		}
		return stmt, nil
	}

	// get field name
	columnName := config.ColumnName
	if columnName == "" {
		columnName = c.Field
	}

	// use MapValue function in config if provided
	rawValues := make([]any, 0, len(c.Values))
	if config.MapValue != nil {
		mappedValues := make([]any, 0, len(rawValues))
		for i := range c.Values {
			mappedValue, err := config.MapValue(c.Values[i])
			if err != nil {
				return stmt, err
			}
			mappedValues = append(mappedValues, mappedValue)
		}
		rawValues = mappedValues
	} else {
		for i := range c.Values {
			rawValues = append(rawValues, c.Values[i])
		}
	}

	switch config.ColumnType {
	case FilterToSquirrelSqlFieldColumnTypeInt:
		nativeValues := make([]int64, 0, len(rawValues))
		for i, v := range rawValues {
			nativeValue, err := any2Int64(v)
			if err != nil {
				return stmt, errors.Wrapf(err, "failed to convert value %+v at index %d to int64", v, i)
			}
			nativeValues = append(nativeValues, nativeValue)
		}
		stmt, err = buildStmtByOperator[int64](stmt, columnName, c.Operator, nativeValues, config)
	case FilterToSquirrelSqlFieldColumnTypeFloat:
		nativeValues := make([]float64, 0, len(rawValues))
		for i, v := range rawValues {
			nativeValue, err := any2Float64(v)
			if err != nil {
				return stmt, errors.Wrapf(valueConvertErr, "failed to convert value %s (index %d in filter c values) to float64", v, i)
			}
			nativeValues = append(nativeValues, nativeValue)
		}
		stmt, err = buildStmtByOperator[float64](stmt, columnName, c.Operator, nativeValues, config)
	case FilterToSquirrelSqlFieldColumnTypeBool:
		nativeValues := make([]bool, 0, len(rawValues))
		for i, v := range rawValues {
			nativeValue, err := any2Bool(v)
			if err != nil {
				return stmt, errors.Wrapf(valueConvertErr, "failed to convert value %s (index %d in filter c values) to bool", v, i)
			}
			nativeValues = append(nativeValues, nativeValue)
		}
		stmt, err = buildStmtByOperator[bool](stmt, columnName, c.Operator, nativeValues, config)
	case FilterToSquirrelSqlFieldColumnTypeTimestamp:
		nativeValues := make([]time.Time, 0, len(rawValues))
		for i, v := range rawValues {
			nativeValue, err := any2Time(v)
			if err != nil {
				return stmt, errors.Wrapf(valueConvertErr, "failed to convert value %s (index %d in filter c values) to time.Time", v, i)
			}
			nativeValues = append(nativeValues, nativeValue)
		}
		stmt, err = buildStmtByOperator[time.Time](stmt, columnName, c.Operator, nativeValues, config)
	default:
		nativeValues := make([]string, 0, len(rawValues))
		for i, v := range rawValues {
			nativeValue := any2Str(v)
			if err != nil {
				return stmt, errors.Wrapf(valueConvertErr, "failed to convert value %s (index %d in filter c values) to time.Time", v, i)
			}
			nativeValues = append(nativeValues, nativeValue)
		}
		stmt, err = buildStmtByOperator[string](stmt, columnName, c.Operator, nativeValues, config)
	}

	if err != nil {
		return stmt, errors.Wrapf(err, "failed to build statement by operator")
	}
	return stmt, nil
}

var emptyValuesErr = errors.Errorf("no values provided")
var valuesNumError = errors.Errorf("wrong values num")
var operatorError = errors.Errorf("unsupported operator")

func buildStmtByOperator[T string | int64 | float64 | bool | time.Time](stmt sq.SelectBuilder, columnName string, op string, values []T, config FilterToSquirrelSqlFieldConfig) (sq.SelectBuilder, error) {
	switch op {
	case "IN":
		if len(values) == 0 {
			return stmt, emptyValuesErr
		}
		if len(values) > 1 && !config.AllowMultipleValues {
			return stmt, errors.Wrapf(valuesNumError, "values num %d doesn't match the operator %s", len(values), op)
		}
		stmt = stmt.Where(sq.Eq{columnName: values})
	case "=", ">", ">=", "<", "<=":
		if !config.AllowRanges && (op == ">" || op == ">=" || op == "<" || op == "<=") {
			return stmt, errors.Wrapf(operatorError, "operator %s not supported", op)
		}
		if len(values) != 1 {
			return stmt, errors.Wrapf(valuesNumError, "values num %d doesn't match the operator %s", len(values), op)
		}
		switch op {
		case "=":
			if vStr, ok := any(values[0]).(string); ok && config.AllowPrefixMatch && strings.HasSuffix(vStr, "*") && !strings.HasSuffix(vStr, `\*`) {
				vStr = vStr[:len(vStr)-1]                  // trim the suffix * ( don't use the TrimRightFunc because it'll also remove the first start from suffix "**"
				vStr = strings.ReplaceAll(vStr, `\`, `\\`) // escape all `\`
				vStr = strings.ReplaceAll(vStr, `%`, `\%`) // escape all `%`
				vStr = strings.ReplaceAll(vStr, `_`, `\_`) // escape all `_`
				stmt = stmt.Where(sq.Like{columnName: vStr + "%"})
			} else {
				stmt = stmt.Where(sq.Eq{columnName: values[0]})
			}
		case ">":
			stmt = stmt.Where(sq.Gt{columnName: values[0]})
		case ">=":
			stmt = stmt.Where(sq.GtOrEq{columnName: values[0]})
		case "<":
			stmt = stmt.Where(sq.Lt{columnName: values[0]})
		case "<=":
			stmt = stmt.Where(sq.LtOrEq{columnName: values[0]})
		}
	default:
		return stmt, errors.Wrapf(operatorError, "unsupported operator %s", op)
	}
	return stmt, nil
}

var valueConvertErr = errors.Errorf("value convert error") // used in test cases
var unexpectedValueTypeErr = errors.Errorf("unexpected value type")

func any2Int64(input any) (int64, error) {
	switch val := input.(type) {
	case string:
		result, err := strconv.ParseInt(val, 10, 64)
		if err != nil {

			return result, errors.Wrapf(valueConvertErr, "failed to convert value %s to int64", val)
		}
		return result, nil
	case int:
		return int64(val), nil
	case int64:
		return val, nil
	case int32:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case uint:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint8:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case float32:
		return int64(val), nil
	default:
		return 0, errors.Wrapf(unexpectedValueTypeErr, "value %+v type %+v doesn't support to be converted to int64", input, reflect.TypeOf(input))
	}
}

func any2Float64(input any) (float64, error) {
	switch val := input.(type) {
	case string:
		result, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return result, errors.Wrapf(valueConvertErr, "failed to convert value %s to float64", val)
		}
		return result, nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int8:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint16:
		return float64(val), nil
	case uint8:
		return float64(val), nil
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	default:
		return 0, errors.Wrapf(unexpectedValueTypeErr, "value %+v type %+v doesn't support to be converted to float64", input, reflect.TypeOf(input))
	}
}

func any2Bool(input any) (bool, error) {
	switch val := input.(type) {
	case bool:
		return val, nil
	case string:
		result, err := strconv.ParseBool(val)
		if err != nil {
			return result, errors.Wrapf(valueConvertErr, "failed to convert value %s to bool", val)
		}
		return result, nil
	default:
		return false, errors.Wrapf(unexpectedValueTypeErr, "value %+v type %+v doesn't support to be converted to bool", input, reflect.TypeOf(input))
	}
}

func any2Time(input any) (time.Time, error) {
	switch val := input.(type) {
	case time.Time:
		return val, nil
	case string:
		result, err := time.Parse(time.RFC3339Nano, val)
		if err != nil {
			return result, errors.Wrapf(valueConvertErr, "failed to convert value %s to time.Time", val)
		}
		return result, nil
	default:
		return time.Time{}, errors.Wrapf(unexpectedValueTypeErr, "value %+v type %+v doesn't support to be converted to bool", input, reflect.TypeOf(input))
	}
}

func any2Str(input any) string {
	switch val := input.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint16:
		return strconv.FormatUint(uint64(val), 10)
	case uint8:
		return strconv.FormatUint(uint64(val), 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	case fmt.Stringer:
		return val.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}
