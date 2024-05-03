package kqlfilter

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

type FilterToSpannerFieldColumnType int

const (
	FilterToSpannerFieldColumnTypeString = iota
	FilterToSpannerFieldColumnTypeInt64
	FilterToSpannerFieldColumnTypeFloat64
	FilterToSpannerFieldColumnTypeBool
	FilterToSpannerFieldColumnTypeTimestamp
)

func (c FilterToSpannerFieldColumnType) String() string {
	switch c {
	case FilterToSpannerFieldColumnTypeString:
		return "STRING"
	case FilterToSpannerFieldColumnTypeInt64:
		return "INT64"
	case FilterToSpannerFieldColumnTypeFloat64:
		return "FLOAT64"
	case FilterToSpannerFieldColumnTypeBool:
		return "BOOL"
	case FilterToSpannerFieldColumnTypeTimestamp:
		return "TIMESTAMP"
	default:
		return "???"
	}
}

type FilterToSpannerFieldConfig struct {
	// SQL table column name. Can be omitted if the column name is equal to the key in the fieldConfigs map.
	ColumnName string
	// SQL column type. Defaults to FilterToSpannerFieldColumnTypeString.
	ColumnType FilterToSpannerFieldColumnType
	// If true, the filter must at least contain this field. Will not apply to empty filters. Defaults to false.
	Required bool
	// Allow prefix matching when a wildcard (`*`) is present at the end of a string.
	// Only applicable for FilterToSpannerFieldColumnTypeString. Defaults to false.
	AllowPrefixMatch bool
	// Allow multiple values for this field. Defaults to false.
	AllowMultipleValues bool
	// Allow this field to be queried with one or more range operators. Defaults to false.
	AllowRanges bool
	// A list of aliases for this field. Can be used if you want to allow users to use different field names to filter
	// on the same column. Useful e.g. to allow different naming conventions, like `type_id` and `typeId`.
	Aliases []string
	// A function that takes a string value as provided by the user and converts it to `any` result that matches how it is
	// stored in the database. This should return an error when the user is providing a value that is illegal for this
	// particular field. Defaults to using the provided value as-is.
	MapValue func(string) (any, error)
}

func (f FilterToSpannerFieldConfig) mapValues(values []string) (any, error) {
	var outputValue any
	var err error
	if f.MapValue != nil {
		outputValue = make([]any, 0, len(values))
		for _, value := range values {
			mappedValue, err := f.MapValue(value)
			if err != nil {
				return nil, err
			}
			outputValue = append(outputValue.([]any), mappedValue)
		}
	} else {
		outputValue = values
	}

	// turn slice of one into a single value
	outputValue = unwrapSlice(outputValue)

	if !f.AllowMultipleValues && reflect.TypeOf(outputValue).Kind() == reflect.Slice {
		return nil, fmt.Errorf("multiple values are not allowed")
	}

	switch ov := outputValue.(type) {
	// convert single string value if needed
	case string:
		outputValue, err = f.convertValue(ov)
		if err != nil {
			return nil, err
		}

	// If output value is a slice of strings, convert each value in the slice if needed
	case []string:
		switch f.ColumnType {
		case FilterToSpannerFieldColumnTypeInt64:
			outSlice := make([]int64, len(ov))
			for i, v := range ov {
				val, err := f.convertValue(v)
				if err != nil {
					return nil, err
				}
				outSlice[i] = val.(int64)
			}
			outputValue = outSlice
		case FilterToSpannerFieldColumnTypeFloat64:
			outSlice := make([]float64, len(ov))
			for i, v := range ov {
				val, err := f.convertValue(v)
				if err != nil {
					return nil, err
				}
				outSlice[i] = val.(float64)
			}
			outputValue = outSlice
		case FilterToSpannerFieldColumnTypeBool:
			outSlice := make([]bool, len(ov))
			for i, v := range ov {
				val, err := f.convertValue(v)
				if err != nil {
					return nil, err
				}
				outSlice[i] = val.(bool)
			}
			outputValue = outSlice
		case FilterToSpannerFieldColumnTypeTimestamp:
			outSlice := make([]time.Time, len(ov))
			for i, v := range ov {
				val, err := f.convertValue(v)
				if err != nil {
					return nil, err
				}
				outSlice[i] = val.(time.Time)
			}
			outputValue = outSlice
		}
	}

	return outputValue, nil
}

func (f FilterToSpannerFieldConfig) convertValue(value string) (any, error) {
	switch f.ColumnType {
	case FilterToSpannerFieldColumnTypeInt64:
		intVal, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid INT64 value: %w", err)
		}
		return intVal, nil

	case FilterToSpannerFieldColumnTypeFloat64:
		doubleVal, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid FLOAT64 value: %w", err)
		}
		return doubleVal, nil

	case FilterToSpannerFieldColumnTypeBool:
		boolVal, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid BOOL value: %w", err)
		}
		return boolVal, nil

	case FilterToSpannerFieldColumnTypeTimestamp:
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, fmt.Errorf("invalid TIMESTAMP value: %w", err)
		}
		return t, nil
	default:
		return value, nil
	}
}

func unwrapSlice(v any) any {
	if reflect.TypeOf(v).Kind() == reflect.Slice {
		if reflect.ValueOf(v).Len() == 1 {
			return reflect.ValueOf(v).Index(0).Interface()
		}
	}
	return v
}

// ToSpannerSQL turns a Filter into a partial StandardSQL statement.
// It takes a map of fields that are allowed to be queried via this filter (as a user should not be able to query all
// db columns via a filter). It returns a partial SQL statement that can be added to a WHERE clause, along with
// associated params. An example follows.
//
// Given a Filter that looks like this:
//
//	[(Field: "userId", Operator: "=", Values: []string{"12345"}), (Field: "email", Operator: "=", Values: []string{"john@example.*"})]
//
// and fieldConfigs that looks like this:
//
//	{
//		"userId": (ColumnName: "user_id", ColumnType: FilterToSpannerFieldColumnTypeInt64,  AllowPartialMatch: false),
//		"email":  (ColumnName: "email",   ColumnType: FilterToSpannerFieldColumnTypeString, AllowPartialMatch: true)
//	}
//
// This returns a slice of SQL conditions that can be appended to an existing WHERE clause (make sure to AND these first):
//
//	["user_id=@KQL0", "email LIKE @KQL1"]
//
// and params:
//
//	{
//		"@KQL0": int64(12345),
//		"@KQL1": "john@example.%"
//	}
//
// For multi-value fields, the returned SQL conditions will use the IN operator:
//
//	[(Field: "team_id", Operator: "IN", Values: []string{"T1", "T2"})]
//
//	{
//		"team_id": (ColumnName: "user_id", ColumnType: FilterToSpannerFieldColumnTypeString, AllowMultipleValues: true),
//	}
//
// SQL would be:
//
//	["team_id IN (@KQL0,@KQL1)"]
//
// and params:
//
//	{
//		"@KQL0": "T1",
//		"@KQL1": "T2"
//	}
//
// Note: The Clause Operator is contextually used/ignored. It only works with INT64, FLOAT64 and TIMESTAMP types currently.
func (f Filter) ToSpannerSQL(fieldConfigs map[string]FilterToSpannerFieldConfig) ([]string, map[string]any, error) {
	var condAnds []string
	params := make(map[string]any)

	paramIndex := 0

	for _, clause := range f.Clauses {
		fieldConfig, ok := fieldConfigs[clause.Field]
		if !ok {
			// There may be an alias defined on one of the other fieldConfigs
			for _, fc := range fieldConfigs {
				for _, alias := range fc.Aliases {
					if alias == clause.Field {
						fieldConfig = fc
						ok = true
						break
					}
				}
				if ok {
					break
				}
			}

			if !ok {
				return nil, nil, fmt.Errorf("unknown field: %s", clause.Field)
			}
		}

		columnName := fieldConfig.ColumnName
		if columnName == "" {
			columnName = clause.Field
		}
		mappedValue, err := fieldConfig.mapValues(clause.Values)
		if err != nil {
			return nil, nil, fmt.Errorf("field %s: %w", clause.Field, err)
		}

		operator := clause.Operator

		if len(clause.Values) > 1 && operator != "IN" {
			return nil, nil, fmt.Errorf("operator %s doesn't support multiple values in field: %s", operator, clause.Field)
		}

		whereClauseFormat := "%s%s@%s"
		switch operator {
		case "IN":
			switch fieldConfig.ColumnType {
			case FilterToSpannerFieldColumnTypeString:
				mappedValue, err = parseAnyToSlice[string](mappedValue)
				if err == nil {
					mappedValue = uniqueSliceElements(mappedValue.([]string))
				}
			case FilterToSpannerFieldColumnTypeInt64:
				mappedValue, err = parseAnyToSlice[int64](mappedValue)
				if err == nil {
					mappedValue = uniqueSliceElements(mappedValue.([]int64))
				}
			case FilterToSpannerFieldColumnTypeFloat64:
				mappedValue, err = parseAnyToSlice[float64](mappedValue)
				if err == nil {
					mappedValue = uniqueSliceElements(mappedValue.([]float64))
				}
			case FilterToSpannerFieldColumnTypeTimestamp:
				mappedValue, err = parseAnyToSlice[time.Time](mappedValue)
				if err == nil {
					mappedValue = uniqueSliceElements(mappedValue.([]time.Time))
				}
			default:
				return nil, nil, fmt.Errorf("operator %s not supported for field type %s", operator, fieldConfig.ColumnType)
			}
			if err != nil {
				return nil, nil, err
			}

			whereClauseFormat = "%s %s UNNEST(@%s)"
		case "=":
			// Prefix match supported only for single string
			mappedString, isString := mappedValue.(string)
			if fieldConfig.AllowPrefixMatch && isString && strings.HasSuffix(mappedString, "*") && !strings.HasSuffix(mappedString, "\\*") {
				operator = " LIKE "
				// escape all instances of \ in the string
				mappedString = strings.ReplaceAll(mappedString, `\`, `\\`)
				// escape all instances of _ in the string
				mappedString = strings.ReplaceAll(mappedString, `_`, `\_`)
				// escape all instances of % in the string
				mappedString = strings.ReplaceAll(mappedString, `%`, `\%`)
				// replace the trailing * with a %
				mappedValue = mappedString[0:len(mappedString)-1] + "%"
				break
			}

		case ">=", "<=", ">", "<":
			if !fieldConfig.AllowRanges {
				return nil, nil, fmt.Errorf("operator %s not supported for field: %s", operator, clause.Field)
			}

			switch fieldConfig.ColumnType {
			case FilterToSpannerFieldColumnTypeInt64, FilterToSpannerFieldColumnTypeFloat64, FilterToSpannerFieldColumnTypeTimestamp:
				break
			default:
				return nil, nil, fmt.Errorf("operator %s not supported for field type %s", operator, fieldConfig.ColumnType)
			}
		}

		paramName := fmt.Sprintf("%s%d", "KQL", paramIndex)
		condAnds = append(condAnds, fmt.Sprintf(whereClauseFormat, columnName, operator, paramName))
		params[paramName] = mappedValue
		paramIndex++
	}

	for field, fieldConfig := range fieldConfigs {
		if fieldConfig.Required {
			found := false
			for _, clause := range f.Clauses {
				if clause.Field == field || (slices.Contains(fieldConfig.Aliases, clause.Field)) {
					found = true
					break
				}
			}
			if !found {
				return nil, nil, fmt.Errorf("required field %s missing", field)
			}
		}
	}

	return condAnds, params, nil
}

func parseAnyToSlice[T any](s any) ([]T, error) {
	if s == nil {
		return nil, nil
	}
	switch sVal := s.(type) {
	case T:
		return []T{sVal}, nil
	case []T:
		return sVal, nil
	case []any:
		var typeSlice []T
		for i := range sVal {
			typeVal, ok := sVal[i].(T)
			if !ok {
				return nil, fmt.Errorf("values' type in any slice doesn't match the expectation, value type: [%+v], expect: [%+v]", reflect.TypeOf(sVal[i]), reflect.TypeOf(*new(T)))
			}
			typeSlice = append(typeSlice, typeVal)
		}
		return typeSlice, nil
	default:
		return nil, fmt.Errorf("cannot parse input to a slice")
	}
}

// uniqueSliceElements removes any duplicate elements in a slice
func uniqueSliceElements[T comparable](inputSlice []T) []T {
	uniqueSlice := make([]T, 0, len(inputSlice))
	seen := make(map[T]bool, len(inputSlice))
	for _, element := range inputSlice {
		if !seen[element] {
			uniqueSlice = append(uniqueSlice, element)
			seen[element] = true
		}
	}
	return uniqueSlice
}
