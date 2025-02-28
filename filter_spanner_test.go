package kqlfilter

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToSpannerSQL(t *testing.T) {
	// All of those should return an error.
	testCases := []struct {
		name           string
		input          string
		columnMap      map[string]FilterToSpannerFieldConfig
		expectedError  bool
		expectedSQL    string
		expectedParams map[string]any
	}{
		{
			"one integer field",
			"userId:12345",
			map[string]FilterToSpannerFieldConfig{
				"userId": {
					ColumnName: "user_id",
					ColumnType: FilterToSpannerFieldColumnTypeInt64,
				},
			},
			false,
			"(user_id=@KQL0)",
			map[string]any{
				"KQL0": int64(12345),
			},
		},
		{
			"one boolean field without value",
			"false",
			map[string]FilterToSpannerFieldConfig{},
			false,
			"(1=@KQL0)",
			map[string]any{
				"KQL0": int64(0),
			},
		},
		{
			"one boolean field without value and one user id",
			"false and userId:123",
			map[string]FilterToSpannerFieldConfig{
				"userId": {
					ColumnName: "user_id",
					ColumnType: FilterToSpannerFieldColumnTypeInt64,
				},
			},
			false,
			"(1=@KQL0 AND user_id=@KQL1)",
			map[string]any{
				"KQL0": int64(0),
				"KQL1": int64(123),
			},
		},
		{
			"one integer field and one string field",
			"userId:12345 email:johnexamplecom",
			map[string]FilterToSpannerFieldConfig{
				"userId": {
					ColumnName: "u.user_id",
					ColumnType: FilterToSpannerFieldColumnTypeInt64,
				},
				"email": {
					ColumnType: FilterToSpannerFieldColumnTypeString,
				},
			},
			false,
			"(u.user_id=@KQL0 AND email=@KQL1)",
			map[string]any{
				"KQL0": int64(12345),
				"KQL1": "johnexamplecom",
			},
		},
		{
			"one integer field and one string field with no partial matching allowed",
			"userId:12345 email:*examplecom", map[string]FilterToSpannerFieldConfig{
			"userId": {
				ColumnName: "u.user_id",
				ColumnType: FilterToSpannerFieldColumnTypeInt64,
			},
			"email": {
				ColumnType: FilterToSpannerFieldColumnTypeString,
			},
		},
			false,
			"(u.user_id=@KQL0 AND email=@KQL1)",
			map[string]any{
				"KQL0": int64(12345),
				"KQL1": "*examplecom",
			},
		},
		{
			"one integer field and one string field with prefix matching allowed",
			"userId:12345 email:johnexample*", map[string]FilterToSpannerFieldConfig{
			"userId": {
				ColumnName: "u.user_id",
				ColumnType: FilterToSpannerFieldColumnTypeInt64,
			},
			"email": {
				ColumnType:       FilterToSpannerFieldColumnTypeString,
				AllowPrefixMatch: true,
			},
		},
			false,
			"(u.user_id=@KQL0 AND email LIKE @KQL1)",
			map[string]any{
				"KQL0": int64(12345),
				"KQL1": "johnexample%",
			},
		},
		{
			"escape percentage sign with wildcard suffix allowed",
			"discount_string:70%*", map[string]FilterToSpannerFieldConfig{
			"discount_string": {
				ColumnType:       FilterToSpannerFieldColumnTypeString,
				AllowPrefixMatch: true,
			},
		},
			false,
			"(discount_string LIKE @KQL0)",
			map[string]any{
				"KQL0": "70\\%%",
			},
		},
		{
			"one integer field and one string field with wildcards allowed, illegal wildcard in middle",
			"userId:12345 email:*example*com", map[string]FilterToSpannerFieldConfig{
			"userId": FilterToSpannerFieldConfig{
				ColumnName: "u.user_id",
				ColumnType: FilterToSpannerFieldColumnTypeInt64,
			},
			"email": FilterToSpannerFieldConfig{
				ColumnType:       FilterToSpannerFieldColumnTypeString,
				AllowPrefixMatch: true,
			},
		},
			false,
			"(u.user_id=@KQL0 AND email=@KQL1)",
			map[string]any{
				"KQL0": int64(12345),
				"KQL1": "*example*com",
			},
		},
		{
			"email prefix",
			"email:john@*", map[string]FilterToSpannerFieldConfig{
			"email": FilterToSpannerFieldConfig{
				ColumnType:       FilterToSpannerFieldColumnTypeString,
				AllowPrefixMatch: true,
			},
		},
			false,
			"(email LIKE @KQL0)",
			map[string]any{
				"KQL0": "john@%",
			},
		},
		{
			"email suffix",
			"email:*@example.com", map[string]FilterToSpannerFieldConfig{
			"email": FilterToSpannerFieldConfig{
				ColumnType:       FilterToSpannerFieldColumnTypeString,
				AllowSuffixMatch: true,
			},
		},
			false,
			"(email LIKE @KQL0)",
			map[string]any{
				"KQL0": "%@example.com",
			},
		},
		{
			"email prefix and suffix",
			"email:*@example.*", map[string]FilterToSpannerFieldConfig{
			"email": FilterToSpannerFieldConfig{
				ColumnType:       FilterToSpannerFieldColumnTypeString,
				AllowPrefixMatch: true,
				AllowSuffixMatch: true,
			},
		},
			false,
			"(email LIKE @KQL0)",
			map[string]any{
				"KQL0": "%@example.%",
			},
		},
		{
			"illegal email suffix",
			"email:*@example.com", map[string]FilterToSpannerFieldConfig{
			"email": FilterToSpannerFieldConfig{
				ColumnType:       FilterToSpannerFieldColumnTypeString,
				AllowPrefixMatch: true,
			},
		},
			false,
			"(email=@KQL0)",
			map[string]any{
				"KQL0": "*@example.com",
			},
		},
		{
			"email force lowercasing on prefix match",
			"email:joHN@exAmple.*", map[string]FilterToSpannerFieldConfig{
			"email": FilterToSpannerFieldConfig{
				ColumnType:                FilterToSpannerFieldColumnTypeString,
				AllowPrefixMatch:          true,
				AllowCaseInsensitiveMatch: true,
			},
		},
			false,
			"(LOWER(email) LIKE LOWER(@KQL0))",
			map[string]any{
				"KQL0": "joHN@exAmple.%",
			},
		},
		{
			"avoid email force lowercasing on prefix match",
			"email:joHN@exAmple.*", map[string]FilterToSpannerFieldConfig{
			"email": FilterToSpannerFieldConfig{
				ColumnType:       FilterToSpannerFieldColumnTypeString,
				AllowPrefixMatch: true,
			},
		},
			false,
			"(email LIKE @KQL0)",
			map[string]any{
				"KQL0": "joHN@exAmple.%",
			},
		},
		{
			"email match with proper casing",
			"email:john@EXAMPLE.com", map[string]FilterToSpannerFieldConfig{
			"email": FilterToSpannerFieldConfig{
				ColumnType:       FilterToSpannerFieldColumnTypeString,
				AllowPrefixMatch: true,
			},
		},
			false,
			"(email=@KQL0)",
			map[string]any{
				"KQL0": "john@EXAMPLE.com",
			},
		},
		{
			"disallow column without alias",
			"user_id:12345", map[string]FilterToSpannerFieldConfig{
			"userId": {
				ColumnName: "u.user_id",
				ColumnType: FilterToSpannerFieldColumnTypeInt64,
			},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"allowed column via alias",
			"user_id:12345", map[string]FilterToSpannerFieldConfig{
			"userId": {
				ColumnName: "u.user_id",
				ColumnType: FilterToSpannerFieldColumnTypeInt64,
				Aliases:    []string{"user_id"},
			},
		},
			false,
			"(u.user_id=@KQL0)",
			map[string]any{
				"KQL0": int64(12345),
			},
		},
		{
			"negation on empty string",
			"not name:\"\"", map[string]FilterToSpannerFieldConfig{
			"name": {
				ColumnName:    "Name",
				ColumnType:    FilterToSpannerFieldColumnTypeString,
				AllowNegation: true,
			},
		},
			false,
			"(Name!=@KQL0)",
			map[string]any{
				"KQL0": "",
			},
		},
		{
			"negation on non-empty string",
			"not name:\"John Adams\"", map[string]FilterToSpannerFieldConfig{
			"name": {
				ColumnName:    "Name",
				ColumnType:    FilterToSpannerFieldColumnTypeString,
				AllowNegation: true,
			},
		},
			false,
			"(Name!=@KQL0)",
			map[string]any{
				"KQL0": "John Adams",
			},
		},
		{
			"negation on number",
			"not amount:5", map[string]FilterToSpannerFieldConfig{
			"amount": {
				ColumnName:    "Amount",
				ColumnType:    FilterToSpannerFieldColumnTypeInt64,
				AllowNegation: true,
			},
		},
			false,
			"(Amount!=@KQL0)",
			map[string]any{
				"KQL0": int64(5),
			},
		},
		{
			"disallow negation",
			"not name:\"John Adams\"", map[string]FilterToSpannerFieldConfig{
			"name": {
				ColumnName: "Name",
				ColumnType: FilterToSpannerFieldColumnTypeString,
			},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"disallowed second column",
			"userId:12345 password:qwertyuiop", map[string]FilterToSpannerFieldConfig{
			"userId": {
				ColumnName: "u.user_id",
				ColumnType: FilterToSpannerFieldColumnTypeInt64,
			},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"disallowed second column, but ignored explicitly",
			"userId:12345 password:qwertyuiop", map[string]FilterToSpannerFieldConfig{
			"userId": {
				ColumnName: "u.user_id",
				ColumnType: FilterToSpannerFieldColumnTypeInt64,
			},
			"password": {
				Ignore: true,
			},
		},
			false,
			"(u.user_id=@KQL0)",
			map[string]any{
				"KQL0": int64(12345),
			},
		},
		{
			"disallowed field value",
			"state:deleted", map[string]FilterToSpannerFieldConfig{
			"state": {
				MapValue: func(inputValue string) (any, error) {
					switch inputValue {
					case "active":
						return "active", nil
					case "canceled":
						return "canceled", nil
					case "expired":
						return "expired", nil
					}
					return nil, errors.New("illegal value provided")
				},
			},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"allowed field value with implicit column value",
			"state:active", map[string]FilterToSpannerFieldConfig{
			"state": {
				MapValue: func(inputValue string) (any, error) {
					switch inputValue {
					case "active":
						return "active", nil
					case "canceled":
						return "canceled", nil
					case "expired":
						return "expired", nil
					}
					return nil, errors.New("illegal value provided")
				},
			},
		},
			false,
			"(state=@KQL0)",
			map[string]any{
				"KQL0": "active",
			},
		},
		{
			"allowed field value with input and column values differing",
			"state:payment_state_active", map[string]FilterToSpannerFieldConfig{
			"state": {
				MapValue: func(inputValue string) (any, error) {
					switch inputValue {
					case "payment_state_active":
						return "active", nil
					case "payment_state_canceled":
						return "canceled", nil
					case "payment_state_expired":
						return "expired", nil
					}
					return nil, errors.New("illegal value provided")
				},
			},
		},
			false,
			"(state=@KQL0)",
			map[string]any{
				"KQL0": "active",
			},
		},
		{
			"FLOAT64 columns and BOOL",
			"lat:52.4052963 lon:4.8856547 exact:false", map[string]FilterToSpannerFieldConfig{
			"lat":   {ColumnType: FilterToSpannerFieldColumnTypeFloat64},
			"lon":   {ColumnType: FilterToSpannerFieldColumnTypeFloat64},
			"exact": {ColumnType: FilterToSpannerFieldColumnTypeBool},
		},
			false,
			"(lat=@KQL0 AND lon=@KQL1 AND exact=@KQL2)",
			map[string]any{
				"KQL0": 52.4052963,
				"KQL1": 4.8856547,
				"KQL2": false,
			},
		},
		{
			"fuzzy booleans",
			"truthy:1 falsey:0 also_truthy:t", map[string]FilterToSpannerFieldConfig{
			"truthy": {ColumnType: FilterToSpannerFieldColumnTypeBool},
			"falsey": {ColumnType: FilterToSpannerFieldColumnTypeBool},
			"also_truthy": {
				ColumnName: "alsoTruthy",
				ColumnType: FilterToSpannerFieldColumnTypeBool,
			},
		},
			false,
			"(truthy=@KQL0 AND falsey=@KQL1 AND alsoTruthy=@KQL2)",
			map[string]any{
				"KQL0": true,
				"KQL1": false,
				"KQL2": true,
			},
		},
		{
			"all four range operators",
			"userId>=12345 lat<50.0 lon>4.1 date<=\"2023-06-01T23:00:00.20Z\"", map[string]FilterToSpannerFieldConfig{
			"userId": {
				ColumnName:  "user_id",
				ColumnType:  FilterToSpannerFieldColumnTypeInt64,
				AllowRanges: true,
			},
			"lat": {
				ColumnType:  FilterToSpannerFieldColumnTypeFloat64,
				AllowRanges: true,
			},
			"lon": {
				ColumnType:  FilterToSpannerFieldColumnTypeFloat64,
				AllowRanges: true,
			},
			"date": {
				ColumnType:  FilterToSpannerFieldColumnTypeTimestamp,
				AllowRanges: true,
			},
		},
			false,
			"(user_id>=@KQL0 AND lat<@KQL1 AND lon>@KQL2 AND date<=@KQL3)",
			map[string]any{
				"KQL0": int64(12345),
				"KQL1": 50.0,
				"KQL2": 4.1,
				"KQL3": time.Date(2023, time.June, 1, 23, 0, 0, 200000000, time.UTC),
			},
		},
		{
			"try a range operator on a field that does not support it",
			"userId>=12345 date<=\"2023-06-01T23:00:00.20Z\"", map[string]FilterToSpannerFieldConfig{
			"userId": {
				ColumnName:  "user_id",
				ColumnType:  FilterToSpannerFieldColumnTypeInt64,
				AllowRanges: false,
			},
			"date": {
				ColumnType:  FilterToSpannerFieldColumnTypeTimestamp,
				AllowRanges: true,
			},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"repeat query on same field more than allowed",
			"count>=1 and count<5 and count>3", map[string]FilterToSpannerFieldConfig{
			"count": {},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"in query",
			"state:(state_active OR state_canceled)", map[string]FilterToSpannerFieldConfig{
			"state": {
				ColumnType:          FilterToSpannerFieldColumnTypeString,
				AllowMultipleValues: true,
				MapValue: func(inputValue string) (any, error) {
					switch inputValue {
					case "state_active":
						return "active", nil
					case "state_canceled":
						return "canceled", nil
					case "state_expired":
						return "expired", nil
					}
					return nil, errors.New("illegal value provided")
				},
			},
		},
			false,
			"(state IN UNNEST(@KQL0))",
			map[string]any{
				"KQL0": []string{"active", "canceled"},
			},
		},
		{
			"in query deduplication of identical values",
			"state:(active OR active)", map[string]FilterToSpannerFieldConfig{
			"state": {
				ColumnType:          FilterToSpannerFieldColumnTypeString,
				AllowMultipleValues: true,
			},
		},
			false,
			"(state IN UNNEST(@KQL0))",
			map[string]any{
				"KQL0": []string{"active"},
			},
		},
		{
			"do not deduplicate if values are not identical",
			"state:(active OR Active)", map[string]FilterToSpannerFieldConfig{
			"state": {
				ColumnType:          FilterToSpannerFieldColumnTypeString,
				AllowMultipleValues: true,
			},
		},
			false,
			"(state IN UNNEST(@KQL0))",
			map[string]any{
				"KQL0": []string{"active", "Active"},
			},
		},
		{
			"in query - disabled",
			"state:(active OR canceled)", map[string]FilterToSpannerFieldConfig{
			"state": {
				AllowMultipleValues: false,
				MapValue: func(inputValue string) (any, error) {
					switch inputValue {
					case "active":
						return "active", nil
					case "canceled":
						return "canceled", nil
					case "expired":
						return "expired", nil
					}
					return nil, errors.New("illegal value provided")
				},
			},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"in query - int",
			"user_id:(123 OR 321)", map[string]FilterToSpannerFieldConfig{
			"user_id": {
				ColumnName:          "UserID",
				ColumnType:          FilterToSpannerFieldColumnTypeInt64,
				AllowMultipleValues: true,
			},
		},
			false,
			"(UserID IN UNNEST(@KQL0))",
			map[string]any{
				"KQL0": []int64{123, 321},
			},
		},
		{
			"in query - bool",
			"user_id:(true OR false)", map[string]FilterToSpannerFieldConfig{
			"user_id": {
				ColumnName:          "UserID",
				ColumnType:          FilterToSpannerFieldColumnTypeBool,
				AllowMultipleValues: true,
			},
		},
			true, // operator IN not supported for field type BOOL
			"",
			map[string]any{},
		},
		{
			"containedBy operator query",
			"sports<@(soccer AND basketball AND handball)", map[string]FilterToSpannerFieldConfig{
			"sports": {
				ColumnType:          FilterToSpannerFieldColumnTypeString,
				AllowMultipleValues: true,
				AllowContainedBy:    true,
			},
		},
			false,
			"(ARRAY_LENGTH(sports) = ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(sports) AS x WHERE x IN UNNEST(@KQL0))))",
			map[string]any{
				"KQL0": []string{"soccer", "basketball", "handball"},
			},
		},
		{
			"contains operator query",
			"sports>@(soccer AND basketball AND handball)", map[string]FilterToSpannerFieldConfig{
			"sports": {
				ColumnType:          FilterToSpannerFieldColumnTypeString,
				AllowMultipleValues: true,
				AllowContains:       true,
			},
		},
			false,
			"(ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(@KQL0) AS x WHERE x IN UNNEST(sports))) = ARRAY_LENGTH(@KQL0))",
			map[string]any{
				"KQL0": []string{"soccer", "basketball", "handball"},
			},
		},
		{
			"containedBy operator query - disallowed",
			"sports<@(soccer AND basketball AND handball)", map[string]FilterToSpannerFieldConfig{
			"sports": {
				ColumnType:          FilterToSpannerFieldColumnTypeString,
				AllowMultipleValues: true,
			},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"contains operator query - disallowed",
			"sports>@(soccer AND basketball AND handball)", map[string]FilterToSpannerFieldConfig{
			"sports": {
				ColumnType:          FilterToSpannerFieldColumnTypeString,
				AllowMultipleValues: true,
			},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"required field - field present",
			"video_id:abcd and type_id:xyz", map[string]FilterToSpannerFieldConfig{
			"video_id": {
				ColumnName: "VideoID",
				ColumnType: FilterToSpannerFieldColumnTypeString,
				Required:   true,
			},
			"type_id": {
				ColumnName: "TypeID",
				ColumnType: FilterToSpannerFieldColumnTypeString,
			},
		},
			false,
			"(VideoID=@KQL0 AND TypeID=@KQL1)",
			map[string]any{
				"KQL0": "abcd",
				"KQL1": "xyz",
			},
		},
		{
			"required field - field absent",
			"type_id:xyz", map[string]FilterToSpannerFieldConfig{
			"video_id": {
				ColumnName: "VideoID",
				ColumnType: FilterToSpannerFieldColumnTypeString,
				Required:   true,
			},
			"type_id": {
				ColumnName: "TypeID",
				ColumnType: FilterToSpannerFieldColumnTypeString,
			},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"requires other field - field present",
			"video_id:abcd and type_id:xyz", map[string]FilterToSpannerFieldConfig{
			"video_id": {
				ColumnName: "VideoID",
				ColumnType: FilterToSpannerFieldColumnTypeString,
				Requires:   []string{"type_id"},
			},
			"type_id": {
				ColumnName: "TypeID",
				ColumnType: FilterToSpannerFieldColumnTypeString,
			},
		},
			false,
			"(VideoID=@KQL0 AND TypeID=@KQL1)",
			map[string]any{
				"KQL0": "abcd",
				"KQL1": "xyz",
			},
		},
		{
			"requires other field - field absent",
			"video_id:abcd", map[string]FilterToSpannerFieldConfig{
			"video_id": {
				ColumnName: "VideoID",
				ColumnType: FilterToSpannerFieldColumnTypeString,
				Requires:   []string{"type_id"},
			},
		},
			true,
			"",
			map[string]any{},
		},
		{
			"requires other field - all relevant fields absent",
			"unrelated:true", map[string]FilterToSpannerFieldConfig{
			"video_id": {
				ColumnName: "VideoID",
				ColumnType: FilterToSpannerFieldColumnTypeString,
				Requires:   []string{"type_id"},
			},
			"type_id": {
				ColumnName: "TypeID",
				ColumnType: FilterToSpannerFieldColumnTypeString,
			},
			"unrelated": {
				ColumnName: "Unrelated",
				ColumnType: FilterToSpannerFieldColumnTypeBool,
			},
		},
			false,
			"(Unrelated=@KQL0)",
			map[string]any{
				"KQL0": true,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			f, errParse := Parse(test.input)
			condAnds, params, err := f.ToSpannerSQL(test.columnMap)
			if test.expectedError {
				if errParse == nil && err == nil {
					t.Errorf("expected error, but got none")
				}
				return
			} else {
				require.NoError(t, errParse)
				require.NoError(t, err)
			}

			sql := ""
			if len(condAnds) > 0 {
				sql = "(" + strings.Join(condAnds, " AND ") + ")"
			}
			assert.Equal(t, test.expectedSQL, sql)
			assert.Equal(t, test.expectedParams, params)
		})
	}
}
