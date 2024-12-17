package kqlfilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAST(t *testing.T) {
	testCases := []struct {
		name          string
		input         string
		expectedError bool
		expected      string
	}{
		{
			"simple filter",
			"field:value",
			false,
			"field=value",
		},
		{
			"quoted",
			`field:"value"`,
			false,
			"field=value",
		},
		{
			"quoted 2",
			`field:"value AND x"`,
			false,
			"field=value AND x",
		},
		{
			"boolean without value",
			"false",
			false,
			"false",
		},
		{
			"wildcard",
			"field:value*",
			false,
			"field=value*",
		},
		{
			"wildcard prefix",
			"field:*value",
			false,
			"field=*value",
		},
		{
			"multi filter",
			"field:value second:filter",
			false,
			"(field=value AND second=filter)",
		},
		{
			"boolean OR",
			"field:value OR second: filter",
			false,
			"(field=value OR second=filter)",
		},
		{
			"boolean mixed",
			"first:x OR second:y AND third:z",
			false,
			"(first=x OR (second=y AND third=z))",
		},
		{
			"boolean mixed 2",
			"first:x OR second:y and NOT third:z",
			false,
			"(first=x OR (second=y AND NOT third=z))",
		},
		{
			"boolean mixed 3",
			"(first:x OR second:y) and NOT third:z",
			false,
			"((first=x OR second=y) AND NOT third=z)",
		},
		{
			"or values",
			"field:(x OR y AND z)",
			false,
			"field=(x OR (y AND z))",
		},
		{
			"nested values",
			"field:{nested:x or y:z}",
			false,
			"field={(nested=x OR y=z)}",
		},
		{
			"ranges",
			`start_time >= "2022-02-02T10:30:00.000Z" start_time < "2022-02-03T10:30:00.000Z"`,
			false,
			"(start_time>=2022-02-02T10:30:00.000Z AND start_time<2022-02-03T10:30:00.000Z)",
		},
		{
			"ranges2",
			`start_time>"2022-02-02T10:30:00.000Z" OR start_time<="2022-02-03T10:30:00.000Z"`,
			false,
			"(start_time>2022-02-02T10:30:00.000Z OR start_time<=2022-02-03T10:30:00.000Z)",
		},
		{
			"escapes",
			"field\\(x\\):separated\\:value",
			false,
			"field(x)=separated:value",
		},
		{
			"escapes 2",
			"field:slashed\\\\value",
			false,
			"field=slashed\\value",
		},
		{
			"escapes 3",
			"field:\\and",
			false,
			"field=and",
		},
		{
			"invalid wildcard",
			"value*",
			true,
			"",
		},
		{
			"invalid parenthesis",
			"field:(x OR y",
			true,
			"",
		},
		{
			"invalid syntax",
			"field>(x OR y)",
			true,
			"",
		},
		{
			"invalid syntax 2",
			"field :: value",
			true,
			"",
		},
		{
			"invalid syntax 3",
			"field < :value",
			true,
			"",
		},
		{
			"invalid syntax 4",
			"some:field AND OR another:field",
			true,
			"",
		},
		{
			"invalid syntax 5",
			"some:field,another:field",
			true,
			"",
		},
		{
			"syntax that includes percentage and wildcard",
			"discount_string:70%*",
			false,
			"discount_string=70%*",
		},
		{
			"not syntax",
			"not field:value",
			false,
			"NOT field=value",
		},
		{
			"not value",
			"field:NOT value",
			true,
			"",
		},
		{
			"nesting error",
			"a:{b:{c:{d:{e:{f:{g:{h:{i:{j:{k:{l:{m:{n:{o:{p:{q:{r:{s:{t:{u:{v:{w:{x:{y:{z:1}}}}}}}}}}}}}}}}}}}}}}}}}",
			true,
			"",
		},
		{
			"nesting",
			"a:{b:{c:{d:{e:{f:{g:{h:{i:{j:{k:{l:{m:{n:{o:{p:{q:{r:{s:{t:1}}}}}}}}}}}}}}}}}}}",
			false,
			"a={b={c={d={e={f={g={h={i={j={k={l={m={n={o={p={q={r={s={t=1}}}}}}}}}}}}}}}}}}}",
		},
		{
			"complexity error",
			"field:(a OR b OR c OR d OR e OR f OR g OR h OR i OR j OR k OR l) OR field:(m OR n OR o OR p OR q OR r OR s OR t OR u OR v OR w OR x OR y OR z)",
			true,
			"",
		},
		{
			"string with leading digit",
			"state:PAYMENT_STATE_FAILED AND content_reference.id:2Ubp5JU6lfqmAWXi0I7fnl1z29y",
			false,
			"(state=PAYMENT_STATE_FAILED AND content_reference.id=2Ubp5JU6lfqmAWXi0I7fnl1z29y)",
		},
		{
			"bool",
			"suspended: true",
			false,
			"suspended=true",
		},
		{
			"multivalue bool",
			"suspended: (true OR false)",
			false,
			"suspended=(true OR false)",
		},
		{
			"3 or more AND in a sequence",
			"a:1 and b:2 and c:3 and d:4 and e:6",
			false,
			"(a=1 AND b=2 AND c=3 AND d=4 AND e=6)",
		},
		{
			"3 or more AND in a sequence + implicit AND",
			"a:1 and b:2 and c:3 and d:4 e:6",
			false,
			"((a=1 AND b=2 AND c=3 AND d=4) AND e=6)",
		},
		{
			"3 or more OR in sequence",
			"a:1 or b:2 or c:3 or d:4 or e:5",
			false,
			"(a=1 OR b=2 OR c=3 OR d=4 OR e=5)",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			n, err := ParseAST(test.input)
			if test.expectedError {
				require.Error(t, err, "expected error, got none")
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expected, n.String())
			}
		})
	}
}

func TestParseSimple(t *testing.T) {
	// All of those should return an error.
	testCases := []struct {
		name  string
		input string
	}{
		{
			"boolean 2",
			"field:value OR second:filter",
		},
		{
			"list of values",
			"field:(x OR y OR z)",
		},
		{
			"nested",
			"field:{nested:x}",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseAST(test.input, DisableComplexExpressions())
			require.Error(t, err)
		})
	}
}
