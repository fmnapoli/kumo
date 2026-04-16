package sns

import "testing"

func stringAttr(v string) MessageAttribute {
	return MessageAttribute{DataType: "String", StringValue: v}
}

type filterCase struct {
	name   string
	policy string
	attrs  map[string]MessageAttribute
	want   bool
}

func runFilterCases(t *testing.T, tests []filterCase) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MatchesFilterPolicy(tt.policy, tt.attrs)
			if got != tt.want {
				t.Errorf("MatchesFilterPolicy(%q, %v) = %v, want %v", tt.policy, tt.attrs, got, tt.want)
			}
		})
	}
}

func TestMatchesFilterPolicy_SingleKey(t *testing.T) {
	t.Parallel()
	runFilterCases(t, []filterCase{
		{
			name:   "empty policy always matches",
			policy: "",
			attrs:  nil,
			want:   true,
		},
		{
			name:   "single value match",
			policy: `{"cluster_id":["dev-local"]}`,
			attrs:  map[string]MessageAttribute{"cluster_id": stringAttr("dev-local")},
			want:   true,
		},
		{
			name:   "value in multi-value list",
			policy: `{"env":["prod","staging","dev"]}`,
			attrs:  map[string]MessageAttribute{"env": stringAttr("staging")},
			want:   true,
		},
		{
			name:   "value not in list",
			policy: `{"cluster_id":["dev-local"]}`,
			attrs:  map[string]MessageAttribute{"cluster_id": stringAttr("other")},
			want:   false,
		},
		{
			name:   "missing key",
			policy: `{"cluster_id":["dev-local"]}`,
			attrs:  map[string]MessageAttribute{"env": stringAttr("dev-local")},
			want:   false,
		},
		{
			name:   "empty attributes with non-empty policy",
			policy: `{"cluster_id":["dev-local"]}`,
			attrs:  nil,
			want:   false,
		},
	})
}

func TestMatchesFilterPolicy_Exists(t *testing.T) {
	t.Parallel()
	runFilterCases(t, []filterCase{
		{
			name:   "exists true with present attr",
			policy: `{"cluster_id":[{"exists":true}]}`,
			attrs:  map[string]MessageAttribute{"cluster_id": stringAttr("any")},
			want:   true,
		},
		{
			name:   "exists true with missing attr",
			policy: `{"cluster_id":[{"exists":true}]}`,
			attrs:  nil,
			want:   false,
		},
		{
			name:   "exists false with missing attr",
			policy: `{"cluster_id":[{"exists":false}]}`,
			attrs:  nil,
			want:   true,
		},
		{
			name:   "exists false with present attr",
			policy: `{"cluster_id":[{"exists":false}]}`,
			attrs:  map[string]MessageAttribute{"cluster_id": stringAttr("any")},
			want:   false,
		},
	})
}

func TestMatchesFilterPolicy_AnythingBut(t *testing.T) {
	t.Parallel()
	runFilterCases(t, []filterCase{
		{
			name:   "anything-but single value, non-matching attr → match",
			policy: `{"cluster_id":[{"anything-but":"prod"}]}`,
			attrs:  map[string]MessageAttribute{"cluster_id": stringAttr("dev")},
			want:   true,
		},
		{
			name:   "anything-but single value, matching attr → no match",
			policy: `{"cluster_id":[{"anything-but":"prod"}]}`,
			attrs:  map[string]MessageAttribute{"cluster_id": stringAttr("prod")},
			want:   false,
		},
		{
			name:   "anything-but list, none match → match",
			policy: `{"env":[{"anything-but":["prod","staging"]}]}`,
			attrs:  map[string]MessageAttribute{"env": stringAttr("dev")},
			want:   true,
		},
		{
			name:   "anything-but list, one matches → no match",
			policy: `{"env":[{"anything-but":["prod","staging"]}]}`,
			attrs:  map[string]MessageAttribute{"env": stringAttr("staging")},
			want:   false,
		},
		{
			name:   "anything-but with missing attr → no match",
			policy: `{"env":[{"anything-but":"prod"}]}`,
			attrs:  nil,
			want:   false,
		},
	})
}

func TestMatchesFilterPolicy_StringArray(t *testing.T) {
	t.Parallel()
	runFilterCases(t, []filterCase{
		{
			name:   "String.Array intersects with policy → match",
			policy: `{"tags":["red"]}`,
			attrs: map[string]MessageAttribute{
				"tags": {DataType: "String.Array", StringValue: `["red","blue"]`},
			},
			want: true,
		},
		{
			name:   "String.Array no intersection → no match",
			policy: `{"tags":["green"]}`,
			attrs: map[string]MessageAttribute{
				"tags": {DataType: "String.Array", StringValue: `["red","blue"]`},
			},
			want: false,
		},
		{
			name:   "String.Array with malformed JSON → no match",
			policy: `{"tags":["red"]}`,
			attrs: map[string]MessageAttribute{
				"tags": {DataType: "String.Array", StringValue: `not-json`},
			},
			want: false,
		},
	})
}

func TestMatchesFilterPolicy_OROfRules(t *testing.T) {
	t.Parallel()
	runFilterCases(t, []filterCase{
		{
			name:   "exact OR operator — exact hits",
			policy: `{"env":["prod",{"anything-but":"dev"}]}`,
			attrs:  map[string]MessageAttribute{"env": stringAttr("prod")},
			want:   true,
		},
		{
			name:   "exact OR operator — operator hits",
			policy: `{"env":["prod",{"anything-but":"dev"}]}`,
			attrs:  map[string]MessageAttribute{"env": stringAttr("staging")},
			want:   true,
		},
		{
			name:   "exact OR operator — neither hits",
			policy: `{"env":["prod",{"anything-but":"dev"}]}`,
			attrs:  map[string]MessageAttribute{"env": stringAttr("dev")},
			want:   false,
		},
	})
}

func TestMatchesFilterPolicy_UnknownOperator(t *testing.T) {
	t.Parallel()
	runFilterCases(t, []filterCase{
		{
			name:   "unknown operator fails closed",
			policy: `{"env":[{"prefix":"dev-"}]}`,
			attrs:  map[string]MessageAttribute{"env": stringAttr("dev-local")},
			want:   false,
		},
	})
}

func TestMatchesFilterPolicy_Edge(t *testing.T) {
	t.Parallel()
	runFilterCases(t, []filterCase{
		{
			name:   "non-string attribute",
			policy: `{"cluster_id":["dev-local"]}`,
			attrs: map[string]MessageAttribute{
				"cluster_id": {DataType: "Number", StringValue: "dev-local"},
			},
			want: false,
		},
		{
			name:   "malformed policy",
			policy: `{"cluster_id":`,
			attrs:  map[string]MessageAttribute{"cluster_id": stringAttr("dev-local")},
			want:   false,
		},
		{
			name:   "multi-key policy all match",
			policy: `{"env":["prod"],"region":["us-east-1"]}`,
			attrs: map[string]MessageAttribute{
				"env":    stringAttr("prod"),
				"region": stringAttr("us-east-1"),
			},
			want: true,
		},
		{
			name:   "multi-key policy one key missing",
			policy: `{"env":["prod"],"region":["us-east-1"]}`,
			attrs:  map[string]MessageAttribute{"env": stringAttr("prod")},
			want:   false,
		},
	})
}
