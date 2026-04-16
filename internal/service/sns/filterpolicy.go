package sns

import (
	"encoding/json"
	"slices"
)

// MatchesFilterPolicy reports whether the given message attributes satisfy the
// SNS FilterPolicy encoded as JSON.
//
// An empty policyJSON always matches. A malformed policy returns false so the
// subscription never receives messages it cannot confidently evaluate.
//
// Supported at MessageAttributes scope (the SNS default):
//
//   - Exact-match-in-list for String:
//     {"key": ["v1", "v2"]}  matches if attr is String and value ∈ {v1,v2}.
//   - Exact-match-in-list for String.Array:
//     {"key": ["v1"]} matches if attr is String.Array and any element equals v1.
//   - {"exists": true} matches if the attribute is present.
//   - {"exists": false} matches if the attribute is absent.
//   - {"anything-but": "v"} or {"anything-but": ["v1","v2"]} matches if attr is
//     String and value is NOT in the list.
//
// Richer operators (prefix, suffix, numeric, ip-address, equals-ignore-case)
// are intentionally out of scope; extend here when needed.
func MatchesFilterPolicy(policyJSON string, attrs map[string]MessageAttribute) bool {
	if policyJSON == "" {
		return true
	}

	var policy map[string][]any
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		return false
	}

	for key, rules := range policy {
		if !matchesAttrRules(key, rules, attrs) {
			return false
		}
	}

	return true
}

// matchesAttrRules reports whether rules for a single attribute key match.
// An attribute matches if any of the listed rules matches (OR semantics).
func matchesAttrRules(key string, rules []any, attrs map[string]MessageAttribute) bool {
	attr, present := attrs[key]

	for _, rule := range rules {
		if matchesAttrRule(rule, attr, present) {
			return true
		}
	}

	return false
}

func matchesAttrRule(rule any, attr MessageAttribute, present bool) bool {
	switch v := rule.(type) {
	case string:
		return present && stringAttrEquals(attr, v)
	case map[string]any:
		return matchesOperator(v, attr, present)
	default:
		return false
	}
}

// matchesOperator evaluates operator-form rules such as {"exists":true} or
// {"anything-but":"..."}. Unknown operators return false (fail-closed).
func matchesOperator(op map[string]any, attr MessageAttribute, present bool) bool {
	for name, arg := range op {
		switch name {
		case "exists":
			want, ok := arg.(bool)
			if !ok {
				return false
			}

			if want != present {
				return false
			}
		case "anything-but":
			if !present {
				return false
			}

			if matchesAnythingBut(arg, attr) {
				continue
			}

			return false
		default:
			return false
		}
	}

	return true
}

func matchesAnythingBut(arg any, attr MessageAttribute) bool {
	switch v := arg.(type) {
	case string:
		return !stringAttrEquals(attr, v)
	case []any:
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return false
			}

			if stringAttrEquals(attr, s) {
				return false
			}
		}

		return true
	default:
		return false
	}
}

// stringAttrEquals reports whether attr has a String DataType equal to want,
// or a String.Array DataType containing want.
func stringAttrEquals(attr MessageAttribute, want string) bool {
	switch attr.DataType {
	case "String":
		return attr.StringValue == want
	case "String.Array":
		var arr []string
		if err := json.Unmarshal([]byte(attr.StringValue), &arr); err != nil {
			return false
		}

		return slices.Contains(arr, want)
	default:
		return false
	}
}
