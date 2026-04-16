package sns

import (
	"encoding/json"
	"testing"
	"time"
)

func decodeEnvelope(t *testing.T, body string) map[string]any {
	t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("envelope is not valid JSON: %v\nbody: %s", err, body)
	}

	return decoded
}

func TestBuildSNSEnvelope_Fields(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 16, 19, 3, 26, int(123*time.Millisecond), time.UTC)
	attrs := map[string]MessageAttribute{
		"cluster_id": {DataType: "String", StringValue: "dev-local"},
	}

	body, err := buildSNSEnvelope(
		"arn:aws:sns:us-east-1:000000000000:test-topic",
		"msg-1",
		`{"ping":"match"}`,
		"Test Subject",
		attrs,
		now,
	)
	if err != nil {
		t.Fatalf("buildSNSEnvelope: %v", err)
	}

	decoded := decodeEnvelope(t, body)
	want := map[string]any{
		"Type":      "Notification",
		"MessageId": "msg-1",
		"TopicArn":  "arn:aws:sns:us-east-1:000000000000:test-topic",
		"Message":   `{"ping":"match"}`,
		"Subject":   "Test Subject",
		"Timestamp": "2026-04-16T19:03:26.123Z",
	}

	for k, v := range want {
		if decoded[k] != v {
			t.Errorf("%s = %v, want %v", k, decoded[k], v)
		}
	}
}

func TestBuildSNSEnvelope_MessageAttributes(t *testing.T) {
	t.Parallel()

	attrs := map[string]MessageAttribute{
		"cluster_id": {DataType: "String", StringValue: "dev-local"},
	}

	body, err := buildSNSEnvelope("arn", "id", "msg", "", attrs, time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}

	decoded := decodeEnvelope(t, body)

	ma, ok := decoded["MessageAttributes"].(map[string]any)
	if !ok {
		t.Fatalf("MessageAttributes missing or wrong type: %T", decoded["MessageAttributes"])
	}

	entry, ok := ma["cluster_id"].(map[string]any)
	if !ok {
		t.Fatalf("cluster_id entry missing or wrong type: %T", ma["cluster_id"])
	}

	if entry["Type"] != "String" {
		t.Errorf("cluster_id.Type = %v, want String", entry["Type"])
	}

	if entry["Value"] != "dev-local" {
		t.Errorf("cluster_id.Value = %v, want dev-local", entry["Value"])
	}
}

func TestBuildSNSEnvelope_OmitsEmptyOptionals(t *testing.T) {
	t.Parallel()

	body, err := buildSNSEnvelope(
		"arn:aws:sns:us-east-1:000000000000:t",
		"id",
		"hello",
		"",
		nil,
		time.Unix(0, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}

	decoded := decodeEnvelope(t, body)

	if _, ok := decoded["Subject"]; ok {
		t.Errorf("empty Subject should be omitted, got %v", decoded["Subject"])
	}

	if _, ok := decoded["MessageAttributes"]; ok {
		t.Errorf("nil MessageAttributes should be omitted, got %v", decoded["MessageAttributes"])
	}
}

func TestBuildSNSEnvelope_BinaryAttributeBase64(t *testing.T) {
	t.Parallel()

	attrs := map[string]MessageAttribute{
		"blob": {DataType: "Binary", BinaryValue: []byte{0xde, 0xad, 0xbe, 0xef}},
	}

	body, err := buildSNSEnvelope("arn", "id", "msg", "", attrs, time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}

	decoded := decodeEnvelope(t, body)

	ma, ok := decoded["MessageAttributes"].(map[string]any)
	if !ok {
		t.Fatalf("MessageAttributes missing or wrong type: %T", decoded["MessageAttributes"])
	}

	entry, ok := ma["blob"].(map[string]any)
	if !ok {
		t.Fatalf("blob entry missing or wrong type: %T", ma["blob"])
	}

	if entry["Type"] != "Binary" {
		t.Errorf("Type = %v, want Binary", entry["Type"])
	}

	if entry["Value"] != "3q2+7w==" {
		t.Errorf("Value = %v, want base64 of deadbeef", entry["Value"])
	}
}
