package sns

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// buildSNSEnvelope serializes an SNS notification envelope for SQS delivery.
// Shape matches the AWS production contract: Type/MessageId/TopicArn/Message/
// Timestamp, with MessageAttributes keyed as {"Type": "<DataType>", "Value": "..."}.
// Binary attributes are base64-encoded as AWS does.
func buildSNSEnvelope(topicARN, messageID, message, subject string, attrs map[string]MessageAttribute, now time.Time) (string, error) {
	env := snsEnvelope{
		Type:             "Notification",
		MessageID:        messageID,
		TopicARN:         topicARN,
		Subject:          subject,
		Message:          message,
		Timestamp:        now.UTC().Format("2006-01-02T15:04:05.000Z"),
		SignatureVersion: "1",
		// Signature/SigningCertURL/UnsubscribeURL are empty placeholders;
		// the emulator does not sign messages and has no unsubscribe endpoint.
	}

	if len(attrs) > 0 {
		env.MessageAttributes = make(map[string]envelopeAttribute, len(attrs))

		for k, v := range attrs {
			value := v.StringValue
			if v.DataType == "Binary" {
				value = base64.StdEncoding.EncodeToString(v.BinaryValue)
			}

			env.MessageAttributes[k] = envelopeAttribute{
				Type:  v.DataType,
				Value: value,
			}
		}
	}

	data, err := json.Marshal(env)
	if err != nil {
		return "", fmt.Errorf("failed to marshal SNS envelope: %w", err)
	}

	return string(data), nil
}
