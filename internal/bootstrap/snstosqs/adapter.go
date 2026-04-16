// Package snstosqs bridges SNS Publish → SQS SendMessage so that SNS
// subscriptions with protocol=sqs deliver messages to the target queue.
package snstosqs

import (
	"context"
	"fmt"
	"strings"

	"github.com/sivchari/kumo/internal/service/sns"
	"github.com/sivchari/kumo/internal/service/sqs"
)

// Adapter implements sns.SQSPublisher using an sqs.Storage directly,
// avoiding the HTTP layer. It resolves the subscription endpoint ARN to
// the queue URL using the SQS storage.
type Adapter struct {
	storage sqs.Storage
}

// New returns an Adapter backed by the given SQS storage.
func New(storage sqs.Storage) *Adapter {
	return &Adapter{storage: storage}
}

// PublishToSQS implements sns.SQSPublisher.
func (a *Adapter) PublishToSQS(ctx context.Context, queueARN, body string, attrs map[string]sns.MessageAttribute, rawDelivery bool) error {
	queueName, err := queueNameFromARN(queueARN)
	if err != nil {
		return err
	}

	queueURL, err := a.storage.GetQueueURL(ctx, queueName)
	if err != nil {
		return fmt.Errorf("sns→sqs: resolve queue %q: %w", queueName, err)
	}

	var sqsAttrs map[string]sqs.MessageAttributeValue
	if rawDelivery && len(attrs) > 0 {
		sqsAttrs = make(map[string]sqs.MessageAttributeValue, len(attrs))
		for k, v := range attrs {
			sqsAttrs[k] = sqs.MessageAttributeValue{
				DataType:    v.DataType,
				StringValue: v.StringValue,
				BinaryValue: v.BinaryValue,
			}
		}
	}

	if _, err := a.storage.SendMessage(ctx, queueURL, body, 0, sqsAttrs, "", ""); err != nil {
		return fmt.Errorf("sns→sqs: send to queue %q: %w", queueName, err)
	}

	return nil
}

// queueNameFromARN extracts the queue name from an SQS ARN of the form
// arn:aws:sqs:<region>:<account>:<queue-name>. It tolerates a plain queue
// name (treated as the name itself) but rejects anything else.
func queueNameFromARN(arn string) (string, error) {
	if !strings.HasPrefix(arn, "arn:") {
		if arn == "" {
			return "", fmt.Errorf("sns→sqs: empty endpoint")
		}

		return arn, nil
	}

	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[2] != "sqs" {
		return "", fmt.Errorf("sns→sqs: invalid SQS ARN %q", arn)
	}

	name := parts[5]
	if name == "" {
		return "", fmt.Errorf("sns→sqs: ARN %q missing queue name", arn)
	}

	return name, nil
}
