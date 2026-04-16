//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/sivchari/golden"
)

func newSNSClient(t *testing.T) *sns.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"test", "test", "",
		)),
	)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	return sns.NewFromConfig(cfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String("http://localhost:4566")
	})
}

func TestSNS_CreateAndDeleteTopic(t *testing.T) {
	client := newSNSClient(t)
	ctx := t.Context()
	topicName := "test-topic-create-delete"

	// Create topic.
	createOutput, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		t.Fatal(err)
	}
	golden.New(t, golden.WithIgnoreFields("TopicArn", "ResultMetadata")).Assert(t.Name()+"_create", createOutput)

	// Delete topic.
	_, err = client.DeleteTopic(ctx, &sns.DeleteTopicInput{
		TopicArn: createOutput.TopicArn,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSNS_ListTopics(t *testing.T) {
	client := newSNSClient(t)
	ctx := t.Context()
	topicName := "test-topic-list"

	// Create topic.
	createOutput, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = client.DeleteTopic(context.Background(), &sns.DeleteTopicInput{
			TopicArn: createOutput.TopicArn,
		})
	})

	// List topics.
	listOutput, err := client.ListTopics(ctx, &sns.ListTopicsInput{})
	if err != nil {
		t.Fatal(err)
	}

	found := false

	for _, topic := range listOutput.Topics {
		if topic.TopicArn != nil && *topic.TopicArn == *createOutput.TopicArn {
			found = true

			break
		}
	}

	if !found {
		t.Errorf("topic %s not found in list", *createOutput.TopicArn)
	}
}

func TestSNS_SubscribeAndUnsubscribe(t *testing.T) {
	client := newSNSClient(t)
	ctx := t.Context()
	topicName := "test-topic-subscribe"

	// Create topic.
	createOutput, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = client.DeleteTopic(context.Background(), &sns.DeleteTopicInput{
			TopicArn: createOutput.TopicArn,
		})
	})

	// Subscribe.
	subscribeOutput, err := client.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: createOutput.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:test-queue"),
	})
	if err != nil {
		t.Fatal(err)
	}
	golden.New(t, golden.WithIgnoreFields("SubscriptionArn", "ResultMetadata")).Assert(t.Name()+"_subscribe", subscribeOutput)

	// Unsubscribe.
	_, err = client.Unsubscribe(ctx, &sns.UnsubscribeInput{
		SubscriptionArn: subscribeOutput.SubscriptionArn,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSNS_ListSubscriptions(t *testing.T) {
	client := newSNSClient(t)
	ctx := t.Context()
	topicName := "test-topic-list-subs"

	// Create topic.
	createOutput, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = client.DeleteTopic(context.Background(), &sns.DeleteTopicInput{
			TopicArn: createOutput.TopicArn,
		})
	})

	// Subscribe.
	subscribeOutput, err := client.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: createOutput.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:test-queue"),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = client.Unsubscribe(context.Background(), &sns.UnsubscribeInput{
			SubscriptionArn: subscribeOutput.SubscriptionArn,
		})
	})

	// List subscriptions.
	listOutput, err := client.ListSubscriptions(ctx, &sns.ListSubscriptionsInput{})
	if err != nil {
		t.Fatal(err)
	}

	found := false

	for _, sub := range listOutput.Subscriptions {
		if sub.SubscriptionArn != nil && *sub.SubscriptionArn == *subscribeOutput.SubscriptionArn {
			found = true

			break
		}
	}

	if !found {
		t.Errorf("subscription %s not found in list", *subscribeOutput.SubscriptionArn)
	}
}

func TestSNS_ListSubscriptionsByTopic(t *testing.T) {
	client := newSNSClient(t)
	ctx := t.Context()
	topicName := "test-topic-list-subs-by-topic"

	// Create topic.
	createOutput, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = client.DeleteTopic(context.Background(), &sns.DeleteTopicInput{
			TopicArn: createOutput.TopicArn,
		})
	})

	// Subscribe.
	subscribeOutput, err := client.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: createOutput.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:test-queue"),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = client.Unsubscribe(context.Background(), &sns.UnsubscribeInput{
			SubscriptionArn: subscribeOutput.SubscriptionArn,
		})
	})

	// List subscriptions by topic.
	listOutput, err := client.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
		TopicArn: createOutput.TopicArn,
	})
	if err != nil {
		t.Fatal(err)
	}
	golden.New(t, golden.WithIgnoreFields("SubscriptionArn", "TopicArn", "ResultMetadata")).Assert(t.Name(), listOutput)
}

func TestSNS_Publish(t *testing.T) {
	client := newSNSClient(t)
	ctx := t.Context()
	topicName := "test-topic-publish"
	message := "Hello, SNS!"

	// Create topic.
	createOutput, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = client.DeleteTopic(context.Background(), &sns.DeleteTopicInput{
			TopicArn: createOutput.TopicArn,
		})
	})

	// Publish message.
	publishOutput, err := client.Publish(ctx, &sns.PublishInput{
		TopicArn: createOutput.TopicArn,
		Message:  aws.String(message),
		Subject:  aws.String("Test Subject"),
	})
	if err != nil {
		t.Fatal(err)
	}
	golden.New(t, golden.WithIgnoreFields("MessageId", "SequenceNumber", "ResultMetadata")).Assert(t.Name(), publishOutput)
}

// snsEnvelope mirrors the JSON body SNS publishes into SQS when RawMessageDelivery is off.
type snsEnvelope struct {
	Type              string                          `json:"Type"`
	MessageID         string                          `json:"MessageId"`
	TopicARN          string                          `json:"TopicArn"`
	Subject           string                          `json:"Subject"`
	Message           string                          `json:"Message"`
	Timestamp         string                          `json:"Timestamp"`
	MessageAttributes map[string]snsEnvelopeAttribute `json:"MessageAttributes"`
}

type snsEnvelopeAttribute struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// fanoutFixture creates a topic + queue + subscription and returns their handles along
// with the queue URL and a cleanup closure.
type fanoutFixture struct {
	topicARN string
	queueURL string
	subARN   string
}

func setupSNSToSQSFanout(t *testing.T, topicName, queueName string, subAttrs map[string]string) fanoutFixture {
	t.Helper()

	ctx := t.Context()
	snsClient := newSNSClient(t)
	sqsClient := newSQSClient(t)

	topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(topicName)})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	queue, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	queueARN := "arn:aws:sqs:us-east-1:000000000000:" + queueName

	sub, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn:   topic.TopicArn,
		Protocol:   aws.String("sqs"),
		Endpoint:   aws.String(queueARN),
		Attributes: subAttrs,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	t.Cleanup(func() {
		_, _ = snsClient.Unsubscribe(context.Background(), &sns.UnsubscribeInput{SubscriptionArn: sub.SubscriptionArn})
		_, _ = snsClient.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: topic.TopicArn})
		_, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: queue.QueueUrl})
	})

	return fanoutFixture{
		topicARN: aws.ToString(topic.TopicArn),
		queueURL: aws.ToString(queue.QueueUrl),
		subARN:   aws.ToString(sub.SubscriptionArn),
	}
}

func receiveAll(t *testing.T, queueURL string) []string {
	t.Helper()

	ctx := t.Context()
	sqsClient := newSQSClient(t)

	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     0,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}

	bodies := make([]string, 0, len(out.Messages))
	for _, m := range out.Messages {
		bodies = append(bodies, aws.ToString(m.Body))
	}

	return bodies
}

func TestSNS_PublishToSQS_WithoutFilter(t *testing.T) {
	fix := setupSNSToSQSFanout(t, "fanout-no-filter", "fanout-no-filter-q", nil)

	snsClient := newSNSClient(t)
	if _, err := snsClient.Publish(t.Context(), &sns.PublishInput{
		TopicArn: aws.String(fix.topicARN),
		Message:  aws.String(`{"ping":"hello"}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	bodies := receiveAll(t, fix.queueURL)
	if len(bodies) != 1 {
		t.Fatalf("len(messages) = %d, want 1", len(bodies))
	}

	var env snsEnvelope
	if err := json.Unmarshal([]byte(bodies[0]), &env); err != nil {
		t.Fatalf("body is not a valid SNS envelope: %v\nbody: %s", err, bodies[0])
	}
	if env.Type != "Notification" {
		t.Errorf("Type = %q, want Notification", env.Type)
	}
	if env.Message != `{"ping":"hello"}` {
		t.Errorf("Message = %q", env.Message)
	}
	if env.TopicARN != fix.topicARN {
		t.Errorf("TopicArn = %q, want %q", env.TopicARN, fix.topicARN)
	}
}

func TestSNS_PublishToSQS_FilterMatch(t *testing.T) {
	fix := setupSNSToSQSFanout(t, "fanout-filter-match", "fanout-filter-match-q", map[string]string{
		"FilterPolicy": `{"cluster_id":["dev-local"]}`,
	})

	snsClient := newSNSClient(t)
	if _, err := snsClient.Publish(t.Context(), &sns.PublishInput{
		TopicArn: aws.String(fix.topicARN),
		Message:  aws.String(`{"ping":"match"}`),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"cluster_id": {DataType: aws.String("String"), StringValue: aws.String("dev-local")},
		},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	bodies := receiveAll(t, fix.queueURL)
	if len(bodies) != 1 {
		t.Fatalf("len(messages) = %d, want 1", len(bodies))
	}
}

func TestSNS_PublishToSQS_FilterNoMatch(t *testing.T) {
	fix := setupSNSToSQSFanout(t, "fanout-filter-nomatch", "fanout-filter-nomatch-q", map[string]string{
		"FilterPolicy": `{"cluster_id":["dev-local"]}`,
	})

	snsClient := newSNSClient(t)
	if _, err := snsClient.Publish(t.Context(), &sns.PublishInput{
		TopicArn: aws.String(fix.topicARN),
		Message:  aws.String(`{"ping":"nomatch"}`),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"cluster_id": {DataType: aws.String("String"), StringValue: aws.String("other")},
		},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if bodies := receiveAll(t, fix.queueURL); len(bodies) != 0 {
		t.Errorf("FilterPolicy should reject mismatched attrs, got %d messages", len(bodies))
	}
}

func TestSNS_PublishToSQS_FilterNoAttributes(t *testing.T) {
	fix := setupSNSToSQSFanout(t, "fanout-filter-noattrs", "fanout-filter-noattrs-q", map[string]string{
		"FilterPolicy": `{"cluster_id":["dev-local"]}`,
	})

	snsClient := newSNSClient(t)
	if _, err := snsClient.Publish(t.Context(), &sns.PublishInput{
		TopicArn: aws.String(fix.topicARN),
		Message:  aws.String(`{"ping":"noattrs"}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if bodies := receiveAll(t, fix.queueURL); len(bodies) != 0 {
		t.Errorf("FilterPolicy with missing attrs should reject, got %d messages", len(bodies))
	}
}

func TestSNS_PublishToSQS_EnvelopeFormat(t *testing.T) {
	fix := setupSNSToSQSFanout(t, "fanout-envelope", "fanout-envelope-q", nil)

	snsClient := newSNSClient(t)
	if _, err := snsClient.Publish(t.Context(), &sns.PublishInput{
		TopicArn: aws.String(fix.topicARN),
		Subject:  aws.String("hello-subject"),
		Message:  aws.String(`{"ping":"envelope"}`),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"cluster_id": {DataType: aws.String("String"), StringValue: aws.String("dev-local")},
		},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	bodies := receiveAll(t, fix.queueURL)
	if len(bodies) != 1 {
		t.Fatalf("len(messages) = %d, want 1", len(bodies))
	}

	var env snsEnvelope
	if err := json.Unmarshal([]byte(bodies[0]), &env); err != nil {
		t.Fatalf("envelope not valid JSON: %v\nbody: %s", err, bodies[0])
	}
	if env.Type != "Notification" {
		t.Errorf("Type = %q", env.Type)
	}
	if env.MessageID == "" {
		t.Errorf("MessageId is empty")
	}
	if env.TopicARN != fix.topicARN {
		t.Errorf("TopicArn = %q", env.TopicARN)
	}
	if env.Message != `{"ping":"envelope"}` {
		t.Errorf("Message = %q", env.Message)
	}
	if env.Subject != "hello-subject" {
		t.Errorf("Subject = %q", env.Subject)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", env.Timestamp); err != nil {
		t.Errorf("Timestamp %q is not RFC3339-millis: %v", env.Timestamp, err)
	}

	attr, ok := env.MessageAttributes["cluster_id"]
	if !ok {
		t.Fatalf("cluster_id missing from envelope MessageAttributes")
	}
	if attr.Type != "String" || attr.Value != "dev-local" {
		t.Errorf("cluster_id = %+v", attr)
	}
}

func TestSNS_PublishToSQS_RawDelivery(t *testing.T) {
	fix := setupSNSToSQSFanout(t, "fanout-raw", "fanout-raw-q", map[string]string{
		"RawMessageDelivery": "true",
	})

	snsClient := newSNSClient(t)
	if _, err := snsClient.Publish(t.Context(), &sns.PublishInput{
		TopicArn: aws.String(fix.topicARN),
		Message:  aws.String(`{"ping":"raw"}`),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"cluster_id": {DataType: aws.String("String"), StringValue: aws.String("dev-local")},
		},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ctx := t.Context()
	sqsClient := newSQSClient(t)

	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(fix.queueURL),
		MaxNumberOfMessages:   10,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}

	if len(out.Messages) != 1 {
		t.Fatalf("len(messages) = %d, want 1", len(out.Messages))
	}

	body := aws.ToString(out.Messages[0].Body)
	if body != `{"ping":"raw"}` {
		t.Errorf("raw delivery body = %q, want raw publish body", body)
	}

	attr, ok := out.Messages[0].MessageAttributes["cluster_id"]
	if !ok {
		t.Fatalf("cluster_id missing from SQS message attributes in raw mode")
	}
	if aws.ToString(attr.DataType) != "String" || aws.ToString(attr.StringValue) != "dev-local" {
		t.Errorf("cluster_id = %+v", attr)
	}
}

func TestSNS_CreateTopicIdempotent(t *testing.T) {
	client := newSNSClient(t)
	ctx := t.Context()
	topicName := "test-topic-idempotent"

	// Create topic first time.
	createOutput1, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = client.DeleteTopic(context.Background(), &sns.DeleteTopicInput{
			TopicArn: createOutput1.TopicArn,
		})
	})

	// Create topic second time (should return the same ARN).
	createOutput2, err := client.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	if err != nil {
		t.Fatal(err)
	}

	if *createOutput1.TopicArn != *createOutput2.TopicArn {
		t.Errorf("topic ARN mismatch: first %s, second %s",
			*createOutput1.TopicArn, *createOutput2.TopicArn)
	}
}
