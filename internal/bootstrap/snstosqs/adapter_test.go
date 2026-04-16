package snstosqs

import (
	"context"
	"testing"

	"github.com/sivchari/kumo/internal/service/sns"
	"github.com/sivchari/kumo/internal/service/sqs"
)

func TestAdapter_PublishToSQS_ResolvesARN(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	storage := sqs.NewMemoryStorage("http://localhost:4566")

	if _, err := storage.CreateQueue(ctx, "test-queue", nil, nil); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	adapter := New(storage)

	err := adapter.PublishToSQS(
		ctx,
		"arn:aws:sqs:us-east-1:000000000000:test-queue",
		`{"hello":"world"}`,
		nil,
		false,
	)
	if err != nil {
		t.Fatalf("PublishToSQS: %v", err)
	}

	url, err := storage.GetQueueURL(ctx, "test-queue")
	if err != nil {
		t.Fatalf("GetQueueURL: %v", err)
	}

	msgs, err := storage.ReceiveMessage(ctx, url, 10, 30, 0)
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}

	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}

	if msgs[0].Body != `{"hello":"world"}` {
		t.Errorf("Body = %q", msgs[0].Body)
	}
}

func TestAdapter_PublishToSQS_RawPropagatesAttributes(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	storage := sqs.NewMemoryStorage("http://localhost:4566")

	if _, err := storage.CreateQueue(ctx, "raw-queue", nil, nil); err != nil {
		t.Fatal(err)
	}

	adapter := New(storage)
	attrs := map[string]sns.MessageAttribute{
		"cluster_id": {DataType: "String", StringValue: "dev-local"},
	}

	if err := adapter.PublishToSQS(ctx, "arn:aws:sqs:us-east-1:000000000000:raw-queue", "raw", attrs, true); err != nil {
		t.Fatalf("PublishToSQS: %v", err)
	}

	url, _ := storage.GetQueueURL(ctx, "raw-queue")
	msgs, _ := storage.ReceiveMessage(ctx, url, 10, 30, 0)

	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d", len(msgs))
	}

	if msgs[0].Body != "raw" {
		t.Errorf("Body = %q, want raw", msgs[0].Body)
	}

	attr, ok := msgs[0].MessageAttributes["cluster_id"]
	if !ok {
		t.Fatalf("cluster_id missing from SQS MessageAttributes")
	}

	if attr.DataType != "String" || attr.StringValue != "dev-local" {
		t.Errorf("cluster_id = %+v", attr)
	}
}

func TestAdapter_PublishToSQS_NonRawDropsSQSAttributes(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	storage := sqs.NewMemoryStorage("http://localhost:4566")

	if _, err := storage.CreateQueue(ctx, "env-queue", nil, nil); err != nil {
		t.Fatal(err)
	}

	adapter := New(storage)
	attrs := map[string]sns.MessageAttribute{
		"cluster_id": {DataType: "String", StringValue: "dev-local"},
	}

	if err := adapter.PublishToSQS(ctx, "arn:aws:sqs:us-east-1:000000000000:env-queue", "envelope-body", attrs, false); err != nil {
		t.Fatalf("PublishToSQS: %v", err)
	}

	url, _ := storage.GetQueueURL(ctx, "env-queue")
	msgs, _ := storage.ReceiveMessage(ctx, url, 10, 30, 0)

	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d", len(msgs))
	}

	if len(msgs[0].MessageAttributes) != 0 {
		t.Errorf("non-raw delivery should not set SQS message attrs, got %v", msgs[0].MessageAttributes)
	}
}

func TestAdapter_PublishToSQS_MissingQueueReturnsError(t *testing.T) {
	t.Parallel()

	storage := sqs.NewMemoryStorage("http://localhost:4566")
	adapter := New(storage)

	err := adapter.PublishToSQS(context.Background(), "arn:aws:sqs:us-east-1:000000000000:missing", "x", nil, false)
	if err == nil {
		t.Fatal("expected error for missing queue, got nil")
	}
}

func TestQueueNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "valid ARN", input: "arn:aws:sqs:us-east-1:000000000000:my-queue", want: "my-queue"},
		{name: "plain name", input: "my-queue", want: "my-queue"},
		{name: "empty", input: "", wantErr: true},
		{name: "wrong service", input: "arn:aws:sns:us-east-1:000000000000:my-queue", wantErr: true},
		{name: "too few parts", input: "arn:aws:sqs:us-east-1:my-queue", wantErr: true},
		{name: "missing name", input: "arn:aws:sqs:us-east-1:000000000000:", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := queueNameFromARN(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("want error, got name=%q", got)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
