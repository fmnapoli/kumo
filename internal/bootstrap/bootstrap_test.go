package bootstrap

import (
	"testing"

	"github.com/sivchari/kumo/internal/service"
	"github.com/sivchari/kumo/internal/service/sns"
	"github.com/sivchari/kumo/internal/service/sqs"
)

func TestWire_AttachesSQSPublisher(t *testing.T) {
	t.Parallel()

	reg := service.NewRegistry()
	snsStorage := sns.NewMemoryStorage("")
	sqsStorage := sqs.NewMemoryStorage("http://localhost:4566")

	reg.Register(sns.New(snsStorage))
	reg.Register(sqs.New(sqsStorage, "http://localhost:4566"))

	if snsStorage.SqsPublisher != nil {
		t.Fatal("precondition: SqsPublisher should start nil")
	}

	Wire(reg)

	if snsStorage.SqsPublisher == nil {
		t.Fatal("Wire did not attach SQSPublisher to SNS storage")
	}
}

func TestWire_NoOpWhenSQSMissing(t *testing.T) {
	t.Parallel()

	reg := service.NewRegistry()
	snsStorage := sns.NewMemoryStorage("")
	reg.Register(sns.New(snsStorage))

	Wire(reg)

	if snsStorage.SqsPublisher != nil {
		t.Error("Wire should be a no-op when SQS service is absent")
	}
}

func TestWire_NoOpWhenSNSMissing(t *testing.T) {
	t.Parallel()

	reg := service.NewRegistry()
	reg.Register(sqs.New(sqs.NewMemoryStorage("http://localhost:4566"), "http://localhost:4566"))

	// Should not panic.
	Wire(reg)
}
