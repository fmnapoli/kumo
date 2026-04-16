// Package bootstrap wires cross-service integrations that cannot be expressed
// via each service's independent init(), such as SNS → SQS delivery.
//
// Entry points (cmd/kumo/main.go, kumo.go) should call Wire exactly once
// after server.New has registered all services.
package bootstrap

import (
	"github.com/sivchari/kumo/internal/bootstrap/snstosqs"
	"github.com/sivchari/kumo/internal/service"
	"github.com/sivchari/kumo/internal/service/sns"
	"github.com/sivchari/kumo/internal/service/sqs"
)

// Wire connects services that depend on each other for emulator fidelity.
// Missing services are a no-op — this keeps custom/partial builds valid.
func Wire(reg *service.Registry) {
	wireSNSToSQS(reg)
}

func wireSNSToSQS(reg *service.Registry) {
	snsSvc, ok := reg.Get("sns")
	if !ok {
		return
	}

	sqsSvc, ok := reg.Get("sqs")
	if !ok {
		return
	}

	snsConcrete, ok := snsSvc.(*sns.Service)
	if !ok {
		return
	}

	sqsConcrete, ok := sqsSvc.(*sqs.Service)
	if !ok {
		return
	}

	snsStorage, ok := snsConcrete.Storage().(*sns.MemoryStorage)
	if !ok {
		return
	}

	snsStorage.SetSQSPublisher(snstosqs.New(sqsConcrete.Storage()))
}
