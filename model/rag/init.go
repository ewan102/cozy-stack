package rag

import (
	"context"
	"errors"
	"sync"
)

// Broker publishes RAG index jobs to the message broker. It is implemented by
// model/stack to avoid a direct import of pkg/rabbitmq from model/rag (which
// would create a cycle via model/instance/lifecycle).
type Broker interface {
	Publish(ctx context.Context, contextName string, payload RAGIndexMessage) error
}

// broker is the active Broker. It defaults to a no-op so that code paths
// exercised in tests before Init is called do not panic.
var broker Broker = noopBroker{}

type noopBroker struct{}

func (noopBroker) Publish(_ context.Context, _ string, _ RAGIndexMessage) error {
	return errors.New("rag: broker not configured")
}

// Init registers the message broker that callRAGIndexer uses to dispatch index
// jobs. It must be called once at startup, before any indexing worker runs.
func Init(b Broker) {
	broker = b
}

// webhookURLCache caches the result of EnsureRAGWebhook per instance domain.
// The webhook URL is stable for the lifetime of the trigger, so caching it
// avoids one CouchDB round-trip per file in every indexing batch.
var webhookURLCache sync.Map // map[domain string]string

// authorizedIndexCache tracks the last known AuthorizedIndex flag seen for
// each folder ID, so callRAGIndexer can skip the (potentially expensive)
// subtree fan-out when a folder document changes for a reason unrelated to
// that flag (e.g. a rename). Process-wide and in-memory: a cold cache (after
// a restart, or the first time a folder is seen) always triggers one fan-out,
// which self-corrects the cache and is harmless since it either finds
// nothing to do or brings RAG back in sync.
var authorizedIndexCache sync.Map // map["domain/dirID" string]bool
