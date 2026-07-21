package rag

import (
	"encoding/json"
	"runtime"
	"time"

	"github.com/cozy/cozy-stack/model/job"
	"github.com/cozy/cozy-stack/model/rag"
)

func init() {
	job.AddWorker(&job.WorkerConfig{
		WorkerType:   "rag-index",
		Concurrency:  runtime.NumCPU(),
		MaxExecCount: 1,
		Reserved:     true,
		Timeout:      15 * time.Minute,
		WorkerFunc:   WorkerIndex,
	})

	job.AddWorker(&job.WorkerConfig{
		WorkerType:   "rag-query",
		Concurrency:  runtime.NumCPU(),
		MaxExecCount: 1,
		Reserved:     true,
		Timeout:      15 * time.Minute,
		WorkerFunc:   WorkerQuery,
	})

	job.AddWorker(&job.WorkerConfig{
		WorkerType:   "rag-workspace-clean",
		Concurrency:  runtime.NumCPU(),
		MaxExecCount: 3,
		Reserved:     true,
		Timeout:      1 * time.Minute,
		WorkerFunc:   WorkerCleanWorkspace,
	})
}

func WorkerIndex(ctx *job.TaskContext) error {
	logger := ctx.Logger()
	var msg rag.IndexMessage
	if err := ctx.UnmarshalMessage(&msg); err != nil {
		return err
	}
	logger.Debugf("RAG: index %s", msg.Doctype)
	return rag.Index(ctx.Instance, logger, msg)
}

func WorkerQuery(ctx *job.TaskContext) error {
	logger := ctx.Logger()
	var msg rag.QueryMessage
	if err := ctx.UnmarshalMessage(&msg); err != nil {
		return err
	}
	logger.Debugf("RAG: query %v", msg)
	return rag.Query(ctx.Instance, logger, msg)
}

// WorkerCleanWorkspace handles the @event trigger on the deletion of an
// io.cozy.ai.chat.assistants document (see docs/ai.md for the trigger
// setup). The event's payload carries the deleted assistant's last known
// content, including its knowledgeBase folder, if any -- interpreting it
// requires model/rag's own (unexported) assistant types, so the raw event
// is passed through as-is rather than re-parsed here.
func WorkerCleanWorkspace(ctx *job.TaskContext) error {
	logger := ctx.Logger()
	var raw json.RawMessage
	if err := ctx.UnmarshalEvent(&raw); err != nil {
		return err
	}
	logger.Debugf("RAG: cleaning up the workspace of a deleted assistant, if any")
	return rag.AssistantDeleted(ctx.Instance, logger, raw)
}
