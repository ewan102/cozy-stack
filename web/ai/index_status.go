package ai

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/cozy/cozy-stack/model/rag"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/jsonapi"
	"github.com/cozy/cozy-stack/web/middlewares"
	"github.com/labstack/echo/v4"
)

// statusMessage is the payload posted by the RAG indexer on the callback URL.
type statusMessage struct {
	Partition string `json:"partition"`
	FileID    string `json:"file_id"`
	Status    string `json:"status"`    // "success" | "error" | "notsupported"
	Timestamp string `json:"timestamp"` // RFC3339Nano
	// The "version" field holds the content md5sum, used to drop callbacks
	// about outdated content.
	Metadata struct {
		Version string `json:"version"`
	} `json:"metadata"`
}

// IndexStatus is the callback route used by the RAG indexer to report the
// indexation status of a file. This route has no authentication.
//
// A malformed payload is answered with a 400: the indexer must not replay it.
// A failure on our side is answered with a 500, so that it can be replayed.
func IndexStatus(c echo.Context) error {
	inst := middlewares.GetInstance(c)
	log := inst.Logger().WithNamespace("rag")

	// A rejected callback is never replayed by the indexer, so it is logged
	// here: the error handler only logs on dev releases.
	badRequest := func(err error) error {
		log.Warnf("index status: rejecting callback: %s", err)
		return jsonapi.BadRequest(err)
	}

	var msg statusMessage
	if err := c.Bind(&msg); err != nil {
		return badRequest(err)
	}
	if msg.FileID == "" {
		return badRequest(errors.New("missing file_id in payload"))
	}

	// The partition is the domain the file was sent for indexation from. A
	// mismatch means the callback was delivered to the wrong instance, where
	// the same file ID designates another file entirely.
	if msg.Partition != "" && msg.Partition != inst.Domain {
		return badRequest(fmt.Errorf("partition %q does not match this instance", msg.Partition))
	}

	switch msg.Status {
	case rag.RAGStatusSuccess, rag.RAGStatusError, rag.RAGStatusNotSupported:
	default:
		return badRequest(fmt.Errorf("unknown status %q for file %s", msg.Status, msg.FileID))
	}

	var ts time.Time
	if msg.Timestamp != "" {
		var err error
		ts, err = time.Parse(time.RFC3339Nano, msg.Timestamp)
		if err != nil {
			log.Warnf("index status: invalid timestamp %q for file %s, using now", msg.Timestamp, msg.FileID)
			ts = time.Now()
		}
	} else {
		log.Warnf("index status: missing timestamp for file %s, using now", msg.FileID)
		ts = time.Now()
	}

	log.Debugf("index status: file %s status=%s ts=%s", msg.FileID, msg.Status, ts)

	if err := rag.SetRAGStatus(inst, msg.FileID, msg.Status, msg.Metadata.Version, ts); err != nil {
		if couchdb.IsNotFoundError(err) {
			log.Debugf("index status: file %s not found (possibly deleted), skipping", msg.FileID)
			return c.NoContent(http.StatusNoContent)
		}
		// The indexer does not replay a failed callback: this status is lost.
		log.Errorf("index status: cannot save status=%s for file %s: %s", msg.Status, msg.FileID, err)
		return jsonapi.InternalServerError(err)
	}
	return c.NoContent(http.StatusNoContent)
}
