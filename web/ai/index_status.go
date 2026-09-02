package ai

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/cozy/cozy-stack/model/rag"
	"github.com/cozy/cozy-stack/pkg/jsonapi"
	"github.com/cozy/cozy-stack/web/middlewares"
	"github.com/labstack/echo/v4"
)

// statusMessage is the payload posted by the RAG indexer on the callback URL.
type statusMessage struct {
	Partition string `json:"partition"`
	DocID     string `json:"file_id"`
	Status    string `json:"status"` // "success" | "error" | "notsupported"
	Metadata  struct {
		// The _rev of the io.cozy.files document when it was sent for indexation.
		DocRev string `json:"doc_rev"`
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
	if msg.DocID == "" {
		return badRequest(errors.New("missing file_id in payload"))
	}
	// Callbacks are ordered on the file revision they carry, so one without it
	// cannot be placed and is refused.
	if msg.Metadata.DocRev == "" {
		return badRequest(fmt.Errorf("missing metadata.doc_rev for doc %s", msg.DocID))
	}

	// The partition is the domain the file was sent for indexation from. A
	// mismatch means the callback was delivered to the wrong instance, where
	// the same file ID designates another file entirely.
	if msg.Partition != "" && msg.Partition != inst.Domain {
		return badRequest(fmt.Errorf("partition %q does not match this instance", msg.Partition))
	}

	switch msg.Status {
	case rag.StatusSuccess, rag.StatusError, rag.StatusNotSupported:
	default:
		return badRequest(fmt.Errorf("unknown status %q for doc %s", msg.Status, msg.DocID))
	}

	log.Debugf("index status: doc %s status=%s", msg.DocID, msg.Status)

	if err := rag.SetIndexStatus(inst, msg.DocID, msg.Status, msg.Metadata.DocRev); err != nil {
		// The indexer does not replay a failed callback: this status is lost.
		log.Errorf("index status: cannot save status=%s for doc %s: %s", msg.Status, msg.DocID, err)
		return jsonapi.InternalServerError(err)
	}
	return c.NoContent(http.StatusNoContent)
}
