package rag

import (
	"errors"
	"os"
	"time"

	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/couchdb/revision"
)

const (
	StatusSuccess      = "success"
	StatusError        = "error"
	StatusNotSupported = "notsupported"
)

// IndexStatusPath is the path of the route on which the RAG indexer posts the
// indexation status of a file. It is what inst.PageURL takes to build the
// callback_url given to the indexer.
const IndexStatusPath = "/ai/index/status"

func SetIndexStatus(inst *instance.Instance, docID, newStatus, rev string, timestamp time.Time) error {
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	log := inst.Logger().WithNamespace("rag")

	// couchdb.Upsert overwrites the revision it finds instead of raising a
	// conflict, so every read this decision rests on must be under the lock.
	mu := config.Lock().ReadWrite(inst, "rag/index-status/"+docID)
	if err := mu.Lock(); err != nil {
		return err
	}
	defer mu.Unlock()

	// A missing file is a no-op: it may have been deleted before its callback.
	file, err := inst.VFS().FileByID(docID)
	if err != nil {
		if couchdb.IsNotFoundError(err) || errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	// A trashed file is out of the index: a callback still in flight must not
	// claim it is indexed.
	if file.Trashed {
		return nil
	}

	doc := NewIndexStatus(docID)
	err = couchdb.GetDoc(inst, consts.ChatRAG, docID, doc)
	switch {
	case err == nil:
		if isOutdated(rev, doc.DocRev) {
			log.Debugf("SetIndexStatus: dropping status=%s on %s (outdated revision)", newStatus, docID)
			return nil
		}
	case couchdb.IsNotFoundError(err) || couchdb.IsNoDatabaseError(err):
		doc = NewIndexStatus(docID)
	default:
		return err
	}

	doc.DocRev = rev
	applyStatus(doc, newStatus, timestamp)
	return couchdb.Upsert(inst, doc)
}

// isOutdated reports whether a callback is older than the stored status. Two
// callbacks about the same revision describe the same indexation, so the last
// one is kept.
func isOutdated(rev, storedRev string) bool {
	if rev == "" || storedRev == "" {
		return false
	}
	return revision.Generation(rev) < revision.Generation(storedRev)
}

func applyStatus(doc *IndexStatus, newStatus string, timestamp time.Time) {
	doc.Status = newStatus
	switch newStatus {
	case StatusSuccess:
		doc.Indexed = true
		doc.LastSuccessDate = &timestamp
	case StatusError:
		// Indexed is preserved: stays true if the file was previously indexed.
		doc.LastErrorDate = &timestamp
	}
}
