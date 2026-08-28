package rag

import (
	"errors"
	"os"
	"time"

	"github.com/cozy/cozy-stack/model/instance"
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

func SetIndexStatus(inst *instance.Instance, fileID, newStatus, rev string, timestamp time.Time) error {
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	log := inst.Logger().WithNamespace("rag")

	// A missing file is a no-op: it may have been deleted before its callback.
	if _, err := inst.VFS().FileByID(fileID); err != nil {
		if couchdb.IsNotFoundError(err) || errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	doc := NewIndexStatus(fileID)
	err := couchdb.GetDoc(inst, consts.ChatRAG, fileID, doc)
	switch {
	case err == nil:
		// Dropped whole: such a callback must not claim the file is indexed, as
		// a more recent status (e.g. a delete) may say otherwise.
		if isOutdated(rev, doc.FileRev) {
			log.Debugf("SetIndexStatus: dropping status=%s on %s (outdated file revision)", newStatus, fileID)
			return nil
		}
	case couchdb.IsNotFoundError(err) || couchdb.IsNoDatabaseError(err):
		doc = NewIndexStatus(fileID)
	default:
		return err
	}

	// Kept so that a client can tell whether the current revision of the file
	// is the one this status describes.
	doc.FileRev = rev
	applyStatus(doc, newStatus, timestamp)
	return couchdb.Upsert(inst, doc)
}

// isOutdated reports whether a callback about the rev revision of a file is not
// newer than the stored one.
func isOutdated(rev, storedRev string) bool {
	if rev == "" || storedRev == "" {
		return false
	}
	return revision.Generation(rev) <= revision.Generation(storedRev)
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
