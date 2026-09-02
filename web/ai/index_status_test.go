package ai

import (
	"strings"
	"testing"
	"time"

	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/model/instance/lifecycle"
	"github.com/cozy/cozy-stack/model/rag"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/tests/testutils"
	"github.com/cozy/cozy-stack/web/errors"
	"github.com/gavv/httpexpect/v2"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestIndexStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("an instance is required for this test: test skipped due to the use of --short flag")
	}

	config.UseTestFile(t)
	testutils.NeedCouchdb(t)
	setup := testutils.NewSetup(t, t.Name())
	inst := setup.GetTestInstance(&lifecycle.Options{})

	ts := setup.GetTestServer("/ai", Routes)
	ts.Config.Handler.(*echo.Echo).HTTPErrorHandler = errors.ErrorHandler
	t.Cleanup(ts.Close)

	postStatus := func(t *testing.T, payload map[string]interface{}) *httpexpect.Response {
		t.Helper()
		e := testutils.CreateTestClient(t, ts.URL)
		// Built from the constant, so a drift between the route and the path
		// given to the indexer as callback_url fails this test.
		return e.POST(rag.IndexStatusPath).
			WithHeader("Content-Type", "application/json").
			WithJSON(payload).
			Expect()
	}

	t.Run("success status sets Status=success and Indexed=true", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-success.txt")
		defer destroyStatusTestFile(t, fs, doc)

		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "success",
			"metadata":  map[string]interface{}{"doc_rev": doc.Rev()},
		}).Status(204)

		status := getStatus(t, inst, doc.DocID)
		require.True(t, status.Indexed)
		require.Equal(t, rag.StatusSuccess, status.Status)
		require.NotNil(t, status.LastSuccessDate)
		require.Nil(t, status.LastErrorDate)
	})

	t.Run("the status document relates to the file", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-rel.txt")
		defer destroyStatusTestFile(t, fs, doc)

		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "success",
			"metadata":  map[string]interface{}{"doc_rev": doc.Rev()},
		}).Status(204)

		status := getStatus(t, inst, doc.DocID)
		require.Equal(t, doc.DocID, status.ID())
		rel, ok := status.Rels["doc"]
		require.True(t, ok)
		data, ok := rel.Data.(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, doc.DocID, data["_id"])
		require.Equal(t, consts.Files, data["_type"])
	})

	t.Run("error status sets Status=error and preserves Indexed", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-error.txt")
		defer destroyStatusTestFile(t, fs, doc)

		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "error",
			"metadata":  map[string]interface{}{"doc_rev": doc.Rev()},
		}).Status(204)

		status := getStatus(t, inst, doc.DocID)
		require.False(t, status.Indexed)
		require.Equal(t, rag.StatusError, status.Status)
		require.Nil(t, status.LastSuccessDate)
		require.NotNil(t, status.LastErrorDate)
	})

	t.Run("notsupported status leaves Indexed and the dates alone", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-notsupported.txt")
		defer destroyStatusTestFile(t, fs, doc)

		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "success",
			"metadata":  map[string]interface{}{"doc_rev": "3-" + strings.Repeat("a", 32)},
		}).Status(204)

		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "notsupported",
			"metadata":  map[string]interface{}{"doc_rev": "4-" + strings.Repeat("b", 32)},
		}).Status(204)

		status := getStatus(t, inst, doc.DocID)
		require.Equal(t, rag.StatusNotSupported, status.Status)
		require.True(t, status.Indexed)
		require.Nil(t, status.LastErrorDate)
	})

	// isOutdated is unit tested in model/rag/status_test.go; this case checks
	// that SetIndexStatus actually applies it before writing.
	t.Run("a callback about an outdated file revision is dropped", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-outdated.txt")
		defer destroyStatusTestFile(t, fs, doc)

		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "success",
			"metadata":  map[string]interface{}{"doc_rev": "5-" + strings.Repeat("a", 32)},
		}).Status(204)

		// A revision that is not newer than the stored one means the callback
		// describes a version the status has moved past: the file must not be
		// claimed as indexed.
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "error",
			"metadata":  map[string]interface{}{"doc_rev": "3-" + strings.Repeat("b", 32)},
		}).Status(204)

		status := getStatus(t, inst, doc.DocID)
		require.Equal(t, rag.StatusSuccess, status.Status)
		require.True(t, status.Indexed)
		require.Nil(t, status.LastErrorDate)
	})

	t.Run("the file revision is stored on the status document", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-filerev.txt")
		defer destroyStatusTestFile(t, fs, doc)

		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "success",
			"metadata":  map[string]interface{}{"doc_rev": doc.Rev()},
		}).Status(204)

		require.Equal(t, doc.Rev(), getStatus(t, inst, doc.DocID).DocRev)
	})

	t.Run("a callback without a revision returns a 400", func(t *testing.T) {
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   "any-file-id",
			"status":    "success",
		}).Status(400)
	})

	t.Run("unknown status returns a 400", func(t *testing.T) {
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   "any-file-id",
			"status":    "weird",
			"metadata":  map[string]interface{}{"doc_rev": "1-abc"},
		}).Status(400)
	})

	t.Run("a partition from another instance returns a 400", func(t *testing.T) {
		postStatus(t, map[string]interface{}{
			"partition": "somewhere-else.cozy.example",
			"file_id":   "any-file-id",
			"status":    "success",
			"metadata":  map[string]interface{}{"doc_rev": "1-abc"},
		}).Status(400)
	})

	t.Run("missing file_id returns a 400", func(t *testing.T) {
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"status":    "success",
			"metadata":  map[string]interface{}{"doc_rev": "1-abc"},
		}).Status(400)
	})

	t.Run("non-existent file_id is a no-op", func(t *testing.T) {
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   "non-existent-file-id",
			"status":    "success",
			"metadata":  map[string]interface{}{"doc_rev": "1-abc"},
		}).Status(204)
	})
}

func getStatus(t *testing.T, inst *instance.Instance, docID string) *rag.IndexStatus {
	t.Helper()
	var status rag.IndexStatus
	require.NoError(t, couchdb.GetDoc(inst, consts.ChatRAG, docID, &status))
	return &status
}

func createStatusTestFile(t *testing.T, fs vfs.VFS, name string) *vfs.FileDoc {
	t.Helper()
	parent, err := fs.DirByPath("/")
	require.NoError(t, err)
	doc, err := vfs.NewFileDoc(name, parent.DocID, 4, nil, "text/plain", "text", time.Now(), false, false, false, nil)
	require.NoError(t, err)
	f, err := fs.CreateFile(doc, nil)
	require.NoError(t, err)
	_, err = f.Write([]byte("test"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	updated, err := fs.FileByID(doc.DocID)
	require.NoError(t, err)
	return updated
}

func destroyStatusTestFile(t *testing.T, fs vfs.VFS, doc *vfs.FileDoc) {
	t.Helper()
	_ = fs.DestroyFile(doc)
}
