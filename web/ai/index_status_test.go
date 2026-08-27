package ai

import (
	"testing"
	"time"

	"github.com/cozy/cozy-stack/model/instance/lifecycle"
	"github.com/cozy/cozy-stack/model/rag"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/config/config"
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

		ts := time.Now().UTC().Truncate(time.Second)
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "success",
			"timestamp": ts.Format(time.RFC3339Nano),
		}).Status(204)

		updated, err := fs.FileByID(doc.DocID)
		require.NoError(t, err)
		require.NotNil(t, updated.CozyMetadata.RAG)
		require.True(t, updated.CozyMetadata.RAG.Indexed)
		require.Equal(t, rag.RAGStatusSuccess, updated.CozyMetadata.RAG.Status)
		require.NotNil(t, updated.CozyMetadata.RAG.LastSuccessDate)
		require.Nil(t, updated.CozyMetadata.RAG.LastErrorDate)
	})

	t.Run("error status sets Status=error and preserves Indexed", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-error.txt")
		defer destroyStatusTestFile(t, fs, doc)

		ts := time.Now().UTC().Truncate(time.Second)
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "error",
			"timestamp": ts.Format(time.RFC3339Nano),
		}).Status(204)

		updated, err := fs.FileByID(doc.DocID)
		require.NoError(t, err)
		require.NotNil(t, updated.CozyMetadata.RAG)
		require.False(t, updated.CozyMetadata.RAG.Indexed)
		require.Equal(t, rag.RAGStatusError, updated.CozyMetadata.RAG.Status)
		require.Nil(t, updated.CozyMetadata.RAG.LastSuccessDate)
		require.NotNil(t, updated.CozyMetadata.RAG.LastErrorDate)
	})

	t.Run("notsupported status sets Status=notsupported without touching Indexed or dates", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-notsupported.txt")
		defer destroyStatusTestFile(t, fs, doc)

		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "notsupported",
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		}).Status(204)

		updated, err := fs.FileByID(doc.DocID)
		require.NoError(t, err)
		require.NotNil(t, updated.CozyMetadata.RAG)
		require.Equal(t, rag.RAGStatusNotSupported, updated.CozyMetadata.RAG.Status)
		require.False(t, updated.CozyMetadata.RAG.Indexed)
		require.Nil(t, updated.CozyMetadata.RAG.LastSuccessDate)
		require.Nil(t, updated.CozyMetadata.RAG.LastErrorDate)
	})

	// The helpers of the two guards below are unit tested in
	// model/rag/status_test.go; only these cases check that SetRAGStatus
	// actually applies them before writing.

	t.Run("a callback about an outdated content version is dropped", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-outdated.txt")
		defer destroyStatusTestFile(t, fs, doc)

		// A version that no longer matches the file means the content changed
		// since: the file must not be claimed as indexed.
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "success",
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"metadata":  map[string]interface{}{"version": "deadbeefdeadbeefdeadbeefdeadbeef"},
		}).Status(204)

		updated, err := fs.FileByID(doc.DocID)
		require.NoError(t, err)
		if updated.CozyMetadata != nil {
			require.Nil(t, updated.CozyMetadata.RAG)
		}
	})

	t.Run("a callback older than the stored status is dropped", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-out-of-order.txt")
		defer destroyStatusTestFile(t, fs, doc)

		now := time.Now().UTC()
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "success",
			"timestamp": now.Format(time.RFC3339Nano),
		}).Status(204)

		// An error callback emitted before the success above, but delivered
		// after it: it must not overwrite the more recent status.
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "error",
			"timestamp": now.Add(-time.Hour).Format(time.RFC3339Nano),
		}).Status(204)

		updated, err := fs.FileByID(doc.DocID)
		require.NoError(t, err)
		require.NotNil(t, updated.CozyMetadata.RAG)
		require.Equal(t, rag.RAGStatusSuccess, updated.CozyMetadata.RAG.Status)
		require.True(t, updated.CozyMetadata.RAG.Indexed)
		require.Nil(t, updated.CozyMetadata.RAG.LastErrorDate)
	})

	t.Run("unknown status returns a 400", func(t *testing.T) {
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   "any-file-id",
			"status":    "weird",
		}).Status(400)
	})

	t.Run("a partition from another instance returns a 400", func(t *testing.T) {
		postStatus(t, map[string]interface{}{
			"partition": "somewhere-else.cozy.example",
			"file_id":   "any-file-id",
			"status":    "success",
		}).Status(400)
	})

	t.Run("missing file_id returns a 400", func(t *testing.T) {
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"status":    "success",
		}).Status(400)
	})

	t.Run("missing timestamp falls back to now", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-no-ts.txt")
		defer destroyStatusTestFile(t, fs, doc)

		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "success",
		}).Status(204)

		updated, err := fs.FileByID(doc.DocID)
		require.NoError(t, err)
		require.NotNil(t, updated.CozyMetadata.RAG.LastSuccessDate)
	})

	t.Run("malformed timestamp falls back to now", func(t *testing.T) {
		fs := inst.VFS()
		doc := createStatusTestFile(t, fs, "rag-bad-ts.txt")
		defer destroyStatusTestFile(t, fs, doc)

		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   doc.DocID,
			"status":    "success",
			"timestamp": "not-a-date",
		}).Status(204)

		updated, err := fs.FileByID(doc.DocID)
		require.NoError(t, err)
		require.NotNil(t, updated.CozyMetadata.RAG.LastSuccessDate)
	})

	t.Run("non-existent file_id is a no-op", func(t *testing.T) {
		postStatus(t, map[string]interface{}{
			"partition": inst.Domain,
			"file_id":   "non-existent-file-id",
			"status":    "success",
		}).Status(204)
	})
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
