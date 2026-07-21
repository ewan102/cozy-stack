package rag_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cozy/cozy-stack/model/rag"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/tests/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBroker captures published RAG index messages instead of hitting RabbitMQ.
type fakeBroker struct {
	mu   sync.Mutex
	msgs []rag.RAGIndexMessage
}

func (b *fakeBroker) Publish(_ context.Context, _ string, payload rag.RAGIndexMessage) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.msgs = append(b.msgs, payload)
	return nil
}

func (b *fakeBroker) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.msgs = nil
}

func (b *fakeBroker) messagesFor(fileID string) []rag.RAGIndexMessage {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []rag.RAGIndexMessage
	for _, m := range b.msgs {
		if m.FileID == fileID {
			out = append(out, m)
		}
	}
	return out
}

// newStubRAGServer simulates the RAG server's GET /partition/{domain}/file/{id}
// endpoint. indexed maps a fileID to its md5sum as currently stored on the RAG
// server (guarded by mu); a fileID absent from the map behaves as "never
// indexed" (404).
func newStubRAGServer(t *testing.T, indexed map[string]string, mu *sync.Mutex) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if len(parts) != 4 || parts[0] != "partition" || parts[2] != "file" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fileID := parts[3]
		mu.Lock()
		md5sum, ok := indexed[fileID]
		mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"metadata": map[string]interface{}{"md5sum": md5sum},
		})
	}))
	t.Cleanup(server.Close)
	return server
}

// TestFolderIndexing exercises the whole RAG indexing pipeline end-to-end
// through the public Index() entry point: per-file AuthorizedIndex flag,
// ancestor folder flag resolution (including a contradictory nested flag),
// immediate fan-out reindexing when a populated folder's flag is toggled, and
// de-indexing a file moved out of a flagged folder.
func TestFolderIndexing(t *testing.T) {
	config.UseTestFile(t)
	setup := testutils.NewSetup(t, "rag_folder_indexing_test")
	inst := setup.GetTestInstance()
	fs := inst.VFS()
	log := inst.Logger().WithNamespace("rag")

	fb := &fakeBroker{}
	rag.Init(fb)

	indexed := map[string]string{}
	var mu sync.Mutex
	server := newStubRAGServer(t, indexed, &mu)

	origServers := config.GetConfig().RAGServers
	config.GetConfig().RAGServers = map[string]config.RAGServer{
		config.DefaultInstanceContext: {URL: server.URL, APIKey: "test-key"},
	}
	t.Cleanup(func() { config.GetConfig().RAGServers = origServers })

	runIndex := func(t *testing.T) {
		t.Helper()
		require.NoError(t, rag.Index(inst, log, rag.IndexMessage{Doctype: consts.Files}))
	}

	root, err := fs.DirByPath("/")
	require.NoError(t, err)

	docs, err := vfs.NewDirDocWithParent("docs", root, nil)
	require.NoError(t, err)
	require.NoError(t, fs.CreateDir(docs))

	archive, err := vfs.NewDirDocWithParent("archive", docs, nil)
	require.NoError(t, err)
	archive.CozyMetadata = &vfs.FilesCozyMetadata{AuthorizedIndex: false}
	require.NoError(t, fs.CreateDir(archive))

	other, err := vfs.NewDirDocWithParent("other", root, nil)
	require.NoError(t, err)
	require.NoError(t, fs.CreateDir(other))

	createFile := func(parentID, name, content string) *vfs.FileDoc {
		t.Helper()
		doc, err := vfs.NewFileDoc(name, parentID, int64(len(content)), nil, "text/plain", "text", time.Now(), false, false, false, nil)
		require.NoError(t, err)
		f, err := fs.CreateFile(doc, nil)
		require.NoError(t, err)
		_, err = f.Write([]byte(content))
		require.NoError(t, err)
		require.NoError(t, f.Close())
		updated, err := fs.FileByID(doc.DocID)
		require.NoError(t, err)
		return updated
	}

	fileA := createFile(docs.DocID, "a.txt", "content a")
	fileB := createFile(docs.DocID, "b.txt", "content b")
	// c.txt sits under archive, whose own flag is explicitly false: it must
	// still be indexed once "docs" (its grandparent) is flagged, proving a
	// non-flagged intermediate folder does not veto a further ancestor's flag.
	fileC := createFile(archive.DocID, "c.txt", "content c")

	// 1. Nothing is flagged yet: settle the initial creation events, expect
	// no publish at all for any of the three files.
	runIndex(t)
	assert.Empty(t, fb.messagesFor(fileA.DocID))
	assert.Empty(t, fb.messagesFor(fileB.DocID))
	assert.Empty(t, fb.messagesFor(fileC.DocID))
	fb.reset()

	// 2. Flip "docs" to AuthorizedIndex=true: it already has files inside, so
	// this must trigger an immediate fan-out over the whole subtree.
	newDocs := *docs
	newDocs.CozyMetadata = &vfs.FilesCozyMetadata{AuthorizedIndex: true}
	require.NoError(t, fs.UpdateDirDoc(docs, &newDocs))
	runIndex(t)

	for _, f := range []*vfs.FileDoc{fileA, fileB, fileC} {
		msgs := fb.messagesFor(f.DocID)
		if assert.Len(t, msgs, 1, "expected exactly one publish for %s", f.DocName) {
			assert.Equal(t, "upsert", msgs[0].Action)
		}
	}

	// Simulate the RAG server having completed indexing of these three files,
	// so the next phase's existence checks find them.
	mu.Lock()
	indexed[fileA.DocID] = hex.EncodeToString(fileA.MD5Sum)
	indexed[fileB.DocID] = hex.EncodeToString(fileB.MD5Sum)
	indexed[fileC.DocID] = hex.EncodeToString(fileC.MD5Sum)
	mu.Unlock()
	fb.reset()

	// 3. Re-running Index() with nothing changed must not republish anything
	// (needsIndexation sees the same md5sum already stored).
	runIndex(t)
	assert.Empty(t, fb.messagesFor(fileA.DocID))
	assert.Empty(t, fb.messagesFor(fileB.DocID))
	assert.Empty(t, fb.messagesFor(fileC.DocID))

	// 4. Move c.txt out of the flagged tree entirely (into "other", which
	// carries no flag): expect a delete, via the single-file path.
	movedC := *fileC
	movedC.DirID = other.DocID
	require.NoError(t, fs.UpdateFileDoc(fileC, &movedC))
	runIndex(t)
	if msgs := fb.messagesFor(fileC.DocID); assert.Len(t, msgs, 1) {
		assert.Equal(t, "delete", msgs[0].Action)
	}
	fb.reset()

	// 5. Flip "docs" back to AuthorizedIndex=false: the still-indexed a.txt
	// and b.txt must be deleted via the fan-out.
	revertedDocs := newDocs
	revertedDocs.CozyMetadata = &vfs.FilesCozyMetadata{AuthorizedIndex: false}
	require.NoError(t, fs.UpdateDirDoc(&newDocs, &revertedDocs))
	runIndex(t)
	for _, f := range []*vfs.FileDoc{fileA, fileB} {
		msgs := fb.messagesFor(f.DocID)
		if assert.Len(t, msgs, 1, "expected exactly one publish for %s", f.DocName) {
			assert.Equal(t, "delete", msgs[0].Action)
		}
	}
}
