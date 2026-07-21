package rag

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedRequest captures one request observed by a test httptest server.
type recordedRequest struct {
	Method string
	Path   string
	Body   []byte
}

// requestRecorder is a small thread-safe helper to record and assert on the
// HTTP requests received by an httptest server.
type requestRecorder struct {
	mu       sync.Mutex
	requests []recordedRequest
}

func (r *requestRecorder) record(req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	req.Body.Close()
	// Restore the body so the test's own handler can still read it.
	req.Body = io.NopCloser(bytes.NewReader(body))
	rr := recordedRequest{Method: req.Method, Path: req.URL.Path, Body: body}
	r.mu.Lock()
	r.requests = append(r.requests, rr)
	r.mu.Unlock()
}

func (r *requestRecorder) all() []recordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedRequest, len(r.requests))
	copy(out, r.requests)
	return out
}

func (r *requestRecorder) countPath(path string) int {
	n := 0
	for _, rr := range r.all() {
		if rr.Path == path {
			n++
		}
	}
	return n
}

func testLogger() logger.Logger {
	return logger.WithNamespace("rag-test")
}

func newRAGTestServer(t *testing.T, handler http.HandlerFunc) (config.RAGServer, *requestRecorder) {
	t.Helper()
	rec := &requestRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rec.record(req)
		handler(w, req)
	}))
	t.Cleanup(srv.Close)
	server := config.RAGServer{URL: srv.URL, APIKey: "test-key"}
	return server, rec
}

func decodeFileIDs(t *testing.T, body []byte) []string {
	t.Helper()
	var payload struct {
		FileIDs []string `json:"file_ids"`
	}
	require.NoError(t, json.Unmarshal(body, &payload))
	return payload.FileIDs
}

// --- ensureWorkspaceHTTP ---------------------------------------------------

func TestEnsureWorkspaceHTTP(t *testing.T) {
	t.Run("workspace already exists: GET 200 short-circuits", func(t *testing.T) {
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			assert.Equal(t, http.MethodGet, req.Method)
			assert.Equal(t, "/partition/example.mycozy.cloud/workspaces/folder1", req.URL.Path)
			w.WriteHeader(http.StatusOK)
		})

		resolve := func() (string, []string, error) { return "My folder", []string{"file1"}, nil }
		err := ensureWorkspaceHTTP(server, "example.mycozy.cloud", "folder1", resolve, testLogger())
		require.NoError(t, err)
		assert.Len(t, rec.all(), 1, "no further calls once the workspace is known to exist")
	})

	t.Run("workspace already exists: resolver is not called (laziness)", func(t *testing.T) {
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		resolverCalled := false
		resolve := func() (string, []string, error) {
			resolverCalled = true
			return "", nil, fmt.Errorf("resolver must not be called on GET 200")
		}
		err := ensureWorkspaceHTTP(server, "example.mycozy.cloud", "folder1", resolve, testLogger())
		require.NoError(t, err)
		assert.False(t, resolverCalled, "resolver must stay lazy on the GET-200 short-circuit")
		assert.Len(t, rec.all(), 1)
	})

	t.Run("workspace missing: creates partition, workspace, and backfills", func(t *testing.T) {
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1":
				w.WriteHeader(http.StatusNotFound)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom":
				w.WriteHeader(http.StatusCreated)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces":
				body, _ := io.ReadAll(req.Body)
				var payload map[string]string
				require.NoError(t, json.Unmarshal(body, &payload))
				assert.Equal(t, "folder1", payload["workspace_id"])
				assert.Equal(t, "My folder", payload["display_name"])
				w.WriteHeader(http.StatusCreated)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				body, _ := io.ReadAll(req.Body)
				assert.Equal(t, []string{"file1", "file2"}, decodeFileIDs(t, body))
				w.WriteHeader(http.StatusOK)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		resolve := func() (string, []string, error) { return "My folder", []string{"file1", "file2"}, nil }
		err := ensureWorkspaceHTTP(server, "dom", "folder1", resolve, testLogger())
		require.NoError(t, err)
		assert.Equal(t, 1, rec.countPath("/partition/dom/workspaces/folder1"))
		assert.Equal(t, 1, rec.countPath("/partition/dom"))
		assert.Equal(t, 1, rec.countPath("/partition/dom/workspaces"))
		assert.Equal(t, 1, rec.countPath("/partition/dom/workspaces/folder1/files"))
	})

	t.Run("workspace create 409 is tolerated and backfill still runs", func(t *testing.T) {
		server, _ := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1":
				w.WriteHeader(http.StatusNotFound)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom":
				w.WriteHeader(http.StatusConflict)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces":
				w.WriteHeader(http.StatusConflict)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				w.WriteHeader(http.StatusOK)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		resolve := func() (string, []string, error) { return "My folder", []string{"file1"}, nil }
		err := ensureWorkspaceHTTP(server, "dom", "folder1", resolve, testLogger())
		require.NoError(t, err)
	})

	t.Run("backfill chunk 404 falls back to per-id retries (CRITICAL fix)", func(t *testing.T) {
		// openRAG's /files endpoint is all-or-nothing: any unknown id in the
		// batch 404s and adds NONE of the ids, even the indexable ones. A
		// single-id post must be attempted for every id in the chunk so
		// indexable files still get attached.
		var mu sync.Mutex
		workspaceExists := false
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1":
				// 404 before creation; 200 for the disambiguation check that
				// follows the chunk 404.
				if workspaceExists {
					w.WriteHeader(http.StatusOK)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom":
				w.WriteHeader(http.StatusCreated)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces":
				workspaceExists = true
				w.WriteHeader(http.StatusCreated)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				body, _ := io.ReadAll(req.Body)
				ids := decodeFileIDs(t, body)
				if len(ids) > 1 {
					// batch contains the non-indexable "image1": reject all.
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if ids[0] == "image1" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.WriteHeader(http.StatusOK)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		resolve := func() (string, []string, error) { return "My folder", []string{"file1", "image1", "file2"}, nil }
		err := ensureWorkspaceHTTP(server, "dom", "folder1", resolve, testLogger())
		require.NoError(t, err)

		var singleIDBodies [][]string
		for _, rr := range rec.all() {
			if rr.Method == http.MethodPost && rr.Path == "/partition/dom/workspaces/folder1/files" {
				ids := decodeFileIDs(t, rr.Body)
				if len(ids) == 1 {
					singleIDBodies = append(singleIDBodies, ids)
				}
			}
		}
		// one retry per id in the rejected chunk
		assert.ElementsMatch(t, [][]string{{"file1"}, {"image1"}, {"file2"}}, singleIDBodies)
	})

	t.Run("backfill 404 with a vanished workspace is an ensure failure", func(t *testing.T) {
		// A chunk 404 normally means "some file id is not indexed" and is
		// retried per id. But when the workspace itself is gone (e.g. deleted
		// concurrently), the ensure must fail so the query does not proceed
		// scoped to a nonexistent workspace.
		var mu sync.Mutex
		created := false
		server, _ := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1":
				// 404 on the initial check AND on the post-chunk-404
				// disambiguation check: the workspace vanished after its
				// creation was acknowledged.
				w.WriteHeader(http.StatusNotFound)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom":
				w.WriteHeader(http.StatusCreated)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces":
				created = true
				w.WriteHeader(http.StatusCreated)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				w.WriteHeader(http.StatusNotFound)
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				// deleteWorkspace's list-before-delete: the workspace is
				// already gone, same as the vanished-workspace scenario.
				w.WriteHeader(http.StatusNotFound)
			case req.Method == http.MethodDelete && req.URL.Path == "/partition/dom/workspaces/folder1":
				// rollback of the failed ensure
				w.WriteHeader(http.StatusNotFound)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		resolve := func() (string, []string, error) { return "My folder", []string{"file1"}, nil }
		err := ensureWorkspaceHTTP(server, "dom", "folder1", resolve, testLogger())
		require.Error(t, err)
		mu.Lock()
		assert.True(t, created)
		mu.Unlock()
	})

	t.Run("backfill 5xx is an ensure failure and rolls the workspace back", func(t *testing.T) {
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1":
				w.WriteHeader(http.StatusNotFound)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom":
				w.WriteHeader(http.StatusCreated)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces":
				w.WriteHeader(http.StatusCreated)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				w.WriteHeader(http.StatusInternalServerError)
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				// deleteWorkspace's list-before-delete: the backfill never
				// got past its first (failing) chunk, so nothing is attached.
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"file_ids": []string{}})
			case req.Method == http.MethodDelete && req.URL.Path == "/partition/dom/workspaces/folder1":
				w.WriteHeader(http.StatusOK)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		resolve := func() (string, []string, error) { return "My folder", []string{"file1"}, nil }
		err := ensureWorkspaceHTTP(server, "dom", "folder1", resolve, testLogger())
		require.Error(t, err)

		var deletes int
		for _, rr := range rec.all() {
			if rr.Method == http.MethodDelete && rr.Path == "/partition/dom/workspaces/folder1" {
				deletes++
			}
		}
		assert.Equal(t, 1, deletes, "the incomplete workspace must be deleted so a later ensure retries the backfill")
	})

	t.Run("failed backfill is retried from scratch on the next ensure", func(t *testing.T) {
		// First ensure: the backfill 5xx-fails, the workspace is rolled back.
		// Second ensure: the GET must see 404 again (not 200) and the whole
		// create+backfill sequence must rerun and succeed.
		var mu sync.Mutex
		workspaceExists := false
		failBackfill := true
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1":
				if workspaceExists {
					w.WriteHeader(http.StatusOK)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom":
				w.WriteHeader(http.StatusCreated)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces":
				workspaceExists = true
				w.WriteHeader(http.StatusCreated)
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				if failBackfill {
					failBackfill = false
					w.WriteHeader(http.StatusInternalServerError)
				} else {
					w.WriteHeader(http.StatusOK)
				}
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				// deleteWorkspace's list-before-delete: the backfill never
				// got past its first (failing) chunk, so nothing is attached.
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"file_ids": []string{}})
			case req.Method == http.MethodDelete && req.URL.Path == "/partition/dom/workspaces/folder1":
				workspaceExists = false
				w.WriteHeader(http.StatusOK)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		resolve := func() (string, []string, error) { return "My folder", []string{"file1"}, nil }
		require.Error(t, ensureWorkspaceHTTP(server, "dom", "folder1", resolve, testLogger()))
		require.NoError(t, ensureWorkspaceHTTP(server, "dom", "folder1", resolve, testLogger()))

		assert.Equal(t, 2, rec.countPath("/partition/dom/workspaces"),
			"the workspace must be re-created by the second ensure")
		var backfillPosts int
		for _, rr := range rec.all() {
			if rr.Method == http.MethodPost && rr.Path == "/partition/dom/workspaces/folder1/files" {
				backfillPosts++
			}
		}
		assert.Equal(t, 2, backfillPosts, "the backfill must rerun after the rollback")
	})
}

// --- deleteWorkspace ---------------------------------------------------

func TestDeleteWorkspace(t *testing.T) {
	t.Run("detaches every file before deleting the now-empty workspace", func(t *testing.T) {
		var detached []string
		var mu sync.Mutex
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"file_ids": []string{"file1", "file2"},
				})
			case req.Method == http.MethodDelete && strings.HasPrefix(req.URL.Path, "/partition/dom/workspaces/folder1/files/"):
				mu.Lock()
				detached = append(detached, strings.TrimPrefix(req.URL.Path, "/partition/dom/workspaces/folder1/files/"))
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
			case req.Method == http.MethodDelete && req.URL.Path == "/partition/dom/workspaces/folder1":
				w.WriteHeader(http.StatusOK)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		err := deleteWorkspace(server, "dom", "folder1", testLogger())
		require.NoError(t, err)

		assert.ElementsMatch(t, []string{"file1", "file2"}, detached,
			"every file must be detached before the workspace itself is deleted")
		assert.Equal(t, 1, rec.countPath("/partition/dom/workspaces/folder1"),
			"the workspace delete must happen exactly once, after the detaches")

		// The workspace delete must be the LAST request: detaching happens
		// strictly before the cascade-risking final delete.
		all := rec.all()
		last := all[len(all)-1]
		assert.Equal(t, http.MethodDelete, last.Method)
		assert.Equal(t, "/partition/dom/workspaces/folder1", last.Path)
	})

	t.Run("empty workspace is deleted directly, no detach calls", func(t *testing.T) {
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"file_ids": []string{}})
			case req.Method == http.MethodDelete && req.URL.Path == "/partition/dom/workspaces/folder1":
				w.WriteHeader(http.StatusOK)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		require.NoError(t, deleteWorkspace(server, "dom", "folder1", testLogger()))
		assert.Len(t, rec.all(), 2, "just the list and the final delete")
	})

	t.Run("a file that fails to detach prevents the workspace delete (never risk the cascade)", func(t *testing.T) {
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"file_ids": []string{"file1", "file2"},
				})
			case req.Method == http.MethodDelete && req.URL.Path == "/partition/dom/workspaces/folder1/files/file1":
				w.WriteHeader(http.StatusInternalServerError)
			case req.Method == http.MethodDelete && req.URL.Path == "/partition/dom/workspaces/folder1/files/file2":
				w.WriteHeader(http.StatusOK)
			case req.Method == http.MethodDelete && req.URL.Path == "/partition/dom/workspaces/folder1":
				t.Fatalf("the workspace must never be deleted when a file failed to detach")
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		err := deleteWorkspace(server, "dom", "folder1", testLogger())
		require.Error(t, err)
		assert.Zero(t, rec.countPath("/partition/dom/workspaces/folder1"))
	})

	t.Run("list failure prevents the workspace delete", func(t *testing.T) {
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				w.WriteHeader(http.StatusInternalServerError)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		err := deleteWorkspace(server, "dom", "folder1", testLogger())
		require.Error(t, err)
		assert.Len(t, rec.all(), 1, "only the failed list call, nothing else")
	})

	t.Run("workspace already gone (404 on both list and final delete) is a no-op success", func(t *testing.T) {
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/workspaces/folder1/files":
				w.WriteHeader(http.StatusNotFound)
			case req.Method == http.MethodDelete && req.URL.Path == "/partition/dom/workspaces/folder1":
				// Nothing to detach, so deleteWorkspace still attempts the
				// final delete; a 404 there is tolerated too.
				w.WriteHeader(http.StatusNotFound)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		require.NoError(t, deleteWorkspace(server, "dom", "folder1", testLogger()))
		assert.Len(t, rec.all(), 2, "the list (404) and the final delete (404, tolerated)")
	})
}

// --- AssistantDeleted / assistantDirIDFromEvent -----------------------------

func TestAssistantDirIDFromEvent(t *testing.T) {
	t.Run("extracts the dirId of a deleted assistant with a knowledge base", func(t *testing.T) {
		event := json.RawMessage(`{
			"verb": "DELETED",
			"doc": {
				"_id": "assistant1",
				"knowledgeBase": [{"doctype": "io.cozy.files", "dirId": "folder1"}]
			}
		}`)
		dirID, err := assistantDirIDFromEvent(event, testLogger())
		require.NoError(t, err)
		assert.Equal(t, "folder1", dirID)
	})

	t.Run("an assistant with no knowledge base yields an empty dirID", func(t *testing.T) {
		event := json.RawMessage(`{"verb": "DELETED", "doc": {"_id": "assistant1"}}`)
		dirID, err := assistantDirIDFromEvent(event, testLogger())
		require.NoError(t, err)
		assert.Equal(t, "", dirID)
	})

	t.Run("malformed event payload returns an error", func(t *testing.T) {
		_, err := assistantDirIDFromEvent(json.RawMessage(`not json`), testLogger())
		require.Error(t, err)
	})
}

// --- reconcileMembershipHTTP -----------------------------------------------

func TestReconcileMembershipHTTP(t *testing.T) {
	t.Run("adds and removes exactly per diff", func(t *testing.T) {
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/files/file1/workspaces":
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"workspace_ids": []string{"ws-remove", "ws-keep"},
				})
			case req.Method == http.MethodPost && req.URL.Path == "/partition/dom/workspaces/ws-add/files":
				body, _ := io.ReadAll(req.Body)
				assert.Equal(t, []string{"file1"}, decodeFileIDs(t, body))
				w.WriteHeader(http.StatusOK)
			case req.Method == http.MethodDelete && req.URL.Path == "/partition/dom/workspaces/ws-remove/files/file1":
				w.WriteHeader(http.StatusOK)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		known := map[string]string{"ws-add": "/a", "ws-keep": "/b", "ws-remove": "/c"}
		reconcileMembershipHTTP(server, "dom", "file1", []string{"ws-keep", "ws-add"}, known, testLogger())

		assert.Equal(t, 1, rec.countPath("/partition/dom/workspaces/ws-add/files"))
		assert.Equal(t, 1, rec.countPath("/partition/dom/workspaces/ws-remove/files/file1"))
		assert.Equal(t, 0, rec.countPath("/partition/dom/workspaces/ws-keep/files"))
	})

	t.Run("foreign workspace in actual membership is never deleted", func(t *testing.T) {
		// The file's actual openRAG membership includes a workspace id the
		// stack does not manage (not in `known`). It must be left alone: no
		// DELETE call for it, even though it is absent from `desired`.
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/partition/dom/files/file1/workspaces":
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"workspace_ids": []string{"ws-keep", "ws-foreign"},
				})
			case req.Method == http.MethodDelete:
				t.Fatalf("unexpected DELETE for foreign workspace: %s", req.URL.Path)
			default:
				t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
			}
		})

		known := map[string]string{"ws-keep": "/b"}
		reconcileMembershipHTTP(server, "dom", "file1", []string{"ws-keep"}, known, testLogger())

		for _, rr := range rec.all() {
			assert.NotEqual(t, http.MethodDelete, rr.Method, "no delete should be issued for a foreign workspace")
		}
	})

	t.Run("GET failure causes no mutations", func(t *testing.T) {
		server, rec := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})

		known := map[string]string{"ws-add": "/a"}
		reconcileMembershipHTTP(server, "dom", "file1", []string{"ws-add"}, known, testLogger())

		assert.Len(t, rec.all(), 1, "only the failed GET, no add/remove calls")
	})
}

func TestNilKBContextIsEmpty(t *testing.T) {
	// loadKBContext returns nil on any error or when no assistant declares a
	// knowledge base: reconcileMembership relies on empty() to gate that.
	var kb *kbContext
	assert.True(t, kb.empty())
}

func TestDirIDEscaping(t *testing.T) {
	// Folder ids are arbitrary CouchDB ids (UUIDs, fixed ids like the Drive
	// root, or anything a client stored): when used as a workspace id they
	// must stay a single URL path segment, whatever characters they contain.
	server, _ := newRAGTestServer(t, func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "/partition/dom/workspaces/kb%2F..%2F1", req.URL.EscapedPath())
		w.WriteHeader(http.StatusOK)
	})

	exists, err := workspaceExists(server, "dom", "kb/../1")
	require.NoError(t, err)
	assert.True(t, exists)
}
