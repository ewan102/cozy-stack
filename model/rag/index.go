package rag

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/cozy/cozy-stack/model/feature"
	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/model/job"
	"github.com/cozy/cozy-stack/model/note"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/config/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/couchdb/revision"
	"github.com/cozy/cozy-stack/pkg/logger"
	"github.com/labstack/echo/v4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// RAGIndexMessage is the payload published to the RAG index broker for each
// file change. The consumer is rag-indexer (producer.py), which expects the
// "cozy-json" format with snake_case JSON keys.
//
// Unlike other broker messages in pkg/rabbitmq/contracts.go that use camelCase,
// snake_case is used here to match the rag-indexer Python contract.
//
// Content is sent in one of two ways:
//   - note_markdown: pre-rendered markdown for io.cozy.notes; the indexer
//     reads it directly without downloading the file.
//   - file_url: rag-indexer downloads the file autonomously from a
//     self-authenticated URL whose secret grants time-limited read access.
type RAGIndexMessage struct {
	Action       string `json:"action"`
	Partition    string `json:"partition"`
	FileID       string `json:"file_id"`
	Doctype      string `json:"doctype"`
	CallbackURL  string `json:"callback_url"`
	RAGBaseURL   string `json:"rag_base_url"`
	RAGAPIKey    string `json:"rag_api_key"`
	MD5Sum       string `json:"md5sum,omitempty"`
	Name         string `json:"name,omitempty"`
	DirID        string `json:"dir_id,omitempty"`
	Datetime     string `json:"datetime,omitempty"`
	ContentType  string `json:"content_type,omitempty"`
	FileURL      string `json:"file_url,omitempty"`
	NoteMarkdown string `json:"note_markdown,omitempty"`
}

// BatchSize is the maximal number of documents manipulated at once by the
// worker.
const BatchSize = 100

// fileURLTTL covers the rag-indexer DLQ retry budget with margin.
const fileURLTTL = 70 * time.Minute

type IndexMessage struct {
	Doctype string `json:"doctype"`
}

func Index(inst *instance.Instance, logger logger.Logger, msg IndexMessage) error {
	if msg.Doctype != consts.Files {
		return errors.New("Only file can be indexed for the moment")
	}

	mu := config.Lock().ReadWrite(inst, "index/"+msg.Doctype)
	if err := mu.Lock(); err != nil {
		return err
	}
	defer mu.Unlock()

	lastSeq, err := getLastSeqNumber(inst, msg.Doctype)
	if err != nil {
		return err
	}
	feed, err := callChangesFeed(inst, msg.Doctype, lastSeq)
	if err != nil {
		return err
	}
	if feed.LastSeq == lastSeq {
		return nil
	}

	flags, err := feature.GetFlags(inst)
	if err != nil {
		return fmt.Errorf("rag: get feature flags: %w", err)
	}

	// Memoizes ancestor-flag resolution per directory ID across the batch.
	isIndexEnabledCache := map[string]bool{}

	var errj error
	for _, change := range feed.Results {
		if err := callRAGIndexer(inst, msg.Doctype, change, flags, isIndexEnabledCache); err != nil {
			logger.Errorf("rag: skipping change for file %s: %s", change.DocID, err)
			errj = errors.Join(errj, err)
		}
	}
	_ = updateLastSequenceNumber(inst, msg.Doctype, feed.LastSeq)

	if feed.Pending > 0 {
		_ = pushJob(inst, msg.Doctype)
	}

	return errj
}

func callRAGIndexer(inst *instance.Instance, doctype string, change couchdb.Change, flags *feature.Flags, isIndexEnabledCache map[string]bool) error {
	if strings.HasPrefix(change.DocID, "_design/") {
		return nil
	}

	ragServer := inst.RAGServer()
	if ragServer.URL == "" {
		return errors.New("no RAG server configured")
	}

	callbackURL, err := cachedWebhookURL(inst)
	if err != nil {
		return fmt.Errorf("rag ensure webhook: %w", err)
	}

	log := inst.Logger().WithNamespace("rag")

	// Folder changed: fan out over its subtree instead of ignoring it.
	if change.Doc.Get("type") == consts.DirType {
		// Domain-namespaced: root/trash dir IDs are identical across instances.
		cacheKey := inst.Domain + "/" + change.DocID
		deleted := change.Deleted || change.Doc.Get("trashed") == true
		if deleted {
			authorizedIndexCache.Delete(cacheKey)
		} else {
			ownMetadata, _ := change.Doc.Get("cozyMetadata").(map[string]interface{})
			newFlag, _ := ownMetadata["authorized_index"].(bool)
			if prev, ok := authorizedIndexCache.Load(cacheKey); ok && prev == newFlag {
				return nil // flag unchanged (e.g. a rename): nothing to catch up on
			}
			authorizedIndexCache.Store(cacheKey, newFlag)
		}
		return reindexFolderSubtree(inst, doctype, change.DocID, flags, deleted, callbackURL, ragServer, log, isIndexEnabledCache)
	}

	if change.Deleted || change.Doc.Get("trashed") == true {
		// No existsInRAG guard: a RAG-server hiccup here must not silently
		// drop a deletion, since there's no later retry for this change.
		return publishDelete(inst, doctype, change.DocID, callbackURL, ragServer, log)
	}

	class, _ := change.Doc.Get("class").(string)
	ownMetadata, _ := change.Doc.Get("cozyMetadata").(map[string]interface{})
	ownFlag, _ := ownMetadata["authorized_index"].(bool)
	dirID, _ := change.Doc.Get("dir_id").(string)

	enabled, err := isIndexEnabled(inst, ownFlag, dirID, isIndexEnabledCache)
	if err != nil {
		return err
	}
	enabled = enabled && isClassAllowed(flags, class)

	if !enabled {
		return cleanUpIfIndexed(inst, doctype, change.DocID, callbackURL, ragServer, log)
	}

	md5sum, err := decodeMD5Sum(change.Doc.Get("md5sum"))
	if err != nil {
		return fmt.Errorf("rag: invalid md5sum for file %s: %w", change.DocID, err)
	}
	name, _ := change.Doc.Get("name").(string)
	mime, _ := change.Doc.Get("mime").(string)
	metadataRaw, _ := change.Doc.Get("metadata").(map[string]interface{})

	// TODO: patch metadata in the vector db on move/rename.
	return upsertIfNeeded(inst, doctype, change.DocID, name, mime, dirID, md5sum, metadataRaw, callbackURL, ragServer, log)
}

// cleanUpIfIndexed removes fileID's RAG entry, if it has one.
func cleanUpIfIndexed(inst *instance.Instance, doctype, fileID, callbackURL string, ragServer config.RAGServer, log logger.Logger) error {
	exists, err := existsInRAG(ragServer, inst.Domain, fileID)
	if err != nil || !exists {
		return err // exists=false -> err is nil, no-op
	}
	return publishDelete(inst, doctype, fileID, callbackURL, ragServer, log)
}

// upsertIfNeeded (re-)publishes fileID unless the RAG server already has it
// indexed at this exact md5sum.
func upsertIfNeeded(inst *instance.Instance, doctype, fileID, name, mime, dirID, md5sum string, metadataRaw map[string]interface{}, callbackURL string, ragServer config.RAGServer, log logger.Logger) error {
	needed, err := needsIndexation(ragServer, inst.Domain, fileID, md5sum)
	if err != nil {
		return err
	}
	if !needed {
		log.Debugf("rag: skip file %s (content unchanged)", fileID)
		return nil
	}
	datetime, _ := metadataRaw["datetime"].(string)
	return publishUpsert(inst, doctype, fileID, name, mime, dirID, datetime, md5sum, metadataRaw, callbackURL, ragServer, log)
}

// ragFanOutConcurrency bounds concurrent RAG-server verification calls during a fan-out.
const ragFanOutConcurrency = 16

// reindexFolderSubtree re-evaluates every file under dirID so a folder's
// AuthorizedIndex toggle takes effect immediately. deleted forces every file
// to the cleanup path regardless of any flag.
func reindexFolderSubtree(inst *instance.Instance, doctype, dirID string, flags *feature.Flags, deleted bool, callbackURL string, ragServer config.RAGServer, log logger.Logger, isIndexEnabledCache map[string]bool) error {
	sem := semaphore.NewWeighted(ragFanOutConcurrency)
	var g errgroup.Group

	// resolved memoizes each directory's ancestor-resolved flag during the walk.
	resolved := map[string]bool{}
	if !deleted {
		root, err := inst.VFS().DirByID(dirID)
		if err != nil {
			return fmt.Errorf("rag: failed to load folder %s: %w", dirID, err)
		}
		parentEnabled, err := isIndexEnabled(inst, false, root.DirID, isIndexEnabledCache)
		if err != nil {
			return err
		}
		resolved[root.DirID] = parentEnabled
	}

	walkErr := vfs.WalkByID(inst.VFS(), dirID, func(name string, dir *vfs.DirDoc, file *vfs.FileDoc, err error) error {
		if err != nil {
			return err
		}
		if dir != nil {
			if !deleted {
				ownFlag := dir.CozyMetadata != nil && dir.CozyMetadata.AuthorizedIndex
				resolved[dir.DocID] = ownFlag || resolved[dir.DirID]
			}
			return nil
		}
		if file == nil {
			return nil
		}

		var enabled bool
		if !deleted {
			ownFlag := file.CozyMetadata != nil && file.CozyMetadata.AuthorizedIndex
			enabled = (ownFlag || resolved[file.DirID]) && isClassAllowed(flags, file.Class)
		}

		if err := sem.Acquire(context.Background(), 1); err != nil {
			return err
		}
		f := file
		g.Go(func() error {
			defer sem.Release(1)
			if deleted {
				return publishDelete(inst, doctype, f.DocID, callbackURL, ragServer, log)
			}
			if !enabled {
				return cleanUpIfIndexed(inst, doctype, f.DocID, callbackURL, ragServer, log)
			}
			md5sum := hex.EncodeToString(f.MD5Sum) // no base64 here, FileDoc already carries the raw bytes
			metadataRaw := map[string]interface{}(f.Metadata)
			return upsertIfNeeded(inst, doctype, f.DocID, f.DocName, f.Mime, f.DirID, md5sum, metadataRaw, callbackURL, ragServer, log)
		})
		return nil
	})

	// Wait even on walk failure, or dispatched goroutines' results leak.
	waitErr := g.Wait()
	if walkErr != nil {
		return errors.Join(walkErr, waitErr)
	}
	return waitErr
}

// decodeMD5Sum converts the base64 changes-feed value into the hex digest
// used everywhere else in the pipeline.
func decodeMD5Sum(v interface{}) (string, error) {
	s, _ := v.(string)
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

// isIndexEnabled reports whether the file's own flag, or an ancestor's, is
// set. cache memoizes the answer per directory ID across a batch.
func isIndexEnabled(inst *instance.Instance, ownFlag bool, dirID string, cache map[string]bool) (result bool, err error) {
	if ownFlag {
		return true, nil
	}
	var chain []string
	defer func() {
		if err == nil {
			for _, id := range chain {
				cache[id] = result
			}
		}
	}()

	fs := inst.VFS()
	var dir *vfs.DirDoc
	for dirID != "" {
		if resolved, ok := cache[dirID]; ok {
			return resolved, nil
		}
		dir, err = fs.DirByID(dirID)
		if err != nil {
			if couchdb.IsNotFoundError(err) || errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			return false, err
		}
		chain = append(chain, dirID)
		if dir.CozyMetadata != nil && dir.CozyMetadata.AuthorizedIndex {
			return true, nil
		}
		dirID = dir.DirID
	}
	return false, nil
}

// existsInRAG reports whether the RAG server already has an entry for
// fileID, regardless of content freshness.
func existsInRAG(ragServer config.RAGServer, domain, fileID string) (bool, error) {
	_, found, err := getRAGFileMetadata(ragServer, domain, fileID)
	return found, err
}

func isClassAllowed(flags *feature.Flags, class string) bool {
	switch class {
	case consts.ImageClass:
		v, _ := flags.M["rag.index.image.enabled"].(bool)
		return v
	case consts.VideoClass:
		v, _ := flags.M["rag.index.video.enabled"].(bool)
		return v
	case consts.AudioClass:
		v, _ := flags.M["rag.index.audio.enabled"].(bool)
		return v
	}
	return true
}

func publishDelete(inst *instance.Instance, doctype, fileID, callbackURL string, ragServer config.RAGServer, log logger.Logger) error {
	log.Debugf("rag: publish delete for file %s", fileID)
	msg := RAGIndexMessage{
		Action:      "delete",
		Partition:   inst.Domain,
		FileID:      fileID,
		Doctype:     doctype,
		CallbackURL: callbackURL,
		RAGBaseURL:  ragServer.URL,
		RAGAPIKey:   ragServer.APIKey,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := broker.Publish(ctx, inst.ContextName, msg); err != nil {
		return fmt.Errorf("rag publish delete: %w", err)
	}
	return nil
}

// needsIndexation checks the RAG server to see if the file content has changed
// since the last indexation. Returns true when the file must be (re-)indexed.
func needsIndexation(ragServer config.RAGServer, domain, fileID, md5sum string) (bool, error) {
	metadata, found, err := getRAGFileMetadata(ragServer, domain, fileID)
	if err != nil {
		return false, err
	}
	if !found {
		return true, nil
	}
	md5sumFromRAG, ok := metadata["md5sum"].(string)
	if !ok {
		return true, nil
	}
	return md5sumFromRAG != md5sum, nil
}

// getRAGFileMetadata is the shared GET used by needsIndexation/existsInRAG.
// found is false on a 404.
func getRAGFileMetadata(ragServer config.RAGServer, domain, fileID string) (map[string]interface{}, bool, error) {
	u, err := url.Parse(ragServer.URL)
	if err != nil {
		return nil, false, err
	}
	u.Path = fmt.Sprintf("/partition/%s/file/%s", domain, fileID)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Add(echo.HeaderAuthorization, "Bearer "+ragServer.APIKey)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case http.StatusOK:
		var response map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
			return nil, false, err
		}
		metadata, _ := response["metadata"].(map[string]interface{})
		return metadata, true, nil
	case http.StatusNotFound:
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("GET status code: %d", res.StatusCode)
	}
}

func publishUpsert(inst *instance.Instance, doctype, fileID, name, mime, dirID, datetime, md5sum string, metadataRaw map[string]interface{}, callbackURL string, ragServer config.RAGServer, log logger.Logger) error {
	msg := RAGIndexMessage{
		Action:      "upsert",
		Partition:   inst.Domain,
		FileID:      fileID,
		Doctype:     doctype,
		MD5Sum:      md5sum,
		Name:        name,
		DirID:       dirID,
		Datetime:    datetime,
		ContentType: mime,
		CallbackURL: callbackURL,
		RAGBaseURL:  ragServer.URL,
		RAGAPIKey:   ragServer.APIKey,
	}

	if mime == consts.NoteMimeType {
		if err := fillNoteContent(fileID, metadataRaw, &msg); err != nil {
			return err
		}
	} else {
		if strings.HasSuffix(name, consts.DocsExtension) {
			// See https://github.com/OpenLLM-France/RAGondin/issues/88
			msg.Name = strings.TrimSuffix(name, consts.DocsExtension) + consts.MarkdownExtension
		}
		if err := fillFileURL(inst, fileID, &msg); err != nil {
			return err
		}
	}

	log.Debugf("rag: publish upsert for file %s (%s)", fileID, msg.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := broker.Publish(ctx, inst.ContextName, msg); err != nil {
		return fmt.Errorf("rag publish upsert: %w", err)
	}
	return nil
}

func fillNoteContent(fileID string, metadataRaw map[string]interface{}, msg *RAGIndexMessage) error {
	schema, _ := metadataRaw["schema"].(map[string]interface{})
	raw, _ := metadataRaw["content"].(map[string]interface{})
	noteDoc := &note.Document{
		DocID:      fileID,
		SchemaSpec: schema,
		RawContent: raw,
	}
	md, err := noteDoc.Markdown(nil)
	if err != nil {
		return err
	}
	msg.NoteMarkdown = string(md)
	// See https://github.com/OpenLLM-France/RAGondin/issues/88
	msg.Name = strings.TrimSuffix(msg.Name, consts.NoteExtension) + consts.MarkdownExtension
	return nil
}

func fillFileURL(inst *instance.Instance, fileID string, msg *RAGIndexMessage) error {
	doc, err := inst.VFS().FileByID(fileID)
	if err != nil {
		return fmt.Errorf("rag: failed to load file %s: %w", fileID, err)
	}
	filePath, err := doc.Path(inst.VFS())
	if err != nil {
		return fmt.Errorf("rag: failed to resolve path for file %s: %w", fileID, err)
	}
	secret, err := vfs.GetStore().AddFileWithTTL(inst, filePath, fileURLTTL)
	if err != nil {
		return fmt.Errorf("rag: failed to add file to VFS store: %w", err)
	}
	msg.FileURL = inst.PageURL("/files/downloads/"+secret+"/"+url.PathEscape(doc.DocName), nil)
	return nil
}

// getLastSeqNumber returns the last sequence number of the previous
// indexation for this doctype.
func getLastSeqNumber(inst *instance.Instance, doctype string) (string, error) {
	result, err := couchdb.GetLocal(inst, doctype, "rag-index")
	if couchdb.IsNotFoundError(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	seq, _ := result["last_seq"].(string)
	return seq, nil
}

// updateLastSequenceNumber updates the last sequence number for this
// indexation if it's superior to the number in CouchDB.
func updateLastSequenceNumber(inst *instance.Instance, doctype, seq string) error {
	result, err := couchdb.GetLocal(inst, doctype, "rag-index")
	if err != nil {
		if !couchdb.IsNotFoundError(err) {
			return err
		}
		result = make(map[string]interface{})
	} else {
		if prev, ok := result["last_seq"].(string); ok {
			if revision.Generation(seq) <= revision.Generation(prev) {
				return nil
			}
		}
	}
	result["last_seq"] = seq
	return couchdb.PutLocal(inst, doctype, "rag-index", result)
}

// callChangesFeed fetches the last changes from the changes feed
// http://docs.couchdb.org/en/stable/api/database/changes.html
func callChangesFeed(inst *instance.Instance, doctype, since string) (*couchdb.ChangesResponse, error) {
	return couchdb.GetChanges(inst, &couchdb.ChangesRequest{
		DocType:     doctype,
		IncludeDocs: true,
		Since:       since,
		Limit:       BatchSize,
	})
}

// pushJob adds a new job to continue on the pending documents in the changes
// feed.
func pushJob(inst *instance.Instance, doctype string) error {
	msg, err := job.NewMessage(&IndexMessage{
		Doctype: doctype,
	})
	if err != nil {
		return err
	}
	_, err = job.System().PushJob(inst, &job.JobRequest{
		WorkerType: "rag-index",
		Message:    msg,
	})
	return err
}

// cachedWebhookURL returns the RAG webhook URL for the instance, calling
// EnsureRAGWebhook only on the first invocation per domain.
func cachedWebhookURL(inst *instance.Instance) (string, error) {
	if v, ok := webhookURLCache.Load(inst.Domain); ok {
		return v.(string), nil
	}
	u, err := EnsureRAGWebhook(inst)
	if err != nil {
		return "", err
	}
	webhookURLCache.Store(inst.Domain, u)
	return u, nil
}

func CleanInstance(inst *instance.Instance) error {
	ragServer := inst.RAGServer()
	if ragServer.URL == "" {
		return nil
	}
	u, err := url.Parse(ragServer.URL)
	if err != nil {
		return err
	}
	u.Path = fmt.Sprintf("/instances/%s", inst.Domain)
	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Add(echo.HeaderAuthorization, "Bearer "+ragServer.APIKey)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 500 {
		return fmt.Errorf("DELETE status code: %d", res.StatusCode)
	}
	return nil
}
