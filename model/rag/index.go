package rag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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

	var errj error
	for _, change := range feed.Results {
		if err := callRAGIndexer(inst, msg.Doctype, change, flags); err != nil {
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

func callRAGIndexer(inst *instance.Instance, doctype string, change couchdb.Change, flags *feature.Flags) error {
	if strings.HasPrefix(change.DocID, "_design/") {
		return nil
	}
	if change.Doc.Get("type") == consts.DirType {
		return nil
	}

	class, _ := change.Doc.Get("class").(string)
	if !isClassAllowed(flags, class) {
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

	if change.Deleted || change.Doc.Get("trashed") == true {
		return publishDelete(inst, doctype, change.DocID, callbackURL, ragServer, log)
	}

	md5sum := fmt.Sprintf("%x", change.Doc.Get("md5sum"))
	needed, err := needsIndexation(ragServer, inst.Domain, change.DocID, md5sum)
	if err != nil {
		return err
	}
	if !needed {
		// TODO we should patch the metadata in the vector db when a
		// file has been moved/renamed.
		log.Debugf("rag: skip file %s (content unchanged)", change.DocID)
		return nil
	}

	return publishUpsert(inst, doctype, change, md5sum, callbackURL, ragServer, log)
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
	u, err := url.Parse(ragServer.URL)
	if err != nil {
		return false, err
	}
	u.Path = fmt.Sprintf("/partition/%s/file/%s", domain, fileID)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return false, err
	}
	req.Header.Add(echo.HeaderAuthorization, "Bearer "+ragServer.APIKey)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case http.StatusOK:
		var response map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&response); err != nil {
			return false, err
		}
		metadata, ok := response["metadata"].(map[string]interface{})
		if !ok {
			return true, nil
		}
		md5sumFromRAG, ok := metadata["md5sum"].(string)
		if !ok {
			return true, nil
		}
		return md5sumFromRAG != md5sum, nil
	case http.StatusNotFound:
		return true, nil
	default:
		return false, fmt.Errorf("GET status code: %d", res.StatusCode)
	}
}

func publishUpsert(inst *instance.Instance, doctype string, change couchdb.Change, md5sum, callbackURL string, ragServer config.RAGServer, log logger.Logger) error {
	name, _ := change.Doc.Get("name").(string)
	mime, _ := change.Doc.Get("mime").(string)
	dirID, _ := change.Doc.Get("dir_id").(string)
	metadataRaw, _ := change.Doc.Get("metadata").(map[string]interface{})
	datetime, _ := metadataRaw["datetime"].(string)

	msg := RAGIndexMessage{
		Action:      "upsert",
		Partition:   inst.Domain,
		FileID:      change.DocID,
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
		if err := fillNoteContent(inst, change, &msg); err != nil {
			return err
		}
	} else {
		if strings.HasSuffix(name, consts.DocsExtension) {
			// See https://github.com/OpenLLM-France/RAGondin/issues/88
			msg.Name = strings.TrimSuffix(name, consts.DocsExtension) + consts.MarkdownExtension
		}
		if err := fillFileURL(inst, change.DocID, &msg); err != nil {
			return err
		}
	}

	log.Debugf("rag: publish upsert for file %s (%s)", change.DocID, msg.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := broker.Publish(ctx, inst.ContextName, msg); err != nil {
		return fmt.Errorf("rag publish upsert: %w", err)
	}
	return nil
}

func fillNoteContent(inst *instance.Instance, change couchdb.Change, msg *RAGIndexMessage) error {
	metadata, _ := change.Doc.Get("metadata").(map[string]interface{})
	schema, _ := metadata["schema"].(map[string]interface{})
	raw, _ := metadata["content"].(map[string]interface{})
	noteDoc := &note.Document{
		DocID:      change.DocID,
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
