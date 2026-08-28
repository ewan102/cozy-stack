package rag

import (
	"time"

	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/jsonapi"
)

// IndexStatus is the RAG indexation status of a file. Its identifier is the
// identifier of the file it describes.
type IndexStatus struct {
	DocID  string `json:"_id,omitempty"`
	DocRev string `json:"_rev,omitempty"`

	Indexed         bool       `json:"indexed"`
	Status          string     `json:"status,omitempty"`
	LastSuccessDate *time.Time `json:"lastSuccessDate,omitempty"`
	LastErrorDate   *time.Time `json:"lastErrorDate,omitempty"`

	// Read it with relData["_id"], not with Relationship.ResourceIdentifier():
	// the latter looks for "id"/"type" and would silently return an empty
	// reference.
	Rels jsonapi.RelationshipMap `json:"relationships,omitempty"`
}

func (s *IndexStatus) ID() string        { return s.DocID }
func (s *IndexStatus) Rev() string       { return s.DocRev }
func (s *IndexStatus) DocType() string   { return consts.ChatRAG }
func (s *IndexStatus) SetID(id string)   { s.DocID = id }
func (s *IndexStatus) SetRev(rev string) { s.DocRev = rev }

func (s *IndexStatus) Clone() couchdb.Doc {
	cloned := *s
	if s.LastSuccessDate != nil {
		at := *s.LastSuccessDate
		cloned.LastSuccessDate = &at
	}
	if s.LastErrorDate != nil {
		at := *s.LastErrorDate
		cloned.LastErrorDate = &at
	}
	if s.Rels != nil {
		cloned.Rels = make(jsonapi.RelationshipMap, len(s.Rels))
		for k, v := range s.Rels {
			cloned.Rels[k] = v
		}
	}
	return &cloned
}

func NewIndexStatus(fileID string) *IndexStatus {
	return &IndexStatus{
		DocID: fileID,
		Rels: jsonapi.RelationshipMap{
			"file": jsonapi.Relationship{
				Data: struct {
					ID   string `json:"_id"`
					Type string `json:"_type"`
				}{ID: fileID, Type: consts.Files},
			},
		},
	}
}
