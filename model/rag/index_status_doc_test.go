package rag

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIndexStatus(t *testing.T) {
	doc := NewIndexStatus("a1b2c3")

	assert.Equal(t, "a1b2c3", doc.ID())
	assert.Equal(t, consts.ChatRAG, doc.DocType())

	// Apps read the relationship in the "_id"/"_type" format, not the
	// "id"/"type" one used by referenced_by.
	raw, err := json.Marshal(doc)
	require.NoError(t, err)

	var out struct {
		Rels struct {
			File struct {
				Data struct {
					ID   string `json:"_id"`
					Type string `json:"_type"`
				} `json:"data"`
			} `json:"file"`
		} `json:"relationships"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	assert.Equal(t, "a1b2c3", out.Rels.File.Data.ID)
	assert.Equal(t, consts.Files, out.Rels.File.Data.Type)
}

func TestIndexStatusClone(t *testing.T) {
	at := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	doc := NewIndexStatus("a1b2c3")
	doc.Indexed = true
	doc.LastSuccessDate = &at

	cloned := doc.Clone().(*IndexStatus)
	other := at.Add(time.Hour)
	cloned.LastSuccessDate = &other
	cloned.Indexed = false
	delete(cloned.Rels, "file")

	assert.True(t, doc.Indexed)
	assert.Equal(t, at, *doc.LastSuccessDate)
	assert.Contains(t, doc.Rels, "file")
}
