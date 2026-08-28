package rag

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestApplyStatus(t *testing.T) {
	t1 := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	t.Run("success sets Indexed=true and LastSuccessDate", func(t *testing.T) {
		doc := NewIndexStatus("a1b2c3")
		applyStatus(doc, StatusSuccess, t1)
		assert.True(t, doc.Indexed)
		assert.Equal(t, StatusSuccess, doc.Status)
		assert.Equal(t, t1, *doc.LastSuccessDate)
		assert.Nil(t, doc.LastErrorDate)
	})

	t.Run("error without prior success keeps Indexed=false", func(t *testing.T) {
		doc := NewIndexStatus("a1b2c3")
		applyStatus(doc, StatusError, t1)
		assert.False(t, doc.Indexed)
		assert.Equal(t, StatusError, doc.Status)
		assert.Equal(t, t1, *doc.LastErrorDate)
		assert.Nil(t, doc.LastSuccessDate)
	})

	t.Run("error after success preserves Indexed=true", func(t *testing.T) {
		doc := NewIndexStatus("a1b2c3")
		doc.Indexed = true
		applyStatus(doc, StatusError, t1)
		assert.True(t, doc.Indexed)
		assert.Equal(t, StatusError, doc.Status)
		assert.Equal(t, t1, *doc.LastErrorDate)
	})

	t.Run("notsupported only sets Status", func(t *testing.T) {
		doc := NewIndexStatus("a1b2c3")
		doc.Indexed = true
		applyStatus(doc, StatusNotSupported, t1)
		assert.True(t, doc.Indexed)
		assert.Equal(t, StatusNotSupported, doc.Status)
		assert.Nil(t, doc.LastSuccessDate)
		assert.Nil(t, doc.LastErrorDate)
	})
}

func TestIsOutdated(t *testing.T) {
	assert.True(t, isOutdated("2-abc", "3-def"))  // an older revision
	assert.True(t, isOutdated("3-abc", "3-def"))  // the revision already stored
	assert.False(t, isOutdated("4-abc", "3-def")) // a newer revision
	assert.False(t, isOutdated("", "3-def"))      // no revision in the callback
	assert.False(t, isOutdated("3-abc", ""))      // nothing stored yet
}
