package rag

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApplyStatus(t *testing.T) {
	t.Run("success sets Indexed=true and LastSuccessDate", func(t *testing.T) {
		doc := NewIndexStatus("a1b2c3")
		applyStatus(doc, StatusSuccess)
		assert.True(t, doc.Indexed)
		assert.Equal(t, StatusSuccess, doc.Status)
		assert.NotNil(t, doc.LastSuccessDate)
		assert.Nil(t, doc.LastErrorDate)
	})

	t.Run("error without prior success keeps Indexed=false", func(t *testing.T) {
		doc := NewIndexStatus("a1b2c3")
		applyStatus(doc, StatusError)
		assert.False(t, doc.Indexed)
		assert.Equal(t, StatusError, doc.Status)
		assert.NotNil(t, doc.LastErrorDate)
		assert.Nil(t, doc.LastSuccessDate)
	})

	t.Run("error after success preserves Indexed=true", func(t *testing.T) {
		doc := NewIndexStatus("a1b2c3")
		doc.Indexed = true
		applyStatus(doc, StatusError)
		assert.True(t, doc.Indexed)
		assert.Equal(t, StatusError, doc.Status)
		assert.NotNil(t, doc.LastErrorDate)
	})

	t.Run("notsupported only sets Status", func(t *testing.T) {
		doc := NewIndexStatus("a1b2c3")
		doc.Indexed = true
		applyStatus(doc, StatusNotSupported)
		assert.True(t, doc.Indexed)
		assert.Equal(t, StatusNotSupported, doc.Status)
		assert.Nil(t, doc.LastSuccessDate)
		assert.Nil(t, doc.LastErrorDate)
	})
}

func TestIsOutdated(t *testing.T) {
	assert.True(t, isOutdated("2-abc", "3-def"))  // an older revision
	assert.False(t, isOutdated("3-abc", "3-def")) // the revision already stored
	assert.False(t, isOutdated("4-abc", "3-def")) // a newer revision
	assert.False(t, isOutdated("", "3-def"))      // no revision in the callback
	assert.False(t, isOutdated("3-abc", ""))      // nothing stored yet
}
