package rag

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cozy/cozy-stack/model/feature"
	"github.com/cozy/cozy-stack/model/vfs"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecodeMD5SumFromFileDoc pins the contract decodeMD5Sum relies on: a
// FileDoc serializes its digest the way the changes feed then carries it, and
// decoding that gives back the hexadecimal digest the RAG server is given.
func TestDecodeMD5SumFromFileDoc(t *testing.T) {
	// The md5sum of "test".
	sum := []byte{0x09, 0x8f, 0x6b, 0xcd, 0x46, 0x21, 0xd3, 0x73,
		0xca, 0xde, 0x4e, 0x83, 0x26, 0x27, 0xb4, 0xf6}

	raw, err := json.Marshal(&vfs.FileDoc{MD5Sum: sum})
	require.NoError(t, err)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &doc))

	assert.Equal(t, fmt.Sprintf("%x", sum), decodeMD5Sum(doc["md5sum"]))
}

func TestDecodeMD5Sum(t *testing.T) {
	const (
		hexSum    = "098f6bcd4621d373cade4e832627b4f6"
		base64Sum = "CY9rzUYh03PK3k6DJie09g=="
	)

	// What the changes feed carries: CouchDB serializes the digest bytes in
	// base64.
	assert.Equal(t, hexSum, decodeMD5Sum(base64Sum))

	assert.Equal(t, "", decodeMD5Sum(""))
	assert.Equal(t, "", decodeMD5Sum(nil))
	assert.Equal(t, "", decodeMD5Sum("not a digest"))
	assert.Equal(t, "", decodeMD5Sum("dG9vIHNob3J0"))   // valid base64, 9 bytes
	assert.Equal(t, "", decodeMD5Sum(map[string]int{})) // not even a string
}

func TestIsClassAllowed(t *testing.T) {
	off := &feature.Flags{M: map[string]interface{}{}}
	on := &feature.Flags{M: map[string]interface{}{
		"rag.index.image.enabled": true,
		"rag.index.video.enabled": true,
		"rag.index.audio.enabled": true,
	}}

	// Text-based files do not depend on any flag.
	assert.True(t, isClassAllowed(off, "text"))
	assert.True(t, isClassAllowed(off, ""))

	for _, class := range []string{consts.ImageClass, consts.VideoClass, consts.AudioClass} {
		assert.False(t, isClassAllowed(off, class), class)
		assert.True(t, isClassAllowed(on, class), class)
	}
}
