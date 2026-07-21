package rag

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeMD5Sum(t *testing.T) {
	t.Run("decodes the base64 form stored in the changes feed into plain hex", func(t *testing.T) {
		raw := []byte{0x01, 0x02, 0x03, 0xab}
		b64 := "AQIDqw==" // base64.StdEncoding.EncodeToString(raw)
		got, err := decodeMD5Sum(b64)
		require.NoError(t, err)
		assert.Equal(t, hex.EncodeToString(raw), got)
	})

	t.Run("empty value decodes to an empty string, no error", func(t *testing.T) {
		got, err := decodeMD5Sum("")
		require.NoError(t, err)
		assert.Equal(t, "", got)
	})

	t.Run("non-string input yields an empty string, no panic", func(t *testing.T) {
		got, err := decodeMD5Sum(nil)
		require.NoError(t, err)
		assert.Equal(t, "", got)
	})

	t.Run("invalid base64 returns an error", func(t *testing.T) {
		_, err := decodeMD5Sum("not-valid-base64!!")
		assert.Error(t, err)
	})
}
