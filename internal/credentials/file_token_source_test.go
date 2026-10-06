package credentials

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	ydbCredentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

func TestFileTokenSourceRereadsRotatedToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("  first-token\n"), 0600))
	source := NewFileTokenSource(path, JWTTokenType)

	token, err := source.Token()
	require.NoError(t, err)
	require.Equal(t, ydbCredentials.Token{Token: "first-token", TokenType: JWTTokenType}, token)

	// Replace the file to ensure the source does not keep an old file descriptor.
	replacement := path + ".new"
	require.NoError(t, os.WriteFile(replacement, []byte("\trotated-token\r\n"), 0600))
	require.NoError(t, os.Rename(replacement, path))
	token, err = source.Token()
	require.NoError(t, err)
	require.Equal(t, ydbCredentials.Token{Token: "rotated-token", TokenType: JWTTokenType}, token)
	require.NotContains(t, source.String(), "rotated-token")

	require.NoError(t, os.Remove(path))
	token, err = source.Token()
	require.ErrorIs(t, err, os.ErrNotExist)
	require.ErrorContains(t, err, path)
	require.Empty(t, token)
}
