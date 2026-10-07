package credentials

import (
	"fmt"
	"os"
	"strings"

	ydbCredentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

// FileTokenSource reads a token from a file on each Token() call,
// e.g. for Kubernetes projected volume tokens that rotate automatically
type FileTokenSource struct {
	path      string
	tokenType string
}

func NewFileTokenSource(path, tokenType string) *FileTokenSource {
	return &FileTokenSource{
		path:      path,
		tokenType: tokenType,
	}
}

// This method is called by ydb-go-sdk, refreshing token automatically
func (s *FileTokenSource) Token() (ydbCredentials.Token, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return ydbCredentials.Token{}, fmt.Errorf("failed to read token from %s: %w", s.path, err)
	}
	return ydbCredentials.Token{
		Token:     strings.TrimSpace(string(data)),
		TokenType: s.tokenType,
	}, nil
}

// String returns a string representation of the token source for logging.
func (s *FileTokenSource) String() string {
	return fmt.Sprintf("FileTokenSource{path:%s,type:%s}", s.path, s.tokenType)
}
