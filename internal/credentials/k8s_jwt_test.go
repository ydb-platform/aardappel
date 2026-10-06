package credentials

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestK8sJWTCredentialsRefreshWithRotatedToken(t *testing.T) {
	requests := make(chan url.Values, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse token exchange request: %v", err)
			http.Error(w, "invalid form", http.StatusBadRequest)
			return
		}
		requests <- r.PostForm
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "access-for-" + r.PostForm.Get("actor_token"),
			"token_type":   "Bearer",
			"expires_in":   1,
		})
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("first-jwt\n"), 0600))
	creds, err := NewK8sJWTCredentials(K8sJWTConfig{
		K8sTokenPath:         path,
		TokenServiceEndpoint: server.URL,
		SubjectToken:         "serviceaccount-example",
		SubjectTokenType:     "urn:ietf:params:oauth:token-type:subject_id",
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i, actor := range []string{"first-jwt", "rotated-jwt"} {
		if i > 0 {
			replacement := path + ".new"
			require.NoError(t, os.WriteFile(replacement, []byte(actor+"\n"), 0600))
			require.NoError(t, os.Rename(replacement, path))
			// The SDK caches the IAM token until expiry. Its response format has
			// whole-second precision, so use the shortest valid lifetime.
			time.Sleep(1100 * time.Millisecond)
		}
		token, err := creds.Token(ctx)
		require.NoError(t, err)
		require.Equal(t, "Bearer access-for-"+actor, token)
		select {
		case request := <-requests:
			require.Equal(t, url.Values{
				"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
				"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
				"actor_token":          {actor},
				"actor_token_type":     {JWTTokenType},
				"subject_token":        {"serviceaccount-example"},
				"subject_token_type":   {"urn:ietf:params:oauth:token-type:subject_id"},
			}, request)
		case <-ctx.Done():
			t.Fatal("token exchange request not received")
		}
	}
}
