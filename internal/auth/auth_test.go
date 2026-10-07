package auth

import (
	"aardappel/internal/config"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	ydbConfig "github.com/ydb-platform/ydb-go-sdk/v3/config"
	ydbCredentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

func TestConfiguredExchangerEndpointOverridesCredentialsEndpoint(t *testing.T) {
	actorJWT := "eyIKwhatever.claims.signature" // supposed to be jwt but meh, let's not trigger semgrep
	actorTokenFile := writeTempFile(t, "actor-token", actorJWT)
	privateKeyPEM := generateRSAPrivateKeyPEM(t)
	server := startTokenExchangeServer(t)

	testCases := []struct {
		name            string
		credentialsJSON string
		authConfig      AuthConfig
		wantRequest     url.Values
		wantSubjectJWT  *jwtExpectation
	}{
		{
			name: "oauth2 token exchange credentials",
			credentialsJSON: fmt.Sprintf(`{
			  "type": "oauth2_token_exchange",
			  "oauth2_token_exchange": {
			    "payload": {
			      "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
			      "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
			      "actor_token_type": "urn:ietf:params:oauth:token-type:jwt",
			      "actor_token": {
			        "type": "file",
			        "file": %q
			      },
			      "subject_token_type": "urn:ietf:params:oauth:token-type:subject_id",
			      "subject_token": {
			        "type": "value",
			        "value": "serviceaccount-xyz123"
			      }
			    },
			    "exchanger": {
			      "endpoint": "http://127.0.0.1:1?some-invalid-endpoint-to-be-overriden-by-config"
			    }
			  }
			}`, actorTokenFile),
			authConfig: AuthConfig{ExchangerEndpoint: server.URL},
			wantRequest: values(
				"grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
				"requested_token_type", "urn:ietf:params:oauth:token-type:access_token",
				"actor_token", actorJWT,
				"actor_token_type", "urn:ietf:params:oauth:token-type:jwt",
				"subject_token", "serviceaccount-xyz123",
				"subject_token_type", "urn:ietf:params:oauth:token-type:subject_id",
			),
		},

		{
			name: "sdk credentials",
			credentialsJSON: `{
			  "subject-credentials": {
			    "type": "FIXED",
			    "token": "subject-token",
			    "token-type": "urn:ietf:params:oauth:token-type:jwt"
			  }
			}`,
			authConfig: AuthConfig{ExchangerEndpoint: server.URL},
			wantRequest: values(
				"grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
				"requested_token_type", "urn:ietf:params:oauth:token-type:access_token",
				"subject_token", "subject-token",
				"subject_token_type", "urn:ietf:params:oauth:token-type:jwt",
			),
		},

		{
			name: "sdk credentials with exchanger from file",
			credentialsJSON: fmt.Sprintf(`{
			  "token-endpoint": %q,
			  "subject-credentials": {
			    "type": "FIXED",
			    "token": "subject-token",
			    "token-type": "urn:ietf:params:oauth:token-type:jwt"
			  }
			}`, server.URL),
			authConfig: AuthConfig{ /* exchanger endpoint from creds will be used */ },
			wantRequest: values(
				"grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
				"requested_token_type", "urn:ietf:params:oauth:token-type:access_token",
				"subject_token", "subject-token",
				"subject_token_type", "urn:ietf:params:oauth:token-type:jwt",
			),
		},

		{
			name: "sdk credentials with jwt subject token",
			credentialsJSON: fmt.Sprintf(`{
			  "token-endpoint": "http://127.0.0.1:1?some-invalid-endpoint-to-be-overriden-by-config",
			  "subject-credentials": {
			    "type": "JWT",
			    "alg": "RS256",
			    "private-key": %q,
			    "kid": "test-key-id",
			    "iss": "test-issuer",
			    "sub": "test-subject",
			    "aud": "test-audience",
			    "jti": "test-jwt-id"
			  }
			}`, privateKeyPEM),
			authConfig: AuthConfig{ExchangerEndpoint: server.URL},
			wantRequest: values(
				"grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
				"requested_token_type", "urn:ietf:params:oauth:token-type:access_token",
				"subject_token_type", "urn:ietf:params:oauth:token-type:jwt",
			),
			wantSubjectJWT: &jwtExpectation{
				Algorithm: "RS256",
				KeyID:     "test-key-id",
				Issuer:    "test-issuer",
				Subject:   "test-subject",
				Audience:  "test-audience",
				ID:        "test-jwt-id",
			},
		},

		{
			name: "oauth2 token exchange credentials with exchanger from file",
			credentialsJSON: fmt.Sprintf(`{
			  "type": "oauth2_token_exchange",
			  "oauth2_token_exchange": {
			    "payload": {
			      "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
			      "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
			      "actor_token_type": "urn:ietf:params:oauth:token-type:jwt",
			      "actor_token": {
			        "type": "file",
			        "file": %q
			      },
			      "subject_token_type": "urn:ietf:params:oauth:token-type:subject_id",
			      "subject_token": {
			        "type": "value",
			        "value": "serviceaccount-xyz123"
			      }
			    },
			    "exchanger": {
			      "endpoint": %q
			    }
			  }
			}`, actorTokenFile, server.URL),
			authConfig: AuthConfig{ /* exchanger endpoint from creds will be used */ },
			wantRequest: values(
				"grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
				"requested_token_type", "urn:ietf:params:oauth:token-type:access_token",
				"actor_token", actorJWT,
				"actor_token_type", "urn:ietf:params:oauth:token-type:jwt",
				"subject_token", "serviceaccount-xyz123",
				"subject_token_type", "urn:ietf:params:oauth:token-type:subject_id",
			),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			credentialsFile := writeTempFile(t, "credentials.json", tc.credentialsJSON)
			authConfig := tc.authConfig
			authConfig.CredentialsFile = credentialsFile

			token := obtainToken(t, authConfig)

			if token != "Bearer very-very-token" {
				t.Fatalf("unexpected access token: %q", token)
			}
			assertTokenExchangeRequest(t, tc.wantRequest, server.Request(), tc.wantSubjectJWT)
		})
	}
}

type jwtExpectation struct {
	Algorithm string
	KeyID     string
	Issuer    string
	Subject   string
	Audience  string
	ID        string
}

type tokenExchangeServer struct {
	URL     string
	mu      sync.Mutex
	request url.Values
}

func startTokenExchangeServer(t *testing.T) *tokenExchangeServer {
	t.Helper()

	server := &tokenExchangeServer{}
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse token exchange request: %v", err)
			http.Error(w, "invalid form", http.StatusBadRequest)
			return
		}
		server.mu.Lock()
		server.request = cloneValues(r.Form)
		server.mu.Unlock()
		_, _ = w.Write([]byte(`{"access_token":"very-very-token","token_type":"Bearer","expires_in":3600}`))
	}))
	t.Cleanup(httpServer.Close)

	server.URL = httpServer.URL
	return server
}

func (s *tokenExchangeServer) Request() url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneValues(s.request)
}

// Inspect the credentials installed by the public auth factory in a real SDK
// driver. SingleConn avoids discovery; no YDB server is needed for these tests.
func driverCredentials(t *testing.T, config AuthConfig) ydbCredentials.Credentials {
	t.Helper()

	opts, err := CreateYdbDriverAuthOptions(config)
	must(t, err)
	var creds ydbCredentials.Credentials
	opts = append(opts,
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.With(func(cfg *ydbConfig.Config) { creds = cfg.Credentials() }),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	driver, err := ydb.Open(ctx, "grpc://127.0.0.1:1/local", opts...)
	must(t, err)
	t.Cleanup(func() { must(t, driver.Close(context.Background())) })
	require.NotNil(t, creds)
	return creds
}

func obtainToken(t *testing.T, config AuthConfig) string {
	t.Helper()

	creds := driverCredentials(t, config)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	token, err := creds.Token(ctx)
	must(t, err)
	return token
}

func writeTempFile(t *testing.T, name string, data string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	must(t, os.WriteFile(path, []byte(data), 0600))
	return path
}

func values(pairs ...string) url.Values {
	result := url.Values{}
	for i := 0; i < len(pairs); i += 2 {
		result.Set(pairs[i], pairs[i+1])
	}
	return result
}

func cloneValues(src url.Values) url.Values {
	result := url.Values{}
	for key, value := range src {
		result[key] = append([]string(nil), value...)
	}
	return result
}

func assertTokenExchangeRequest(t *testing.T, want url.Values, got url.Values, wantSubjectJWT *jwtExpectation) {
	t.Helper()

	got = cloneValues(got)
	if wantSubjectJWT != nil {
		subjectToken := got.Get("subject_token")
		got.Del("subject_token")
		assertJWT(t, wantSubjectJWT, subjectToken)
	}

	if !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected token exchange params:\nwant: %s\n got: %s", want.Encode(), got.Encode())
	}
}

func assertJWT(t *testing.T, want *jwtExpectation, token string) {
	t.Helper()

	if token == "" {
		t.Fatal("expected subject_token JWT")
	}

	claims := &jwt.RegisteredClaims{}
	parsed, _, err := jwt.NewParser().ParseUnverified(token, claims)
	must(t, err)

	if parsed.Header["alg"] != want.Algorithm {
		t.Fatalf("unexpected JWT alg: %v", parsed.Header["alg"])
	}
	if parsed.Header["kid"] != want.KeyID {
		t.Fatalf("unexpected JWT kid: %v", parsed.Header["kid"])
	}
	if claims.Issuer != want.Issuer {
		t.Fatalf("unexpected JWT issuer: %q", claims.Issuer)
	}
	if claims.Subject != want.Subject {
		t.Fatalf("unexpected JWT subject: %q", claims.Subject)
	}
	if claims.ID != want.ID {
		t.Fatalf("unexpected JWT id: %q", claims.ID)
	}
	if !contains(claims.Audience, want.Audience) {
		t.Fatalf("unexpected JWT audience: %v", claims.Audience)
	}
	if claims.IssuedAt == nil {
		t.Fatal("expected JWT issued-at claim")
	}
	if claims.ExpiresAt == nil {
		t.Fatal("expected JWT expiration claim")
	}
}

func generateRSAPrivateKeyPEM(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	must(t, err)

	data, err := x509.MarshalPKCS8PrivateKey(key)
	must(t, err)

	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: data,
	}))
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateYdbDriverAuthOptionsRejectsInvalidMethodSelection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config AuthConfig
	}{
		{name: "no credentials"},
		{name: "endpoint only", config: AuthConfig{ExchangerEndpoint: "https://sts.example.net/token"}},
		{name: "file and static token", config: AuthConfig{CredentialsFile: "unused.json", StaticToken: "unused"}},
		{name: "file and K8s JWT", config: AuthConfig{CredentialsFile: "unused.json", K8sJWTAuth: &config.K8sJWTAuthConfig{}}},
		{name: "static token and K8s JWT", config: AuthConfig{StaticToken: "unused", K8sJWTAuth: &config.K8sJWTAuthConfig{}}},
		{name: "all methods", config: AuthConfig{CredentialsFile: "unused.json", StaticToken: "unused", K8sJWTAuth: &config.K8sJWTAuthConfig{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := CreateYdbDriverAuthOptions(tc.config)
			require.ErrorContains(t, err, "exactly one of oauth2_file, static_token or k8s_jwt_auth must be set")
			require.Nil(t, opts)
		})
	}
}

func TestStaticTokenCredentials(t *testing.T) {
	require.Equal(t, "static-access-token", obtainToken(t, AuthConfig{StaticToken: "static-access-token"}))
}

func TestK8sJWTDriverCredentials(t *testing.T) {
	server := startTokenExchangeServer(t)
	actorJWT := "test-k8s-jwt"
	tokenPath := writeTempFile(t, "token", " \n"+actorJWT+"\n")
	token := obtainToken(t, AuthConfig{
		ExchangerEndpoint: server.URL,
		K8sJWTAuth: &config.K8sJWTAuthConfig{
			K8sTokenPath:         tokenPath,
			SubjectToken:         "serviceaccount-example",
			SubjectTokenType:     "urn:ietf:params:oauth:token-type:subject_id",
		},
	})
	require.Equal(t, "Bearer very-very-token", token)
	assertTokenExchangeRequest(t, values(
		"grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
		"requested_token_type", "urn:ietf:params:oauth:token-type:access_token",
		"actor_token", actorJWT,
		"actor_token_type", "urn:ietf:params:oauth:token-type:jwt",
		"subject_token", "serviceaccount-example",
		"subject_token_type", "urn:ietf:params:oauth:token-type:subject_id",
	), server.Request(), nil)
}

func TestK8sJWTDriverCredentialsRequireAllFields(t *testing.T) {
	for _, field := range []string{"k8s_token_path", "oauth2_endpoint", "subject_token", "subject_token_type"} {
		t.Run(field, func(t *testing.T) {
			cfg := AuthConfig{
				ExchangerEndpoint: "https://sts.example.net/token",
				K8sJWTAuth: &config.K8sJWTAuthConfig{
					K8sTokenPath:         "/unused/token",
					SubjectToken:         "serviceaccount-example",
					SubjectTokenType:     "urn:ietf:params:oauth:token-type:subject_id",
				},
			}
			expectedError := "create K8s JWT credentials: "+field+" is required for K8s JWT auth"
			switch field {
			case "k8s_token_path":
				cfg.K8sJWTAuth.K8sTokenPath = ""
			case "oauth2_endpoint":
				cfg.ExchangerEndpoint = ""
				expectedError = "oauth2_endpoint must be set"
			case "subject_token":
				cfg.K8sJWTAuth.SubjectToken = ""
			case "subject_token_type":
				cfg.K8sJWTAuth.SubjectTokenType = ""
			}
			opts, err := CreateYdbDriverAuthOptions(cfg)
			require.ErrorContains(t, err, expectedError)
			require.Nil(t, opts)
		})
	}
}

func TestK8sJWTDriverCredentialsPropagateTokenFileError(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "missing-token")
	creds := driverCredentials(t, AuthConfig{
		ExchangerEndpoint: "http://127.0.0.1:1/unused",
		K8sJWTAuth: &config.K8sJWTAuthConfig{
			K8sTokenPath:         tokenPath,
			SubjectToken:         "serviceaccount-example",
			SubjectTokenType:     "urn:ietf:params:oauth:token-type:subject_id",
		}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	token, err := creds.Token(ctx)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.ErrorContains(t, err, tokenPath)
	require.Empty(t, token)
}
