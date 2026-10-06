package credentials

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

const (
	JWTTokenType = "urn:ietf:params:oauth:token-type:jwt"
)

// K8sJWTConfig holds configuration for Kubernetes projected volume JWT authentication.
type K8sJWTConfig struct {
	K8sTokenPath         string
	TokenServiceEndpoint string
	SubjectToken         string
	SubjectTokenType     string
}

// NewK8sJWTCredentials creates YDB credentials using a Kubernetes projected volume JWT.
// The JWT is used as the actor token and exchanged for an IAM token via OAuth2 token exchange.
//
// Token exchange flow:
//   - actor_token: K8s JWT (read from file on each exchange)
//   - actor_token_type: urn:ietf:params:oauth:token-type:jwt
//   - subject_token: service account id (static)
//   - subject_token_type: urn:<some-static-string>
func NewK8sJWTCredentials(cfg K8sJWTConfig) (credentials.Credentials, error) {
	if cfg.K8sTokenPath == "" {
		return nil, fmt.Errorf("k8s_token_path is required for K8s JWT auth")
	}
	if cfg.TokenServiceEndpoint == "" {
		return nil, fmt.Errorf("token_service_endpoint is required for K8s JWT auth")
	}
	if cfg.SubjectToken == "" {
		return nil, fmt.Errorf("subject_token is required for K8s JWT auth")
	}
	if cfg.SubjectTokenType == "" {
		return nil, fmt.Errorf("subject_token_type is required for K8s JWT auth")
	}

	return credentials.NewOauth2TokenExchangeCredentials(
		credentials.WithTokenEndpoint(cfg.TokenServiceEndpoint),
		// Actor = K8s JWT from projected volume (dynamic, re-read on each exchange)
		credentials.WithActorToken(
			NewFileTokenSource(cfg.K8sTokenPath, JWTTokenType),
		),
		// Subject = Service Account ID (static)
		credentials.WithFixedSubjectToken(
			cfg.SubjectToken,
			cfg.SubjectTokenType,
		),
	)
}
