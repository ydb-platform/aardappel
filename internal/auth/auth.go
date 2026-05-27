package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	ydbCredentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

type credentialsFileType struct {
	Type string `json:"type"`
}

type oauth2TokenExchangeFile struct {
	Type                string              `json:"type"`
	Oauth2TokenExchange oauth2TokenExchange `json:"oauth2_token_exchange"`
}

type oauth2TokenExchange struct {
	Payload   oauth2TokenExchangePayload `json:"payload"`
	Exchanger tokenExchangerConfig       `json:"exchanger"`
}

type tokenExchangerConfig struct {
	Endpoint string `json:"endpoint"`
}

type oauth2TokenExchangePayload struct {
	GrantType          string            `json:"grant_type"`
	RequestedTokenType string            `json:"requested_token_type"`
	SubjectTokenType   string            `json:"subject_token_type"`
	SubjectToken       tokenSourceConfig `json:"subject_token"`
	ActorTokenType     string            `json:"actor_token_type"`
	ActorToken         tokenSourceConfig `json:"actor_token"`
}

type tokenSourceConfig struct {
	Type  string `json:"type"`
	Value string `json:"value"`
	File  string `json:"file"`
}

type fileTokenSource struct {
	path      string
	tokenType string
}

type AuthConfig struct {
	CredentialsFile   string
	StaticToken       string
	ExchangerEndpoint string
}

func (s fileTokenSource) Token() (ydbCredentials.Token, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return ydbCredentials.Token{}, fmt.Errorf("read token file %q: %w", s.path, err)
	}
	return ydbCredentials.Token{
		Token:     string(data),
		TokenType: s.tokenType,
	}, nil
}

func CreateYdbDriverAuthOptions(config AuthConfig) ([]ydb.Option, error) {
	if (len(config.CredentialsFile) > 0 && len(config.StaticToken) > 0) ||
		(len(config.CredentialsFile) == 0 && len(config.StaticToken) == 0) {
		return nil, errors.New("it's either oauth2_file or static_token option must be set")
	}

	if len(config.StaticToken) > 0 {
		return []ydb.Option{
			ydb.WithAccessTokenCredentials(config.StaticToken),
		}, nil
	}

	opts, err := oauth2CredentialsOptions(config.CredentialsFile)
	if err != nil {
		return nil, err
	}
	if config.ExchangerEndpoint != "" {
		opts = append(opts, ydbCredentials.WithTokenEndpoint(config.ExchangerEndpoint))
	}
	return []ydb.Option{ydb.WithOauth2TokenExchangeCredentials(opts...)}, nil
}

func oauth2CredentialsOptions(oauthFile string) ([]ydbCredentials.Oauth2TokenExchangeCredentialsOption, error) {
	data, err := os.ReadFile(oauthFile)
	if err != nil {
		return nil, fmt.Errorf("read oauth2 credentials file %q: %w", oauthFile, err)
	}

	var fileType credentialsFileType
	if err = json.Unmarshal(data, &fileType); err != nil {
		return nil, fmt.Errorf("parse oauth2 credentials file %q: %w", oauthFile, err)
	}

	if strings.EqualFold(fileType.Type, "oauth2_token_exchange") {
		// Support an alternative credentials format that describes oauth2 exchange request,
		// but allows to keep secret tokens in separate files. See package README.md for details and tests for samples.
		return assembleOauth2ExchangeOptions(data)
	}

	return delegateCredentialsFileToSdk(data)
}

func delegateCredentialsFileToSdk(data []byte) ([]ydbCredentials.Oauth2TokenExchangeCredentialsOption, error) {
	var cfg ydbCredentials.OAuth2Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse oauth2 credentials file: %w", err)
	}
	return cfg.AsOptions()
}

func assembleOauth2ExchangeOptions(data []byte) ([]ydbCredentials.Oauth2TokenExchangeCredentialsOption, error) {
	var cfg oauth2TokenExchangeFile
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse oauth2 token exchange credentials file: %w", err)
	}

	payload := cfg.Oauth2TokenExchange.Payload
	var opts []ydbCredentials.Oauth2TokenExchangeCredentialsOption
	if payload.GrantType != "" {
		opts = append(opts, ydbCredentials.WithGrantType(payload.GrantType))
	}
	if payload.RequestedTokenType != "" {
		opts = append(opts, ydbCredentials.WithRequestedTokenType(payload.RequestedTokenType))
	}
	if cfg.Oauth2TokenExchange.Exchanger.Endpoint != "" {
		opts = append(opts, ydbCredentials.WithTokenEndpoint(cfg.Oauth2TokenExchange.Exchanger.Endpoint))
	}

	if payload.SubjectToken.Type != "" {
		tokenSource, err := tokenSource(payload.SubjectToken, payload.SubjectTokenType)
		if err != nil {
			return nil, err
		}
		opts = append(opts, ydbCredentials.WithSubjectToken(tokenSource))
	}
	if payload.ActorToken.Type != "" {
		tokenSource, err := tokenSource(payload.ActorToken, payload.ActorTokenType)
		if err != nil {
			return nil, err
		}
		opts = append(opts, ydbCredentials.WithActorToken(tokenSource))
	}

	return opts, nil
}

func tokenSource(src tokenSourceConfig, tokenType string) (ydbCredentials.TokenSource, error) {
	if tokenType == "" {
		return nil, errors.New("oauth2 token exchange token source requires token type")
	}

	switch strings.ToLower(src.Type) {
	case "value":
		if src.Value == "" {
			return nil, errors.New("oauth2 token exchange token source type value requires value")
		}
		return ydbCredentials.NewFixedTokenSource(src.Value, tokenType), nil
	case "file":
		if src.File == "" {
			return nil, errors.New("oauth2 token exchange token source type file requires file")
		}
		return fileTokenSource{path: src.File, tokenType: tokenType}, nil
	default:
		return nil, fmt.Errorf("unsupported oauth2 token exchange token source type %q", src.Type)
	}
}
