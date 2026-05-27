# Auth

This package creates YDB driver authentication options for aardappel.

At the aardappel config level, each side of replication can use exactly one of:

- `*_static_token`: a final YDB access token, used directly without OAuth2 token exchange.
- `*_oauth2_file`: a credentials file used to configure OAuth2 token exchange.

For OAuth2 credentials, aardappel currently supports two JSON file formats.

## YDB SDK OAuth2 Format

This is the native YDB SDK OAuth2 token exchange credentials format documented here:

https://ydb.tech/docs/en/reference/ydb-sdk/auth?version=v25.3#oauth2-key-file-format

Example:

```json
{
  "token-endpoint": "https://sts.example.net/oauth2/token/exchange",
  "subject-credentials": {
    "type": "JWT",
    "alg": "RS256",
    "private-key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
    "kid": "publickey-example",
    "iss": "serviceaccount-example",
    "sub": "serviceaccount-example"
  }
}
```

The file is parsed by the YDB Go SDK. Supported fields and token source types are therefore the ones supported by the SDK, including `JWT` and `FIXED` token sources.

## OAuth2 Token Exchange Format

This package also supports an alternative format:

```json
{
  "type": "oauth2_token_exchange",
  "oauth2_token_exchange": {
    "payload": {
      "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
      "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
      "actor_token_type": "urn:ietf:params:oauth:token-type:jwt",
      "actor_token": {
        "type": "file",
        "file": "/var/run/secrets/serviceaccount/token"
      },
      "subject_token_type": "urn:ietf:params:oauth:token-type:subject_id",
      "subject_token": {
        "type": "value",
        "value": "serviceaccount-example"
      }
    },
    "exchanger": {
      "endpoint": "https://sts.example.net/oauth2/token/exchange"
    }
  }
}
```

The purpose of this format is to keep sensitive and non-sensitive parts separate.
The native YDB format requires secret material and other significant but non-sensitive settings to live in the same file. In Kubernetes, for example, this is inconvenient when the actor token is already available as a mounted service account token file.

The alternative format lets most of the OAuth2 request be described as non-sensitive config while secret token values can be read from separate files.
This is friendlier to Kubernetes and similar environments where a JWT or another source token is provisioned separately by the platform, often as a mounted file, and the application cannot control its shape or wrap it into the YDB SDK credentials-file format.

Supported token sources in the alternative format:

- `"type": "value"`: the token value is stored directly in the credentials file.
- `"type": "file"`: the token value is read from the referenced file when the SDK performs token exchange.

Supported payload fields:

- `grant_type`
- `requested_token_type`
- `subject_token_type`
- `subject_token`
- `actor_token_type`
- `actor_token`

The optional exchanger endpoint is configured as:

```json
{
  "exchanger": {
    "endpoint": "https://sts.example.net/oauth2/token/exchange"
  }
}
```

## Endpoint Override

The aardappel config can specify side-specific OAuth2 exchanger endpoints:

- `src_oauth2_endpoint`
- `dst_oauth2_endpoint`

When a side-specific endpoint is set in aardappel config, it overrides the endpoint from the credentials file.

When it is not set, the endpoint from the credentials file is used:

- YDB SDK format: `token-endpoint`
- alternative format: `oauth2_token_exchange.exchanger.endpoint`
