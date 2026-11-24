package sessions

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/codefly-dev/service-object-storage/internal/backend"
)

// Qualification is whether an issuer's sessions were proven, against the real
// store, to be refused outside their scope.
type Qualification string

const (
	// Qualified: the integration suite issues from this issuer and asserts
	// the store's own refusals.
	Qualified Qualification = "qualified"
	// Unqualified: the mechanism is rendered from the provider's
	// documentation and nothing was ever issued. Such an issuer issues
	// nothing, and a gateway configured for scoped sessions refuses to start
	// on it.
	Unqualified Qualification = "unqualified"
)

// Credential is one issued session.
type Credential struct {
	AccessKey, SecretKey, SessionToken string
	// Expires is when the store stops honouring it.
	Expires time.Time
}

// Issuer is a store's way of confining a credential to a scope.
type Issuer interface {
	// Kind is the backend kind it issues for.
	Kind() string
	Qualification() Qualification
	// Grant renders, without calling anything, the provider's own statement
	// of what a session for scope may do.
	Grant(scope Scope) ([]byte, error)
	// Issue asks the store for a session confined to scope, valid for ttl.
	Issue(ctx context.Context, scope Scope, ttl time.Duration) (Credential, error)
}

// IssuerFor is the issuer of the store the gateway is configured for. A kind
// with no way to confine a credential ("mem") has none.
func IssuerFor(cfg backend.Config) (Issuer, error) {
	prefix, err := backend.NormalizePrefix(cfg.Prefix)
	if err != nil {
		return nil, err
	}
	cfg.Prefix = prefix
	switch cfg.Kind {
	case "minio":
		return &minioIssuer{cfg: cfg}, nil
	case "s3":
		return unqualified{kind: "s3", grant: func(scope Scope) ([]byte, error) { return S3SessionPolicy(cfg.Bucket, cfg.Prefix, scope) }}, nil
	case "gcs":
		return unqualified{kind: "gcs", grant: func(scope Scope) ([]byte, error) { return GCSAccessBoundary(cfg.Bucket, cfg.Prefix, scope) }}, nil
	case "azure":
		return unqualified{kind: "azure", grant: func(scope Scope) ([]byte, error) {
			sas, err := AzureUserDelegationSAS(cfg.Bucket, cfg.Prefix, scope)
			if err != nil {
				return nil, err
			}
			return json.Marshal(sas)
		}}, nil
	default:
		return nil, fmt.Errorf("%w: backend %q cannot confine a credential", ErrUnqualified, cfg.Kind)
	}
}

// unqualified renders a provider's grant and issues nothing.
type unqualified struct {
	kind  string
	grant func(Scope) ([]byte, error)
}

func (issuer unqualified) Kind() string                      { return issuer.kind }
func (issuer unqualified) Qualification() Qualification      { return Unqualified }
func (issuer unqualified) Grant(scope Scope) ([]byte, error) { return issuer.grant(scope) }

func (issuer unqualified) Issue(context.Context, Scope, time.Duration) (Credential, error) {
	return Credential{}, fmt.Errorf("%w: the %s issuer is not qualified", ErrUnqualified, issuer.kind)
}

// minioIssuer exchanges the gateway's own MinIO credential for an STS session
// under a scope's session policy.
type minioIssuer struct {
	cfg backend.Config
}

func (issuer *minioIssuer) Kind() string                 { return "minio" }
func (issuer *minioIssuer) Qualification() Qualification { return Qualified }

func (issuer *minioIssuer) Grant(scope Scope) ([]byte, error) {
	return S3SessionPolicy(issuer.cfg.Bucket, issuer.cfg.Prefix, scope)
}

func (issuer *minioIssuer) Issue(ctx context.Context, scope Scope, ttl time.Duration) (Credential, error) {
	policy, err := issuer.Grant(scope)
	if err != nil {
		return Credential{}, err
	}
	endpoint := issuer.cfg.Endpoint
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	provider, err := credentials.NewSTSAssumeRole(endpoint, credentials.STSAssumeRoleOptions{
		AccessKey: issuer.cfg.AccessKey, SecretKey: issuer.cfg.SecretKey, Location: issuer.cfg.Region,
		Policy: string(policy), DurationSeconds: int(ttl / time.Second),
	})
	if err != nil {
		return Credential{}, fmt.Errorf("%w: %w", ErrIssuerUnavailable, err)
	}
	asked := time.Now()
	// A redirect is not followed: the issuing credential signs one request,
	// to the endpoint the gateway was configured for.
	issued, err := provider.GetWithContext(&credentials.CredContext{Context: ctx, Client: &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}})
	if err != nil {
		return Credential{}, fmt.Errorf("%w: %w", ErrIssuerUnavailable, err)
	}
	// A static credential answered in place of a session would be the
	// gateway's own reach under another name.
	if issued.SessionToken == "" || issued.AccessKeyID == "" || issued.SecretAccessKey == "" || issued.AccessKeyID == issuer.cfg.AccessKey {
		return Credential{}, fmt.Errorf("%w: the store answered without temporary credentials", ErrIssuerUnavailable)
	}
	expires := asked.Add(ttl)
	if !issued.Expiration.IsZero() && issued.Expiration.Before(expires) {
		expires = issued.Expiration
	}
	return Credential{AccessKey: issued.AccessKeyID, SecretKey: issued.SecretAccessKey, SessionToken: issued.SessionToken, Expires: expires}, nil
}
