package objectstorage

import (
	"context"
	"errors"
	"net/http"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIOReaderIssuer belongs to the trusted composition root. Its endpoint,
// credentials, base and binder must come from deployment configuration and
// verified caller context. Only the resulting short-lived binding is handed
// to the gateway's bound read RPCs.
type MinIOReaderIssuer struct {
	Endpoint, Region, Bucket, Prefix, Base string
	AccessKey, SecretKey                   string
	Binder                                 Binder
}

// Issue returns a read credential for one scope of the admitted caller.
// Even callers admitted for writes receive a SELECT-equivalent session here.
func (issuer MinIOReaderIssuer) Issue(ctx context.Context, scope string) (*storagev0.Binding, error) {
	if isNil(issuer.Binder) || issuer.Endpoint == "" || issuer.AccessKey == "" || issuer.SecretKey == "" {
		return nil, errors.New("a MinIO reader issuer requires an endpoint, credentials and a binder")
	}
	binding, err := issuer.Binder.Bind(ctx)
	if err != nil {
		return nil, err
	}
	binding, err = binding.Normalize()
	if err != nil {
		return nil, err
	}
	scope, _, err = binding.place(Ref{Scope: scope})
	if err != nil {
		return nil, err
	}
	binding.Reach = []string{scope}
	binding.Access, binding.Shared = Read, Read
	policy, err := S3SessionPolicy(issuer.Bucket, issuer.Prefix, issuer.Base, binding)
	if err != nil {
		return nil, err
	}
	provider, err := credentials.NewSTSAssumeRole(issuer.Endpoint, credentials.STSAssumeRoleOptions{
		AccessKey: issuer.AccessKey, SecretKey: issuer.SecretKey, Location: issuer.Region,
		Policy: string(policy), DurationSeconds: 900,
	})
	if err != nil {
		return nil, err
	}
	issued, err := provider.GetWithContext(&credentials.CredContext{Context: ctx, Client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}})
	if err != nil {
		return nil, err
	}
	if issued.SessionToken == "" || issued.AccessKeyID == "" || issued.SecretAccessKey == "" || issued.AccessKeyID == issuer.AccessKey {
		return nil, errors.New("MinIO did not issue temporary session credentials")
	}
	return &storagev0.Binding{Kind: "minio", Endpoint: issuer.Endpoint, Region: issuer.Region, Bucket: issuer.Bucket, Prefix: issuer.Prefix,
		Credential: &storagev0.Credential{Kind: &storagev0.Credential_AccessKey{AccessKey: &storagev0.AccessKey{Id: issued.AccessKeyID, Secret: issued.SecretAccessKey, SessionToken: issued.SessionToken}}}}, nil
}
