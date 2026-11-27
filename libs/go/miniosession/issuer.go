// Package miniosession issues temporary credentials at the trusted composition root.
package miniosession

import (
	"context"
	"errors"
	"net/http"
	"slices"

	storagev1 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v1"
	objectstorage "github.com/codefly-dev/service-object-storage/libs/go"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Issuer belongs to the trusted composition root. Its endpoint,
// credentials, base and binder must come from deployment configuration and
// verified caller context. Only the resulting short-lived binding is handed
// to the gateway's bound read RPCs.
type Issuer struct {
	Endpoint, Region, Bucket, Prefix string
	AccessKey, SecretKey             string
}

// Issue returns a read credential for one scope of the admitted caller.
// Even callers admitted for writes receive a SELECT-equivalent session here.
func (issuer Issuer) Issue(ctx context.Context, base string, binding objectstorage.Binding, scope string) (*storagev1.Binding, error) {
	if issuer.Endpoint == "" || issuer.AccessKey == "" || issuer.SecretKey == "" {
		return nil, errors.New("a MinIO reader issuer requires an endpoint and credentials")
	}
	binding, err := binding.Normalize()
	if err != nil {
		return nil, err
	}
	if !slices.Contains(binding.Reach, scope) {
		return nil, objectstorage.ErrOutsideReach
	}
	binding.Reach = []string{scope}
	binding.Access, binding.Shared = objectstorage.Read, objectstorage.Read
	policy, err := objectstorage.S3SessionPolicy(issuer.Bucket, issuer.Prefix, base, binding)
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
	return &storagev1.Binding{Kind: "minio", Endpoint: issuer.Endpoint, Region: issuer.Region, Bucket: issuer.Bucket, Prefix: issuer.Prefix,
		Credential: &storagev1.Credential{Kind: &storagev1.Credential_AccessKey{AccessKey: &storagev1.AccessKey{Id: issued.AccessKeyID, Secret: issued.SecretAccessKey, SessionToken: issued.SessionToken}}}}, nil
}
