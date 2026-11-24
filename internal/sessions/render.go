package sessions

import (
	"encoding/json"
	"fmt"
	"strings"
)

// MaxS3SessionPolicyBytes is the size S3 and MinIO both refuse an inline
// session policy above (libs/go.MaxSessionPolicyBytes states the same limit
// for a Binding's policy).
const MaxS3SessionPolicyBytes = 2048

// MaxGCSBoundaryBytes is the size Google's token exchange refuses a
// Credential Access Boundary above, by its documentation; not measured.
const MaxGCSBoundaryBytes = 2048

// location is where a scope's keys are in a bucket: the gateway's own prefix
// ("" or ending in "/") followed by the scope's.
func location(bucket, gatewayPrefix string, scope Scope) (string, error) {
	prefix, err := scope.Prefix()
	if err != nil {
		return "", err
	}
	// A resource is a pattern in every provider's grammar here: nothing from
	// configuration may bring a wildcard, a variable or a quote into it. The
	// key convention writes none.
	if bucket == "" || strings.ContainsAny(bucket+gatewayPrefix, "*?$'\"\\") || (gatewayPrefix != "" && !strings.HasSuffix(gatewayPrefix, "/")) {
		return "", fmt.Errorf("%w: bucket %q and key prefix %q cannot be written into a grant", ErrInvalidScope, bucket, gatewayPrefix)
	}
	return gatewayPrefix + prefix, nil
}

type s3Document struct {
	Version   string        `json:"Version"`
	Statement []s3Statement `json:"Statement"`
}

type s3Statement struct {
	Effect    string                         `json:"Effect"`
	Action    []string                       `json:"Action"`
	Resource  []string                       `json:"Resource"`
	Condition map[string]map[string][]string `json:"Condition,omitempty"`
}

// S3SessionPolicy renders a scope as the inline session policy of an S3 or
// MinIO AssumeRole call. A session's permissions are the intersection of this
// policy and the issuing identity's, so the policy can only narrow.
//
//	read         s3:GetObject under the prefix; s3:ListBucket for that prefix
//	ingest       s3:PutObject and the two multipart verbs under the prefix
//	maintenance  read's, plus s3:PutObject (the destination of a copy),
//	             s3:DeleteObject and s3:AbortMultipartUpload under the prefix
//
// A policy over MaxS3SessionPolicyBytes is ErrPolicyTooLarge: long
// identifiers are refused, never answered with a shorter, wider resource.
func S3SessionPolicy(bucket, gatewayPrefix string, scope Scope) ([]byte, error) {
	prefix, err := location(bucket, gatewayPrefix, scope)
	if err != nil {
		return nil, err
	}
	arn := "arn:aws:s3:::" + bucket
	objects := []string{arn + "/" + prefix + "*"}
	list := s3Statement{Effect: "Allow", Action: []string{"s3:ListBucket"}, Resource: []string{arn},
		Condition: map[string]map[string][]string{"StringLike": {"s3:prefix": {prefix + "*"}}}}
	var document s3Document
	switch scope.Mode {
	case Read:
		document.Statement = []s3Statement{{Effect: "Allow", Action: []string{"s3:GetObject"}, Resource: objects}, list}
	case Ingest:
		document.Statement = []s3Statement{{Effect: "Allow", Action: []string{"s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"}, Resource: objects}}
	case Maintenance:
		document.Statement = []s3Statement{{Effect: "Allow", Action: []string{"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"}, Resource: objects}, list}
	}
	document.Version = "2012-10-17"
	rendered, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	if len(rendered) > MaxS3SessionPolicyBytes {
		return nil, fmt.Errorf("%w: %d bytes, the limit is %d", ErrPolicyTooLarge, len(rendered), MaxS3SessionPolicyBytes)
	}
	return rendered, nil
}

type gcsBoundary struct {
	AccessBoundary struct {
		Rules []gcsRule `json:"accessBoundaryRules"`
	} `json:"accessBoundary"`
}

type gcsRule struct {
	AvailableResource    string   `json:"availableResource"`
	AvailablePermissions []string `json:"availablePermissions"`
	Condition            struct {
		Expression string `json:"expression"`
	} `json:"availabilityCondition"`
}

// GCSAccessBoundary renders a scope as the Credential Access Boundary of a
// downscoped Google token: one rule on the bucket, whose condition holds the
// object name, and for listing the list prefix, to the scope's prefix.
//
//	read         roles/storage.objectViewer   (get and list)
//	ingest       roles/storage.objectCreator  (create only: it cannot read,
//	             list, delete, or therefore overwrite)
//	maintenance  roles/storage.objectAdmin
//
// Not qualified: rendered from Google's documentation and never exchanged
// for a token.
func GCSAccessBoundary(bucket, gatewayPrefix string, scope Scope) ([]byte, error) {
	prefix, err := location(bucket, gatewayPrefix, scope)
	if err != nil {
		return nil, err
	}
	role := map[Mode]string{Read: "objectViewer", Ingest: "objectCreator", Maintenance: "objectAdmin"}[scope.Mode]
	rule := gcsRule{
		AvailableResource:    "//storage.googleapis.com/projects/_/buckets/" + bucket,
		AvailablePermissions: []string{"inRole:roles/storage." + role},
	}
	rule.Condition.Expression = fmt.Sprintf("resource.name.startsWith('projects/_/buckets/%s/objects/%s')", bucket, prefix)
	if scope.Mode != Ingest {
		// The list prefix is an API attribute, and it confines a listing only
		// inside a boundary: the same attribute in a bucket's IAM condition is
		// documented as not working.
		rule.Condition.Expression += fmt.Sprintf(" || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')", prefix)
	}
	var boundary gcsBoundary
	boundary.AccessBoundary.Rules = []gcsRule{rule}
	rendered, err := json.Marshal(boundary)
	if err != nil {
		return nil, err
	}
	if len(rendered) > MaxGCSBoundaryBytes {
		return nil, fmt.Errorf("%w: %d bytes, the limit is %d", ErrPolicyTooLarge, len(rendered), MaxGCSBoundaryBytes)
	}
	return rendered, nil
}

// AzureDirectorySAS is the shape of the user-delegation SAS a scope maps to
// on Azure: the fields that decide its reach, before anything is signed.
type AzureDirectorySAS struct {
	// SignedResource is "d": a directory and everything under it.
	SignedResource string `json:"sr"`
	// Container and Directory name it; Directory has no trailing separator.
	Container string `json:"container"`
	Directory string `json:"directory"`
	// Depth is the directory's number of segments (sdd), which the service
	// requires with sr=d.
	Depth int `json:"sdd"`
	// Permissions are the signed permissions (sp), in the service's order.
	Permissions string `json:"sp"`
	// RequiresHierarchicalNamespace: sr=d is defined for accounts with a
	// hierarchical namespace. On a flat account this grant is not available
	// and nothing narrower than a container is.
	RequiresHierarchicalNamespace bool `json:"requires_hierarchical_namespace"`
}

// AzureUserDelegationSAS renders a scope as a directory-scoped user-delegation
// SAS.
//
//	read         r, l          (read, list)
//	ingest       c, w          (create, write)
//	maintenance  r, c, w, d, l
//
// Not qualified: rendered from Microsoft's documentation and never signed.
// The gateway's Azure backend authenticates with the account's shared key
// today, which cannot request a user delegation key at all.
func AzureUserDelegationSAS(container, gatewayPrefix string, scope Scope) (AzureDirectorySAS, error) {
	prefix, err := location(container, gatewayPrefix, scope)
	if err != nil {
		return AzureDirectorySAS{}, err
	}
	directory := strings.TrimSuffix(prefix, "/")
	return AzureDirectorySAS{
		SignedResource: "d", Container: container, Directory: directory,
		Depth:                         strings.Count(directory, "/") + 1,
		Permissions:                   map[Mode]string{Read: "rl", Ingest: "cw", Maintenance: "rcwdl"}[scope.Mode],
		RequiresHierarchicalNamespace: true,
	}, nil
}
