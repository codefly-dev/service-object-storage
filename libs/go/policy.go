package objectstorage

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
)

// MaxSessionPolicyBytes is the size S3 and MinIO both refuse an inline
// session policy above.
const MaxSessionPolicyBytes = 2048

// ErrPolicyTooLarge: the Binding reaches more scopes than one session policy
// can name. The Store still confines it; the store itself cannot be asked to.
var ErrPolicyTooLarge = errors.New("object store: the binding does not fit one session policy")

type policyDocument struct {
	Version   string            `json:"Version"`
	Statement []policyStatement `json:"Statement"`
}

type policyStatement struct {
	Effect    string                         `json:"Effect"`
	Action    []string                       `json:"Action"`
	Resource  []string                       `json:"Resource"`
	Condition map[string]map[string][]string `json:"Condition,omitempty"`
}

// S3SessionPolicy renders a Binding as the inline session policy of an S3 or
// MinIO AssumeRole call: the same model the Store enforces, stated in the
// store's own terms, so that a credential issued under it is refused outside
// the Binding by the store itself. keyPrefix is the prefix the gateway puts
// in front of every key ("" or ending in "/").
//
// The policy holds for whoever carries the credential, Store or not. It is
// narrower than the Store in one place: a session policy cannot tell a
// well-formed key from another, so it confines by prefix and nothing else.
func S3SessionPolicy(bucket, keyPrefix, base string, binding Binding) ([]byte, error) {
	normalized, err := binding.Normalize()
	if err != nil {
		return nil, err
	}
	codec, err := objectkey.New(base)
	if err != nil {
		return nil, err
	}
	// A resource is a pattern: none of these may reach it from configuration.
	// Nothing the key convention writes holds one.
	if bucket == "" || strings.ContainsAny(bucket+keyPrefix, "*?$") || (keyPrefix != "" && !strings.HasSuffix(keyPrefix, "/")) {
		return nil, fmt.Errorf("bucket %q and key prefix %q cannot be written into a policy", bucket, keyPrefix)
	}
	arn := "arn:aws:s3:::" + bucket
	var read, write, remove, keep, listed []string
	own := objectkey.Principal(normalized.Principal)
	for _, scope := range normalized.Reach {
		scopePrefix, err := codec.ScopePrefix(normalized.Partition, scope)
		if err != nil {
			return nil, err
		}
		slot := func(slot objectkey.Slot) string {
			prefix, _ := codec.SlotPrefix(normalized.Partition, scope, slot)
			return keyPrefix + prefix
		}
		if _, err := codec.SlotPrefix(normalized.Partition, scope, own); err != nil {
			return nil, err
		}
		content := arn + "/" + slot(objectkey.Content()) + "*"
		if normalized.Mode == Privileged {
			everything := arn + "/" + keyPrefix + scopePrefix + "*"
			read, listed = append(read, everything), append(listed, keyPrefix+scopePrefix+"*")
			if normalized.Access == ReadWrite {
				write, remove, keep = append(write, everything), append(remove, everything), append(keep, content)
			}
			continue
		}
		read = append(read, arn+"/"+slot(own)+"*", arn+"/"+slot(objectkey.Shared())+"*", content)
		listed = append(listed, slot(own)+"*", slot(objectkey.Shared())+"*")
		if normalized.Access == ReadWrite {
			// Content is written by a copy from the writer's own staging key,
			// so the writer may put there. It may never delete there.
			write, remove = append(write, arn+"/"+slot(own)+"*", content), append(remove, arn+"/"+slot(own)+"*")
			if normalized.Shared == ReadWrite {
				write, remove = append(write, arn+"/"+slot(objectkey.Shared())+"*"), append(remove, arn+"/"+slot(objectkey.Shared())+"*")
			}
		}
	}
	document := policyDocument{Version: "2012-10-17", Statement: []policyStatement{
		{Effect: "Allow", Action: []string{"s3:GetObject"}, Resource: read},
		{Effect: "Allow", Action: []string{"s3:ListBucket"}, Resource: []string{arn},
			Condition: map[string]map[string][]string{"StringLike": {"s3:prefix": listed}}},
	}}
	if len(write) > 0 {
		document.Statement = append(document.Statement,
			policyStatement{Effect: "Allow", Action: []string{"s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"}, Resource: write},
			policyStatement{Effect: "Allow", Action: []string{"s3:DeleteObject"}, Resource: remove})
	}
	if len(keep) > 0 {
		document.Statement = append(document.Statement, policyStatement{Effect: "Deny", Action: []string{"s3:DeleteObject"}, Resource: keep})
	}
	rendered, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	if len(rendered) > MaxSessionPolicyBytes {
		return nil, fmt.Errorf("%w: %d bytes for %d scopes, the limit is %d", ErrPolicyTooLarge, len(rendered), len(normalized.Reach), MaxSessionPolicyBytes)
	}
	return rendered, nil
}
