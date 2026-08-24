package s3

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

func TestClampToCreds(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	static := aws.Credentials{AccessKeyID: "AK", SecretAccessKey: "SK"}
	soon := aws.Credentials{AccessKeyID: "AK", SecretAccessKey: "SK", CanExpire: true, Expires: now.Add(10 * time.Minute)}
	later := aws.Credentials{AccessKeyID: "AK", SecretAccessKey: "SK", CanExpire: true, Expires: now.Add(48 * time.Hour)}

	// Static credentials never expire: the requested lifetime passes through.
	require.Equal(t, time.Hour, clampToCreds(now, time.Hour, static))
	// Session credentials expiring before the request cap the lifetime.
	require.Equal(t, 10*time.Minute, clampToCreds(now, time.Hour, soon))
	// Session credentials outliving the request leave it unchanged.
	require.Equal(t, time.Hour, clampToCreds(now, time.Hour, later))
}
