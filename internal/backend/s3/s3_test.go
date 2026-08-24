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
	subSecond := aws.Credentials{AccessKeyID: "AK", SecretAccessKey: "SK", CanExpire: true, Expires: now.Add(500 * time.Millisecond)}
	expired := aws.Credentials{AccessKeyID: "AK", SecretAccessKey: "SK", CanExpire: true, Expires: now.Add(-5 * time.Second)}

	// Static credentials never expire: the requested lifetime passes through.
	d, err := clampToCreds(now, time.Hour, static)
	require.NoError(t, err)
	require.Equal(t, time.Hour, d)

	// Session credentials expiring before the request cap the lifetime.
	d, err = clampToCreds(now, time.Hour, soon)
	require.NoError(t, err)
	require.Equal(t, 10*time.Minute, d)

	// Session credentials outliving the request leave it unchanged.
	d, err = clampToCreds(now, time.Hour, later)
	require.NoError(t, err)
	require.Equal(t, time.Hour, d)

	// Sub-second remaining validity would render X-Amz-Expires as 0: refuse.
	_, err = clampToCreds(now, time.Hour, subSecond)
	require.Error(t, err)

	// Already-expired credentials (clock skew) refuse rather than sign a
	// negative-lifetime URL.
	_, err = clampToCreds(now, time.Hour, expired)
	require.Error(t, err)
}
