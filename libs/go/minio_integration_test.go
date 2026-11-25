//go:build integration

package objectstorage_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"

	storagev1 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v1"
	objectstorage "github.com/codefly-dev/service-object-storage/libs/go"
	"github.com/codefly-dev/service-object-storage/libs/go/internal/gatewaytest"
	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
	"github.com/codefly-dev/service-object-storage/libs/go/objectscopetest"
)

func fetch(url string) ([]byte, error) {
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", response.StatusCode, body)
	}
	return body, nil
}

// The proof, on the pinned MinIO, through the real gateway server. It also
// follows every presigned GET the model allows, so what a URL serves is held
// to the model as well.
func TestMinIOProveHoldsOnAStore(t *testing.T) {
	client, _ := gatewaytest.LiveMinIO(t)
	objectscopetest.Prove(t, objectscopetest.Proof{Client: client, Base: "proof", Rounds: 400, Objects: 200, Fetch: fetch})
}

// The proof still fails on MinIO for a store that does not hold the model:
// the two defects that live in what the store is asked, not in the wrapper.
func TestMinIOProveCatchesADefectiveStore(t *testing.T) {
	for name, defect := range map[string]func(*objectstorage.Store, objectstorage.Binder) objectscopetest.Confined{
		"copy does not check its source": func(store *objectstorage.Store, binder objectstorage.Binder) objectscopetest.Confined {
			return copyUnchecked{Store: store, bind: binder.Bind}
		},
		"list asks for the prefix without its separator": func(store *objectstorage.Store, binder objectstorage.Binder) objectscopetest.Confined {
			return listRawPrefix{Store: store, bind: binder.Bind}
		},
		"keys are the identifiers joined as they come": func(store *objectstorage.Store, _ objectstorage.Binder) objectscopetest.Confined {
			return store.WithLayout(joined{base: store.Base()})
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := gatewaytest.LiveMinIO(t)
			proof := objectscopetest.Proof{Client: client, Base: "proof", Rounds: 400, Objects: 200, Seed: 20261121}
			proof.Open = func(binder objectstorage.Binder) (objectscopetest.Confined, error) {
				store, err := objectstorage.NewStore(client, proof.Base, binder)
				if err != nil {
					return nil, err
				}
				return defect(store, binder), nil
			}
			recorder := &recorder{TB: t}
			func() {
				defer func() {
					if recovered := recover(); recovered != nil && recovered != recorder {
						panic(recovered)
					}
				}()
				objectscopetest.Prove(recorder, proof)
			}()
			if recorder.failure == "" {
				t.Fatal("the proof passed on a store that does not hold the model")
			}
			t.Log(strings.SplitN(recorder.failure, "\n", 2)[0])
		})
	}
}

// The key convention keeps case, so the store must. Pinned here because a
// MinIO serving a directory of a case-insensitive file system would not.
func TestMinIOKeepsCaseAndEveryByteOfTheAlphabet(t *testing.T) {
	client, _ := gatewaytest.LiveMinIO(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	upper, lower := bound(strict("human:AMY", "Inst")), bound(strict("human:amy", "inst"))
	put(t, store, upper, objectstorage.Own("Report.PDF"), "upper")
	put(t, store, lower, objectstorage.Own("report.pdf"), "lower")
	put(t, store, lower, objectstorage.Own("Report.PDF"), "mixed")
	put(t, store, lower, objectstorage.Own("a_2fb/.hidden/é x/..."), "odd")
	for ctx, want := range map[context.Context]map[string]string{
		upper: {"Report.PDF": "upper"},
		lower: {"report.pdf": "lower", "Report.PDF": "mixed", "a_2fb/.hidden/é x/...": "odd"},
	} {
		for name, body := range want {
			if got := get(t, store, ctx, objectstorage.Own(name)); got != body {
				t.Errorf("%s holds %q, want %q", name, got, body)
			}
		}
		page, err := store.List(ctx, objectstorage.Own(""), objectstorage.ListOptions{})
		if err != nil || len(page.Objects) != len(want) || page.Skipped != 0 {
			t.Errorf("listed %d objects (%d skipped), want %d: %v", len(page.Objects), page.Skipped, len(want), err)
		}
	}
}

// A presigned URL is a capability for one key: it reads that object with no
// credential, a PUT writes that object and no other, and neither outlives its
// lifetime. What the PUT does not bind is pinned too.
func TestMinIOPresignedURLsReachOneObject(t *testing.T) {
	client, live := gatewaytest.LiveMinIO(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	amy, bob := bound(strict("human:amy", "inst")), bound(strict("human:bob", "inst"))
	put(t, store, amy, objectstorage.Own("a.txt"), "amy's")
	put(t, store, bob, objectstorage.Own("a.txt"), "bob's")

	signed, err := store.PresignGet(amy, objectstorage.Own("a.txt"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if body, err := fetch(signed.URL); err != nil || string(body) != "amy's" {
		t.Fatalf("the presigned URL served %q, %v", body, err)
	}
	if until := time.Until(signed.Expires); until > time.Minute+5*time.Second || until < 30*time.Second {
		t.Errorf("a URL asked for one minute expires in %s", until)
	}
	// The signature covers the path: the same URL aimed at Bob's object, or
	// at the bucket, is refused by the store.
	for _, tampered := range []string{
		strings.Replace(signed.URL, "human.amy", "human.bob", 1),
		strings.Replace(signed.URL, live.Prefix+"files/org/inst/human.amy/a.txt", "", 1),
	} {
		if tampered == signed.URL {
			t.Fatalf("the URL %s does not name the key", signed.URL)
		}
		if body, err := fetch(tampered); err == nil || !strings.Contains(err.Error(), "403") {
			t.Errorf("a tampered URL served %q, %v; want 403", body, err)
		}
	}
	// A GET URL does not write.
	if status := send(t, http.MethodPut, signed.URL, "overwritten", nil); status != http.StatusForbidden {
		t.Errorf("a PUT on a presigned GET: status %d, want 403", status)
	}

	upload, err := store.PresignPut(amy, objectstorage.Own("uploads/new.bin"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if status := send(t, http.MethodPut, upload.URL, "uploaded", upload.Headers); status != http.StatusOK {
		t.Fatalf("the presigned PUT: status %d", status)
	}
	if got := get(t, store, amy, objectstorage.Own("uploads/new.bin")); got != "uploaded" {
		t.Fatalf("the upload holds %q", got)
	}
	if status := send(t, http.MethodPut, strings.Replace(upload.URL, "human.amy", "human.bob", 1), "x", nil); status != http.StatusForbidden {
		t.Errorf("a presigned PUT aimed at another principal: status %d, want 403", status)
	}
	// Pinned, not wanted: the URL binds neither a size nor a content type, so
	// its holder may upload anything of any size until it expires.
	big := strings.Repeat("x", 1<<20)
	if status := send(t, http.MethodPut, upload.URL, big, map[string]string{"Content-Type": "application/x-anything"}); status != http.StatusOK {
		t.Errorf("a megabyte of another type on the presigned PUT: status %d; the gateway now binds something, update the design", status)
	}

	// An expired URL is refused.
	brief, err := store.PresignGet(amy, objectstorage.Own("a.txt"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	if body, err := fetch(brief.URL); err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("an expired URL served %q, %v; want 403", body, err)
	}
}

func send(t *testing.T, method, url, body string, headers map[string]string) int {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

// What holds without the Store, stated and not assumed: the gateway client
// reaches every key of every base, and the credential the gateway holds
// reaches the whole bucket. Today the Store is the only confinement.
func TestMinIOWithoutTheStoreNothingIsConfined(t *testing.T) {
	client, live := gatewaytest.LiveMinIO(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	put(t, store, bound(strict("human:bob", "inst")), objectstorage.Own("secret.txt"), "bob's")

	const key = "files/org/inst/human.bob/secret.txt"
	stream, err := client.Get(context.Background(), &storagev1.GetRequest{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	var body []byte
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("the gateway client was refused %s: %v", key, err)
		}
		body = append(body, frame.GetData()...)
	}
	if string(body) != "bob's" {
		t.Fatalf("the gateway client read %q", body)
	}
	root := live.Raw(t, live.AccessKey, live.SecretKey, "")
	object, err := root.GetObject(context.Background(), live.Bucket, live.Prefix+key, miniogo.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if content, err := io.ReadAll(object); err != nil || string(content) != "bob's" {
		t.Fatalf("the gateway's credential read %q, %v", content, err)
	}
}

// scoped asks the pinned MinIO for a temporary credential confined to one
// Binding by an inline session policy, and returns a client on the store
// itself that holds nothing else.
func scoped(t *testing.T, live gatewaytest.MinIO, binding objectstorage.Binding) *miniogo.Client {
	t.Helper()
	policy, err := objectstorage.S3SessionPolicy(live.Bucket, live.Prefix, "files", binding)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := miniocreds.NewSTSAssumeRole("http://"+live.Endpoint, miniocreds.STSAssumeRoleOptions{
		AccessKey: live.AccessKey, SecretKey: live.SecretKey, Policy: string(policy), DurationSeconds: 900, Location: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	issued, err := provider.GetWithContext(nil)
	if err != nil {
		t.Fatalf("AssumeRole with a session policy: %v", err)
	}
	if issued.SessionToken == "" || issued.AccessKeyID == live.AccessKey {
		t.Fatalf("AssumeRole returned no temporary credential: %+v", issued.AccessKeyID)
	}
	return live.Raw(t, issued.AccessKeyID, issued.SecretAccessKey, issued.SessionToken)
}

type rawCall struct {
	what    string
	allowed bool
	call    func(*miniogo.Client) error
}

// Storage-side enforcement, proved where it can be: a client on MinIO itself,
// past the gateway and past the Store, holding only a credential issued under
// the Binding's session policy, is refused outside the Binding by MinIO.
func TestMinIOSessionPolicyConfinesARawClient(t *testing.T) {
	client, live := gatewaytest.LiveMinIO(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	// Two partitions, two scopes, a principal whose name begins another's.
	for _, partition := range []string{"org", "org2"} {
		for _, scope := range []string{"inst", "inst2"} {
			job := privileged(scope)
			job.Partition = partition
			for _, principal := range []string{"human:amy", "human:amy2", "human:bob"} {
				put(t, store, bound(job), objectstorage.Of(principal, "a.txt"), partition+scope+principal)
			}
			put(t, store, bound(job), objectstorage.Shared("logo.png"), "logo")
			if _, err := store.PutContent(bound(job), scope, strings.NewReader("content-"+partition+scope), objectstorage.PutOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	codec, _ := objectkey.New("files")
	key := func(partition, scope string, slot objectkey.Slot, name string) string {
		physical, err := codec.Key(objectkey.Key{Partition: partition, Scope: scope, Slot: slot, Name: name})
		if err != nil {
			t.Fatal(err)
		}
		return live.Prefix + physical
	}
	amy, amy2, bob := objectkey.Principal("human:amy"), objectkey.Principal("human:amy2"), objectkey.Principal("human:bob")
	ctx := context.Background()
	read := func(physical string) func(*miniogo.Client) error {
		return func(raw *miniogo.Client) error {
			object, err := raw.GetObject(ctx, live.Bucket, physical, miniogo.GetObjectOptions{})
			if err != nil {
				return err
			}
			_, err = io.ReadAll(object)
			return err
		}
	}
	head := func(physical string) func(*miniogo.Client) error {
		return func(raw *miniogo.Client) error {
			_, err := raw.StatObject(ctx, live.Bucket, physical, miniogo.StatObjectOptions{})
			return err
		}
	}
	write := func(physical string) func(*miniogo.Client) error {
		return func(raw *miniogo.Client) error {
			_, err := raw.PutObject(ctx, live.Bucket, physical, strings.NewReader("raw"), 3, miniogo.PutObjectOptions{})
			return err
		}
	}
	remove := func(physical string) func(*miniogo.Client) error {
		return func(raw *miniogo.Client) error {
			return raw.RemoveObject(ctx, live.Bucket, physical, miniogo.RemoveObjectOptions{})
		}
	}
	copyTo := func(from, to string) func(*miniogo.Client) error {
		return func(raw *miniogo.Client) error {
			_, err := raw.CopyObject(ctx, miniogo.CopyDestOptions{Bucket: live.Bucket, Object: to}, miniogo.CopySrcOptions{Bucket: live.Bucket, Object: from})
			return err
		}
	}
	list := func(prefix string, recursive bool) func(*miniogo.Client) error {
		return func(raw *miniogo.Client) error {
			for object := range raw.ListObjects(ctx, live.Bucket, miniogo.ListObjectsOptions{Prefix: prefix, Recursive: recursive}) {
				if object.Err != nil {
					return object.Err
				}
				if !strings.HasPrefix(object.Key, prefix) {
					return fmt.Errorf("listing %q returned %q", prefix, object.Key)
				}
			}
			return nil
		}
	}
	multipart := func(physical string) func(*miniogo.Client) error {
		return func(raw *miniogo.Client) error {
			core := miniogo.Core{Client: raw}
			upload, err := core.NewMultipartUpload(ctx, live.Bucket, physical, miniogo.PutObjectOptions{})
			if err != nil {
				return err
			}
			return core.AbortMultipartUpload(ctx, live.Bucket, physical, upload)
		}
	}
	scopePrefix := live.Prefix + "files/org/inst/"
	digest := func(scope string) string {
		for object := range live.Raw(t, live.AccessKey, live.SecretKey, "").ListObjects(ctx, live.Bucket, miniogo.ListObjectsOptions{Prefix: live.Prefix + "files/org/" + scope + "/_sha256/"}) {
			return object.Key
		}
		t.Fatal("no content was seeded")
		return ""
	}

	strictAmy := strict("human:amy", "inst")
	reader := strictAmy
	reader.Access = objectstorage.Read
	sharer := strictAmy
	sharer.Shared = objectstorage.ReadWrite
	for name, test := range map[string]struct {
		binding objectstorage.Binding
		calls   []rawCall
	}{
		"strict": {strictAmy, []rawCall{
			{"read its own object", true, read(key("org", "inst", amy, "a.txt"))},
			{"head its own object", true, head(key("org", "inst", amy, "a.txt"))},
			{"write its own object", true, write(key("org", "inst", amy, "new.txt"))},
			{"delete its own object", true, remove(key("org", "inst", amy, "new.txt"))},
			{"start and abort a multipart upload of its own", true, multipart(key("org", "inst", amy, "big.bin"))},
			{"list its own prefix", true, list(scopePrefix+"human.amy/", true)},
			{"read a shared object", true, read(key("org", "inst", objectkey.Shared(), "logo.png"))},
			{"list the shared prefix", true, list(scopePrefix+"_shared/", true)},
			{"read content it can name", true, read(digest("inst"))},
			{"copy its own object to its own", true, copyTo(key("org", "inst", amy, "a.txt"), key("org", "inst", amy, "copy.txt"))},
			{"read another principal's object", false, read(key("org", "inst", bob, "a.txt"))},
			{"head another principal's object", false, head(key("org", "inst", bob, "a.txt"))},
			{"read the principal its name begins", false, read(key("org", "inst", amy2, "a.txt"))},
			{"write another principal's object", false, write(key("org", "inst", bob, "a.txt"))},
			{"delete another principal's object", false, remove(key("org", "inst", bob, "a.txt"))},
			{"start a multipart upload of another's", false, multipart(key("org", "inst", bob, "big.bin"))},
			{"copy from another principal's object", false, copyTo(key("org", "inst", bob, "a.txt"), key("org", "inst", amy, "stolen.txt"))},
			{"copy its own object over another's", false, copyTo(key("org", "inst", amy, "a.txt"), key("org", "inst", bob, "a.txt"))},
			{"list another principal's prefix", false, list(scopePrefix+"human.bob/", true)},
			{"list its own prefix without the separator", false, list(scopePrefix+"human.amy", true)},
			{"list the scope", false, list(scopePrefix, false)},
			{"list the content", false, list(scopePrefix+"_sha256/", true)},
			{"list the partition", false, list(live.Prefix+"files/org/", false)},
			{"list the base", false, list(live.Prefix+"files/", false)},
			{"list the bucket", false, list("", false)},
			{"write a shared object", false, write(key("org", "inst", objectkey.Shared(), "logo.png"))},
			{"delete a shared object", false, remove(key("org", "inst", objectkey.Shared(), "logo.png"))},
			{"delete content", false, remove(digest("inst"))},
			{"read its own name in another scope", false, read(key("org", "inst2", amy, "a.txt"))},
			{"read a shared object of another scope", false, read(key("org", "inst2", objectkey.Shared(), "logo.png"))},
			{"read its own name in another partition", false, read(key("org2", "inst", amy, "a.txt"))},
			{"write outside the base", false, write(live.Prefix + "elsewhere/x")},
			{"write outside the gateway's prefix", false, write("outside/x")},
		}},
		"strict, read only": {reader, []rawCall{
			{"read its own object", true, read(key("org", "inst", amy, "a.txt"))},
			{"list its own prefix", true, list(scopePrefix+"human.amy/", true)},
			{"write its own object", false, write(key("org", "inst", amy, "a.txt"))},
			{"delete its own object", false, remove(key("org", "inst", amy, "a.txt"))},
			{"copy its own object to its own", false, copyTo(key("org", "inst", amy, "a.txt"), key("org", "inst", amy, "copy2.txt"))},
			{"start a multipart upload of its own", false, multipart(key("org", "inst", amy, "big.bin"))},
			{"read another principal's object", false, read(key("org", "inst", bob, "a.txt"))},
		}},
		"strict, shared read-write": {sharer, []rawCall{
			{"write a shared object", true, write(key("org", "inst", objectkey.Shared(), "notes.txt"))},
			{"delete a shared object", true, remove(key("org", "inst", objectkey.Shared(), "notes.txt"))},
			{"write another principal's object", false, write(key("org", "inst", bob, "a.txt"))},
			{"write a shared object of another scope", false, write(key("org", "inst2", objectkey.Shared(), "notes.txt"))},
		}},
		"privileged": {privileged("inst"), []rawCall{
			{"read any principal's object", true, read(key("org", "inst", bob, "a.txt"))},
			{"write any principal's object", true, write(key("org", "inst", bob, "by-the-job.txt"))},
			{"delete any principal's object", true, remove(key("org", "inst", bob, "by-the-job.txt"))},
			{"copy between principals", true, copyTo(key("org", "inst", amy, "a.txt"), key("org", "inst", bob, "from-amy.txt"))},
			{"list the scope", true, list(scopePrefix, true)},
			{"list the content", true, list(scopePrefix+"_sha256/", true)},
			{"write a shared object", true, write(key("org", "inst", objectkey.Shared(), "by-the-job.txt"))},
			{"delete content", false, remove(digest("inst"))},
			{"read another scope", false, read(key("org", "inst2", bob, "a.txt"))},
			{"write another scope", false, write(key("org", "inst2", bob, "a.txt"))},
			{"list another scope", false, list(live.Prefix+"files/org/inst2/", true)},
			{"list the scope its name begins", false, list(live.Prefix+"files/org/inst", true)},
			{"list the partition", false, list(live.Prefix+"files/org/", false)},
			{"copy from another scope", false, copyTo(key("org", "inst2", bob, "a.txt"), key("org", "inst", bob, "stolen.txt"))},
			{"read another partition", false, read(key("org2", "inst", bob, "a.txt"))},
			{"list another partition", false, list(live.Prefix+"files/org2/inst/", true)},
			{"list the bucket", false, list("", false)},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			raw := scoped(t, live, test.binding)
			for _, call := range test.calls {
				err := call.call(raw)
				denied := miniogo.ToErrorResponse(err).Code == "AccessDenied"
				switch {
				case call.allowed && err != nil:
					t.Errorf("%s: %v, and the policy allows it", call.what, err)
				case !call.allowed && err == nil:
					t.Errorf("%s: MinIO allowed it", call.what)
				case !call.allowed && !denied:
					t.Errorf("%s: %v, want AccessDenied", call.what, err)
				}
			}
		})
	}
}

// Why a directory segment carries a mark. On this MinIO a key that is also
// the prefix of other keys hides them from every listing, though each still
// reads. Pinned on the store itself; then the same names through the Store,
// which all list, and an erasure that leaves nothing even under a hand-built
// pair of the hidden kind.
func TestMinIOHidesWhatIsUnderAnObjectAndTheConventionNeverWritesOne(t *testing.T) {
	client, live := gatewaytest.LiveMinIO(t)
	ctx := context.Background()
	root := live.Raw(t, live.AccessKey, live.SecretKey, "")
	names := []string{"a", "a/b", "a/b/c"}
	hand := live.Prefix + "files/org/inst/human.zed/"
	for _, name := range names {
		if _, err := root.PutObject(ctx, live.Bucket, hand+name, strings.NewReader(name), int64(len(name)), miniogo.PutObjectOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	var listed []string
	for object := range root.ListObjects(ctx, live.Bucket, miniogo.ListObjectsOptions{Prefix: hand, Recursive: true}) {
		listed = append(listed, strings.TrimPrefix(object.Key, hand))
	}
	if len(listed) != 1 || listed[0] != "a" {
		t.Errorf("MinIO listed %v of the hand-built keys %v; it no longer hides keys under an object, and the design says it does", listed, names)
	}

	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	amy := bound(strict("human:amy", "inst"))
	for _, name := range names {
		put(t, store, amy, objectstorage.Own(name), name)
	}
	page, err := store.List(amy, objectstorage.Own(""), objectstorage.ListOptions{})
	if err != nil || len(page.Objects) != len(names) {
		t.Fatalf("the store listed %d of %d names, %v", len(page.Objects), len(names), err)
	}
	maintain, _ := objectstorage.NewMaintenance(client, "files")
	for principal, want := range map[string]int{"human:amy": 3, "human:zed": 3} {
		erased, err := maintain.ErasePrincipal(ctx, "org", principal)
		if err != nil || erased.Objects != want || len(erased.Failed) != 0 {
			t.Errorf("erasing %s removed %d objects, want %d: %v %v", principal, erased.Objects, want, erased.Failed, err)
		}
	}
	for object := range root.ListObjects(ctx, live.Bucket, miniogo.ListObjectsOptions{Prefix: live.Prefix + "files/", Recursive: true}) {
		t.Errorf("%s survived the erasure", object.Key)
	}
	for _, name := range names {
		if _, err := root.StatObject(ctx, live.Bucket, hand+name, miniogo.StatObjectOptions{}); err == nil {
			t.Errorf("%s%s survived the erasure, hidden", hand, name)
		}
	}
}

// On MinIO itself: content a writer finds already stored is modified again,
// so a sweep in the gap before the writer's row keeps it; the sweep's remove
// refuses content written again since it was listed; and the delete is made
// inside the caller's decision.
func TestMinIOReusedContentIsYoungAgainForTheSweep(t *testing.T) {
	// MinIO reports a HEAD's modification time to the second.
	const grace = 3 * time.Second
	client, _ := gatewaytest.LiveMinIO(t)
	const base = "sweep" // the gateway of this test has a prefix of its own
	store, err := objectstorage.NewStore(client, base, fromContext)
	if err != nil {
		t.Fatal(err)
	}
	maintain, err := objectstorage.NewMaintenance(client, base)
	if err != nil {
		t.Fatal(err)
	}
	amy := bound(strict("human:amy", "inst"))
	write := func(body string) objectstorage.Ref {
		t.Helper()
		info, err := store.PutContent(amy, "inst", strings.NewReader(body), objectstorage.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return info.Ref
	}
	nothing := func(string) (bool, error) { return false, nil }

	reused, raced, abandoned := write("reused"), write("raced"), write("abandoned")
	time.Sleep(grace + 2*time.Second)

	// The writer finds its bytes there; its row is not committed.
	if again := write("reused"); again != reused {
		t.Fatalf("the same bytes are %v and %v", reused, again)
	}
	var removedInside bool
	swept, err := maintain.SweepContentWith(context.Background(), "org", "inst", grace, func(digest string, remove func() error) error {
		switch digest {
		case reused.Name:
			t.Errorf("the sweep offered content written again within the grace")
			return nil
		case raced.Name:
			// Written again after the sweep listed it, before it removes.
			write("raced")
			if err := remove(); !errors.Is(err, objectstorage.ErrContentInUse) {
				t.Errorf("removing content written again since the listing: %v", err)
			}
			return nil
		}
		if err := remove(); err != nil {
			return err
		}
		_, err := store.Head(amy, objectstorage.Content(digest).In("inst"))
		removedInside = errors.Is(err, objectstorage.ErrNotFound)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if swept.Objects != 1 || !removedInside {
		t.Fatalf("the sweep removed %d object(s), inside the decision: %v", swept.Objects, removedInside)
	}
	for name, ref := range map[string]objectstorage.Ref{"reused": reused, "raced": raced} {
		if got := get(t, store, amy, ref); got != name {
			t.Fatalf("%s content reads %q", name, got)
		}
	}
	if _, err := store.Head(amy, abandoned); !errors.Is(err, objectstorage.ErrNotFound) {
		t.Fatalf("abandoned content survived: %v", err)
	}

	// Left for the grace with still no reference, the rest is swept.
	time.Sleep(grace + 2*time.Second)
	if swept, err := maintain.SweepContent(context.Background(), "org", "inst", grace, nothing); err != nil || swept.Objects != 2 {
		t.Fatalf("the sweep of abandoned content removed %d, %v", swept.Objects, err)
	}
}
