package objectstorage_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	objectstorage "github.com/codefly-dev/service-object-storage/libs/go"
	"github.com/codefly-dev/service-object-storage/libs/go/internal/gatewaytest"
)

// A writer that finds its bytes already stored has decided to reference
// them, and has not recorded the reference yet. If the object kept the age
// of its first write, a sweep running in that gap would see old content that
// nothing references and remove it under the writer. PutContent therefore
// leaves the content modified now, which the sweep's grace protects.
func TestAWriterThatFindsExistingContentIsNotSweptUnderIt(t *testing.T) {
	const grace = 400 * time.Millisecond
	client := gatewaytest.Memory(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	maintain, err := objectstorage.NewMaintenance(client, "files")
	if err != nil {
		t.Fatal(err)
	}
	amy := bound(strict("human:amy", "inst"))
	nothing := func(string) (bool, error) { return false, nil }

	// Stored long ago by a version that is gone: old, and referenced by nothing.
	first, err := store.PutContent(amy, "inst", strings.NewReader("the same bytes"), objectstorage.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(grace + 200*time.Millisecond)

	// A writer stores the same bytes and finds them there. Its row is not
	// committed yet, so the application still answers "not referenced".
	again, err := store.PutContent(amy, "inst", strings.NewReader("the same bytes"), objectstorage.PutOptions{})
	if err != nil || again.Ref != first.Ref {
		t.Fatalf("the second write: %+v, %v", again.Ref, err)
	}
	swept, err := maintain.SweepContent(context.Background(), "org", "inst", grace, nothing)
	if err != nil {
		t.Fatal(err)
	}
	if swept.Objects != 0 {
		t.Fatalf("the sweep removed %d object(s) a writer had just decided to reuse", swept.Objects)
	}
	if got := get(t, store, amy, again.Ref); got != "the same bytes" {
		t.Fatalf("the reused content reads %q", got)
	}

	// Left alone for the grace, with still no reference, it is swept.
	time.Sleep(grace + 200*time.Millisecond)
	swept, err = maintain.SweepContent(context.Background(), "org", "inst", grace, nothing)
	if err != nil || swept.Objects != 1 {
		t.Fatalf("the sweep of abandoned content removed %d, %v", swept.Objects, err)
	}
	if _, err := store.Head(amy, first.Ref); !errors.Is(err, objectstorage.ErrNotFound) {
		t.Fatalf("abandoned content survived: %v", err)
	}
}

func readStaged(t *testing.T, store *objectstorage.Store, ctx context.Context, scope, id string) (string, error) {
	t.Helper()
	body, size, err := store.GetStaged(ctx, scope, id)
	if err != nil {
		return "", err
	}
	defer body.Close()
	content, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(content)) {
		t.Fatalf("staged part %s reported %d bytes and held %d", id, size, len(content))
	}
	return string(content), nil
}

// A staged part is an object of the staging slot of the binding's own
// principal, in a scope it reaches. No listing shows it, no other principal
// reads, replaces or removes it, not even a privileged one, and it is
// addressed by its id alone: there is no principal to name.
func TestStagedPartsAreInvisibleToListAndReadableOnlyByTheirPrincipal(t *testing.T) {
	client := gatewaytest.Memory(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	amy, bob := bound(strict("human:amy", "inst", "other")), bound(strict("human:bob", "inst"))
	job := bound(privileged("inst"))
	put(t, store, amy, objectstorage.Own("a").In("inst"), "an object")

	for part, body := range map[string]string{"upload1part0001": "first ", "upload1part0002": "second"} {
		size, err := store.PutStaged(amy, "inst", part, strings.NewReader(body), objectstorage.PutOptions{})
		if err != nil || size != int64(len(body)) {
			t.Fatalf("put staged %s: %d, %v", part, size, err)
		}
	}
	if got, err := readStaged(t, store, amy, "inst", "upload1part0001"); err != nil || got != "first " {
		t.Fatalf("its principal read %q, %v", got, err)
	}

	// Invisible to every listing, of its principal and of a privileged job.
	page, err := store.List(amy, objectstorage.Own("").In("inst"), objectstorage.ListOptions{})
	if err != nil || len(page.Objects) != 1 || page.Objects[0].Ref.Name != "a" || page.Skipped != 2 {
		t.Fatalf("the principal's listing: %+v, %v", page, err)
	}
	scope, err := store.ListScope(job, "inst", objectstorage.ListOptions{})
	if err != nil || len(scope.Objects) != 1 {
		t.Fatalf("a privileged listing shows %d objects, %v", len(scope.Objects), err)
	}
	// Not an object by any name: a reference cannot spell the staging slot.
	for _, name := range []string{"_staging/upload1part0001", "upload1part0001"} {
		if _, err := store.Head(amy, objectstorage.Own(name).In("inst")); err == nil {
			t.Fatalf("a staged part is an object named %q", name)
		}
	}

	// Another principal's same id is another key: bob and the privileged job
	// find nothing, and what they stage or remove under it is their own.
	for who, ctx := range map[string]context.Context{"another principal": bob, "a privileged job": job} {
		if _, err := readStaged(t, store, ctx, "inst", "upload1part0001"); !errors.Is(err, objectstorage.ErrNotFound) {
			t.Fatalf("%s read amy's staged part: %v", who, err)
		}
		if _, err := store.PutStaged(ctx, "inst", "upload1part0001", strings.NewReader("theirs"), objectstorage.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := store.DeleteStaged(ctx, "inst", "upload1part0001"); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := readStaged(t, store, amy, "inst", "upload1part0001"); err != nil || got != "first " {
		t.Fatalf("after the others wrote and removed their own, amy reads %q, %v", got, err)
	}
	// The same principal in another scope has another staging slot.
	if _, err := readStaged(t, store, amy, "other", "upload1part0001"); !errors.Is(err, objectstorage.ErrNotFound) {
		t.Fatalf("a staged part crossed scopes: %v", err)
	}

	// Refused before the gateway: a scope out of reach, a binding that may
	// not write, and an id that is not a staging id.
	if _, err := store.PutStaged(bob, "other", "x1", strings.NewReader("x"), objectstorage.PutOptions{}); !errors.Is(err, objectstorage.ErrOutsideReach) {
		t.Fatalf("a scope out of reach: %v", err)
	}
	if _, _, err := store.GetStaged(bob, "other", "x1"); !errors.Is(err, objectstorage.ErrOutsideReach) {
		t.Fatalf("a scope out of reach: %v", err)
	}
	reader := strict("human:amy", "inst")
	reader.Access = objectstorage.Read
	if _, err := store.PutStaged(bound(reader), "inst", "x1", strings.NewReader("x"), objectstorage.PutOptions{}); !errors.Is(err, objectstorage.ErrReadOnly) {
		t.Fatalf("a read-only binding staged: %v", err)
	}
	if err := store.DeleteStaged(bound(reader), "inst", "upload1part0001"); !errors.Is(err, objectstorage.ErrReadOnly) {
		t.Fatalf("a read-only binding removed a staged part: %v", err)
	}
	if got, err := readStaged(t, store, bound(reader), "inst", "upload1part0002"); err != nil || got != "second" {
		t.Fatalf("a read-only binding reads its own staged part: %q, %v", got, err)
	}
	for _, id := range []string{"", "Upper", "with/slash", "../a", "dot.ted", strings.Repeat("a", 65)} {
		if _, err := store.PutStaged(amy, "inst", id, strings.NewReader("x"), objectstorage.PutOptions{}); !errors.Is(err, objectstorage.ErrInvalidName) {
			t.Fatalf("staging id %q: %v", id, err)
		}
		if _, _, err := store.GetStaged(amy, "inst", id); !errors.Is(err, objectstorage.ErrInvalidName) {
			t.Fatalf("staging id %q: %v", id, err)
		}
	}
	// A part is written once when asked.
	if _, err := store.PutStaged(amy, "inst", "upload1part0001", strings.NewReader("again"), objectstorage.PutOptions{IfAbsent: true}); !errors.Is(err, objectstorage.ErrExists) {
		t.Fatalf("a part written twice: %v", err)
	}

	// Removed by its principal; removing what is gone is not an error. The
	// staging sweep reclaims whatever an abandoned upload left.
	if err := store.DeleteStaged(amy, "inst", "upload1part0001"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteStaged(amy, "inst", "upload1part0001"); err != nil {
		t.Fatal(err)
	}
	if _, err := readStaged(t, store, amy, "inst", "upload1part0001"); !errors.Is(err, objectstorage.ErrNotFound) {
		t.Fatalf("a removed part reads: %v", err)
	}
	maintain, _ := objectstorage.NewMaintenance(client, "files")
	if swept, err := maintain.SweepStaging(context.Background(), -time.Second); err != nil || swept.Objects != 1 {
		t.Fatalf("the staging sweep removed %d, %v", swept.Objects, err)
	}
	if got := get(t, store, amy, objectstorage.Own("a").In("inst")); got != "an object" {
		t.Fatalf("the sweep touched an object: %q", got)
	}
}

// SweepContentWith hands each candidate to the caller with the means to
// remove it, so the caller's recheck and the delete happen inside one
// critical section of the caller's own (a lock a writer takes too). The
// delete is done when remove returns, not after the decision.
func TestSweepContentWithHoldsTheDecisionAcrossTheDelete(t *testing.T) {
	const grace = 400 * time.Millisecond
	ctx := context.Background()
	client := gatewaytest.Memory(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	maintain, err := objectstorage.NewMaintenance(client, "files")
	if err != nil {
		t.Fatal(err)
	}
	amy := bound(strict("human:amy", "inst", "other"))
	content := func(scope, body string) objectstorage.Info {
		t.Helper()
		info, err := store.PutContent(amy, scope, strings.NewReader(body), objectstorage.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return info
	}
	exists := func(ref objectstorage.Ref) bool {
		t.Helper()
		_, err := store.Head(amy, ref)
		if err != nil && !errors.Is(err, objectstorage.ErrNotFound) {
			t.Fatal(err)
		}
		return err == nil
	}
	abandoned, kept, reused := content("inst", "abandoned"), content("inst", "kept"), content("inst", "reused")
	elsewhere := content("other", "abandoned")
	time.Sleep(grace + 200*time.Millisecond)
	young := content("inst", "young")

	// The caller's lock: held for the whole of each decision.
	var lock sync.Mutex
	locked := false
	var offered []string
	var late func() error
	swept, err := maintain.SweepContentWith(ctx, "org", "inst", grace, func(digest string, remove func() error) error {
		lock.Lock()
		locked = true
		defer func() { locked = false; lock.Unlock() }()
		offered = append(offered, digest)
		switch digest {
		case kept.Ref.Name:
			// Referenced: the caller does not remove, and that is all it does.
			return nil
		case reused.Ref.Name:
			// A writer that got in before the lock was taken: it found the
			// bytes and reused them. The removal the sweeper then asks for
			// is refused, because the content is no longer old.
			content("inst", "reused")
			if err := remove(); !errors.Is(err, objectstorage.ErrContentInUse) {
				t.Errorf("removing content a writer had just reused: %v", err)
			}
			return nil
		}
		late = remove
		if !exists(objectstorage.Content(digest).In("inst")) {
			t.Errorf("%s was removed before the caller decided", digest)
		}
		if err := remove(); err != nil {
			return err
		}
		// The delete is done here, with the caller's lock still held.
		if !locked || exists(objectstorage.Content(digest).In("inst")) {
			t.Errorf("%s was still there when remove returned inside the decision", digest)
		}
		return remove() // removing what is gone is not an error
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(offered)
	want := []string{abandoned.Ref.Name, kept.Ref.Name, reused.Ref.Name}
	sort.Strings(want)
	if strings.Join(offered, ",") != strings.Join(want, ",") {
		t.Fatalf("the sweep offered %v, want every old digest of the scope and no young one: %v", offered, want)
	}
	if swept.Objects != 1 || swept.Bytes != int64(len("abandoned")) || len(swept.Failed) != 0 {
		t.Fatalf("the sweep reports %+v", swept)
	}
	for name, ref := range map[string]objectstorage.Ref{"referenced": kept.Ref, "reused": reused.Ref, "young": young.Ref, "another scope's": elsewhere.Ref} {
		if !exists(ref) {
			t.Fatalf("%s content was removed", name)
		}
	}
	if exists(abandoned.Ref) {
		t.Fatal("abandoned content survived")
	}
	// Outside its decision a remove does nothing: the lock is gone.
	content("inst", "abandoned")
	if err := late(); err == nil || !exists(abandoned.Ref) {
		t.Fatalf("a remove kept past its decision still removes: %v", err)
	}

	// A decision that fails stops the sweep, and its error is the sweep's.
	time.Sleep(grace + 200*time.Millisecond)
	failed := errors.New("the database is gone")
	calls := 0
	_, err = maintain.SweepContentWith(ctx, "org", "inst", grace, func(string, func() error) error {
		calls++
		return failed
	})
	if !errors.Is(err, failed) || calls != 1 {
		t.Fatalf("a failed decision: %v after %d call(s)", err, calls)
	}
	if !exists(kept.Ref) || !exists(young.Ref) {
		t.Fatal("a failed sweep removed content")
	}
	if _, err := maintain.SweepContentWith(ctx, "org", "inst", grace, nil); err == nil {
		t.Fatal("a sweep with nobody to decide ran")
	}
}

// application is what a consumer holds beside the object store: its rows, its
// intents, and the one lock its writers and its sweeper both take. A database
// does this with a table and an advisory lock keyed on partition, scope and
// digest; a mutex and two sets are the same protocol.
type application struct {
	lock    sync.Mutex
	intents map[string]int
	rows    map[string]int
}

func newApplication() *application {
	return &application{intents: map[string]int{}, rows: map[string]int{}}
}

// intend records, under the lock, that a writer is about to reference a
// digest. It is committed before the writer touches the object store.
func (a *application) intend(digest string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.intents[digest]++
}

// commit turns an intent into a row.
func (a *application) commit(digest string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.intents[digest]--
	a.rows[digest]++
}

func (a *application) forget(digest string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.rows[digest]--
}

// decide is the sweeper's half: the recheck and the delete under the lock.
func (a *application) decide(digest string, remove func() error) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.intents[digest] > 0 || a.rows[digest] > 0 {
		return nil
	}
	return remove()
}

// The grace is a margin. This is the protocol that needs none: the writer
// records an intent under a lock before it stores, and the sweeper rechecks
// and deletes under the same lock. Every sweep here runs with no grace at
// all, so nothing but the intent and the lock protects the bytes.
func TestAnIntentAndALockKeepReusedContentFromASweepWithNoGrace(t *testing.T) {
	const everything = -time.Hour // older than a cutoff in the future: every digest is offered
	ctx := context.Background()
	client := gatewaytest.Memory(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	maintain, err := objectstorage.NewMaintenance(client, "files")
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
	exists := func(ref objectstorage.Ref) bool {
		t.Helper()
		_, err := store.Head(amy, ref)
		if err != nil && !errors.Is(err, objectstorage.ErrNotFound) {
			t.Fatal(err)
		}
		return err == nil
	}

	// Without an intent, and without a grace, the writer loses: it stored its
	// bytes, has no row yet, and the sweep removes them. This is the race.
	app := newApplication()
	unprotected := write("no intent")
	if swept, err := maintain.SweepContentWith(ctx, "org", "inst", everything, app.decide); err != nil || swept.Objects != 1 {
		t.Fatalf("the sweep of content with no intent removed %d, %v", swept.Objects, err)
	}
	if exists(unprotected) {
		t.Fatal("the control did not race: content with no intent and no row survived a sweep with no grace")
	}

	// With the intent recorded first, the same sweep in the same gap keeps
	// them, whether the bytes were there before or not.
	old := write("reused")
	if swept, err := maintain.SweepContentWith(ctx, "org", "inst", everything, func(string, func() error) error { return nil }); err != nil || swept.Objects != 0 {
		t.Fatalf("a sweep that keeps everything removed %d, %v", swept.Objects, err)
	}
	app.intend(old.Name)
	reused := write("reused") // finds the bytes there
	if reused != old {
		t.Fatalf("the same bytes are %v and %v", old, reused)
	}
	if swept, err := maintain.SweepContentWith(ctx, "org", "inst", everything, app.decide); err != nil || swept.Objects != 0 {
		t.Fatalf("the sweep removed %d object(s) a writer intended to reference, %v", swept.Objects, err)
	}
	app.commit(old.Name)
	if !exists(old) {
		t.Fatal("content was swept between a writer's intent and its row")
	}

	// A writer that arrives while the sweeper is deciding waits for the lock.
	// The delete is done before it gets it, so its PutContent writes the
	// bytes again instead of trusting bytes that are about to go.
	app.forget(old.Name)
	intended, written := make(chan struct{}), make(chan objectstorage.Ref, 1)
	started := false
	swept, err := maintain.SweepContentWith(ctx, "org", "inst", everything, func(digest string, remove func() error) error {
		return app.decide(digest, func() error {
			if digest != old.Name || started {
				return remove()
			}
			started = true
			go func() {
				app.intend(old.Name) // blocks: the sweeper holds the lock
				close(intended)
				info, err := store.PutContent(amy, "inst", strings.NewReader("reused"), objectstorage.PutOptions{})
				if err != nil {
					t.Error(err)
				}
				written <- info.Ref
			}()
			select {
			case <-intended:
				t.Error("a writer recorded its intent while the sweeper held the lock")
			case <-time.After(100 * time.Millisecond):
			}
			if err := remove(); err != nil {
				return err
			}
			if exists(old) {
				t.Error("the content was still there when remove returned under the lock")
			}
			return nil
		})
	})
	if err != nil || swept.Objects != 1 {
		t.Fatalf("the sweep removed %d, %v", swept.Objects, err)
	}
	if ref := <-written; ref != old || !exists(old) {
		t.Fatalf("the writer that waited for the lock has no bytes: %v", ref)
	}
	if swept, err := maintain.SweepContentWith(ctx, "org", "inst", everything, app.decide); err != nil || swept.Objects != 0 {
		t.Fatalf("the next sweep removed %d object(s) under an intent, %v", swept.Objects, err)
	}
	app.commit(old.Name)

	// Writers and a sweeper at once: a writer that has committed its row
	// always finds its bytes.
	var writers sync.WaitGroup
	stop := make(chan struct{})
	sweeping := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				sweeping <- nil
				return
			default:
			}
			if _, err := maintain.SweepContentWith(ctx, "org", "inst", everything, app.decide); err != nil {
				sweeping <- err
				return
			}
		}
	}()
	bodies := []string{"shared one", "shared two"}
	for writer := 0; writer < 4; writer++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for round := 0; round < 25; round++ {
				body := bodies[(writer+round)%len(bodies)]
				digest := sha256Hex(body)
				app.intend(digest)
				info, err := store.PutContent(amy, "inst", strings.NewReader(body), objectstorage.PutOptions{})
				if err != nil || info.Ref.Name != digest {
					t.Errorf("writer %d: %v, %v", writer, info.Ref, err)
					return
				}
				app.commit(digest)
				if _, err := store.Head(amy, info.Ref); err != nil {
					t.Errorf("writer %d committed a row for %s and its bytes are gone: %v", writer, digest, err)
					return
				}
				app.forget(digest)
			}
		}()
	}
	writers.Wait()
	close(stop)
	if err := <-sweeping; err != nil {
		t.Fatal(err)
	}
}

func sha256Hex(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}
