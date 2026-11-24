package objectstorage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
)

const (
	defaultPresignLimit = 15 * time.Minute
	maxPresignLimit     = 7 * 24 * time.Hour
	putChunkBytes       = 256 << 10
	stagingCleanup      = 10 * time.Second
)

// layout is what a Store needs of the key convention. It is objectkey.Codec;
// it is an interface only so the confinement proof can be shown to fail on a
// store whose keys are built another way.
type layout interface {
	Key(objectkey.Key) (string, error)
	Parse(string) (objectkey.Key, error)
	ScopePrefix(partition, scope string) (string, error)
	DirectoryPrefix(partition, scope string, slot objectkey.Slot, directory string) (string, error)
}

type config struct {
	readerIssuer    ReaderSessionIssuer
	gatewaySessions bool
	presignLimit    time.Duration
}

// Option configures a Store or a Maintenance.
type Option func(*config) error

// WithPresignLimit changes the longest lifetime a presigned URL may be asked
// for. The default is 15 minutes; a presigned URL is a capability nothing
// here can take back.
func WithPresignLimit(limit time.Duration) Option {
	return func(configuration *config) error {
		if limit <= 0 || limit > maxPresignLimit {
			return fmt.Errorf("a presign limit is between 0 and %s", maxPresignLimit)
		}
		configuration.presignLimit = limit
		return nil
	}
}

// ReaderSessionIssuer supplies a short-lived, backend-confined credential
// for one scope of the binding the Store verified.
type ReaderSessionIssuer interface {
	Issue(context.Context, string, Binding, string) (*storagev0.Binding, error)
}

// WithReaderSessions requires a confined session on Get, Head and listings.
func WithReaderSessions(issuer ReaderSessionIssuer) Option {
	return func(c *config) error {
		if isNil(issuer) {
			return errors.New("scoped reads require a session issuer")
		}
		c.readerIssuer = issuer
		return nil
	}
}

// WithGatewaySessions makes every call of the Store name its scope to the
// gateway, which then makes the call with a short-lived credential of the
// store's own, confined to that scope: the partition and the scope of the
// object, under this Store's base. The Store holds no credential and issues
// none. A key outside the scope is refused by the object store itself, so
// the Store's own checks stop being the only thing between a caller and the
// rest of the bucket.
//
// Each call runs under one of the scope's three sessions, and no call can
// use another's:
//
//	read         Get, Head, List, ListScope, PresignGet
//	ingest       Put, the upload half of PutContent, PresignPut, PresignPost
//	maintenance  Delete, Copy, and the promotion half of PutContent (the
//	             check for existing content, the copy into place, and the
//	             removal of the staged upload)
//
// The gateway must offer scoped sessions (SOS_SCOPED_SESSIONS=allow or
// require) and grant this Store's base those modes; otherwise every call is
// refused, never served with the gateway's own credential. It excludes
// WithReaderSessions: there the caller issues the credential, here nobody
// outside the gateway holds one.
func WithGatewaySessions() Option {
	return func(c *config) error {
		c.gatewaySessions = true
		return nil
	}
}

// Store is the request-time boundary of one confined object store. It owns
// the gateway client and exposes it to nobody.
type Store struct {
	client storagev0.ObjectStorageClient
	keys   layout
	base   string
	binder Binder
	config config
}

// NewStore binds a gateway client to one base at a trusted composition root.
// The base is the store's own name inside the bucket and prefix the gateway
// was given: one segment of lowercase letters, digits and hyphens. A Store
// never addresses anything outside it, and two stores with different bases
// never address the same object.
func NewStore(client storagev0.ObjectStorageClient, base string, binder Binder, options ...Option) (*Store, error) {
	if isNil(client) {
		return nil, errors.New("object storage client is required")
	}
	if isNil(binder) {
		return nil, errors.New("object storage binder is required")
	}
	codec, err := objectkey.New(base)
	if err != nil {
		return nil, err
	}
	configuration := config{presignLimit: defaultPresignLimit}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&configuration); err != nil {
			return nil, err
		}
	}
	if configuration.gatewaySessions && configuration.readerIssuer != nil {
		return nil, errors.New("a store uses gateway sessions or its own reader sessions, not both")
	}
	return &Store{client: client, keys: codec, base: base, binder: binder, config: configuration}, nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return reflected.IsNil()
	default:
		return false
	}
}

// in is the scope a call names to the gateway, or nil when the Store was not
// built with gateway sessions.
func (s *Store) in(binding Binding, scope string, mode storagev0.ScopeMode) *storagev0.Scope {
	if !s.config.gatewaySessions {
		return nil
	}
	return &storagev0.Scope{Base: s.base, Partition: binding.Partition, Scope: scope, Mode: mode}
}

const (
	readSession        = storagev0.ScopeMode_SCOPE_MODE_READ
	ingestSession      = storagev0.ScopeMode_SCOPE_MODE_INGEST
	maintenanceSession = storagev0.ScopeMode_SCOPE_MODE_MAINTENANCE
)

// Base is the base this Store is confined to.
func (s *Store) Base() string { return s.base }

func (s *Store) bind(ctx context.Context) (Binding, error) {
	if s == nil || isNil(s.binder) || ctx == nil {
		return Binding{}, ErrNoBinding
	}
	binding, err := s.binder.Bind(ctx)
	if err != nil {
		if errors.Is(err, ErrUnauthenticated) || errors.Is(err, ErrUnauthorized) {
			return Binding{}, err
		}
		return Binding{}, errors.Join(ErrNoBinding, err)
	}
	return binding.Normalize()
}

// target is one resolved reference: where it is and what the key is.
type target struct {
	scope string
	slot  objectkey.Slot
	key   string
}

func (s *Store) resolve(binding Binding, ref Ref, may func(objectkey.Slot) error) (target, error) {
	scope, slot, err := binding.place(ref)
	if err != nil {
		return target{}, err
	}
	if err := may(slot); err != nil {
		return target{}, err
	}
	key, err := s.keys.Key(objectkey.Key{Partition: binding.Partition, Scope: scope, Slot: slot, Name: ref.Name})
	if err != nil {
		return target{}, err
	}
	return target{scope: scope, slot: slot, key: key}, nil
}

// Info describes one object, by logical reference.
type Info struct {
	// Ref is fully qualified: its scope, and its principal unless it is shared
	// or content.
	Ref         Ref
	Size        int64
	ETag        string
	ContentType string
	Modified    time.Time
	VersionID   string
	Metadata    map[string]string
}

func refOf(key objectkey.Key) Ref {
	return Ref{
		Scope: key.Scope, Principal: key.Slot.Reference(),
		Shared: key.Slot.IsShared(), Content: key.Slot.IsContent(), Name: key.Name,
	}
}

func infoOf(key objectkey.Key, object *storagev0.ObjectInfo) Info {
	info := Info{Ref: refOf(key)}
	if object == nil {
		return info
	}
	info.Size, info.ETag, info.ContentType = object.GetSize(), object.GetEtag(), object.GetContentType()
	info.VersionID, info.Metadata = object.GetVersionId(), object.GetUserMetadata()
	if milliseconds := object.GetLastModifiedUnixMs(); milliseconds > 0 {
		info.Modified = time.UnixMilli(milliseconds)
	}
	return info
}

// translate turns the gateway's answer into this package's errors and keeps
// the original beside them.
func translate(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return errors.Join(ErrNotFound, err)
	case codes.AlreadyExists, codes.FailedPrecondition:
		return errors.Join(ErrExists, err)
	default:
		return err
	}
}

// PutOptions are the properties of a write.
type PutOptions struct {
	ContentType string
	// Metadata is stored with the object and returned to every reader of it.
	Metadata map[string]string
	// IfAbsent refuses the write with ErrExists when the object is there.
	IfAbsent bool
	// Size is a hint; zero means unknown.
	Size int64
}

// Put writes an object the binding may write.
func (s *Store) Put(ctx context.Context, ref Ref, body io.Reader, options PutOptions) (Info, error) {
	binding, err := s.bind(ctx)
	if err != nil {
		return Info{}, err
	}
	at, err := s.resolve(binding, ref, binding.mayWrite)
	if err != nil {
		return Info{}, err
	}
	size, _, result, err := s.send(ctx, s.in(binding, at.scope, ingestSession), at.key, body, options, false)
	if err != nil {
		return Info{}, err
	}
	return Info{
		Ref:  refOf(objectkey.Key{Scope: at.scope, Slot: at.slot, Name: ref.Name}),
		Size: size, ETag: result.GetEtag(), VersionID: result.GetVersionId(), ContentType: options.ContentType,
	}, nil
}

// send streams a body to one key, hashing it on the way when asked.
func (s *Store) send(ctx context.Context, scope *storagev0.Scope, key string, body io.Reader, options PutOptions, hash bool) (int64, string, *storagev0.PutResult, error) {
	if body == nil {
		return 0, "", nil, errors.New("object store: a write needs a body")
	}
	upload, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := s.client.Put(upload)
	if err != nil {
		return 0, "", nil, err
	}
	header := &storagev0.PutHeader{Key: key, ContentType: options.ContentType, UserMetadata: options.Metadata, TotalSize: -1, Scope: scope}
	if options.Size > 0 {
		header.TotalSize = options.Size
	}
	if options.IfAbsent {
		header.IfNoneMatch = "*"
	}
	if err := stream.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: header}}); err != nil {
		return 0, "", nil, translate(streamError(stream, err))
	}
	hasher := sha256.New()
	var size int64
	buffer := make([]byte, putChunkBytes)
	for {
		read, readErr := body.Read(buffer)
		if read > 0 {
			if hash {
				hasher.Write(buffer[:read])
			}
			size += int64(read)
			if err := stream.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: buffer[:read]}}); err != nil {
				return 0, "", nil, translate(streamError(stream, err))
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, "", nil, readErr
		}
	}
	result, err := stream.CloseAndRecv()
	if err != nil {
		return 0, "", nil, translate(err)
	}
	return size, hex.EncodeToString(hasher.Sum(nil)), result, nil
}

// streamError recovers the status a failed Send hides behind io.EOF.
func streamError(stream storagev0.ObjectStorage_PutClient, err error) error {
	if !errors.Is(err, io.EOF) {
		return err
	}
	if _, closeErr := stream.CloseAndRecv(); closeErr != nil {
		return closeErr
	}
	return err
}

// PutContent writes a body under the digest of its bytes, in the content slot
// of a scope in reach, and returns that digest as the reference's name. The
// same bytes written twice are one object. The body is streamed to a staging
// key of the binding's own principal while it is hashed, then copied in place
// by the store unless the content is already there.
func (s *Store) PutContent(ctx context.Context, scope string, body io.Reader, options PutOptions) (Info, error) {
	binding, err := s.bind(ctx)
	if err != nil {
		return Info{}, err
	}
	place, _, err := binding.place(Ref{Scope: scope})
	if err != nil {
		return Info{}, err
	}
	if binding.Access != ReadWrite {
		return Info{}, ErrReadOnly
	}
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return Info{}, err
	}
	own := objectkey.Principal(binding.Principal)
	staging, err := s.keys.Key(objectkey.Key{Partition: binding.Partition, Scope: place, Slot: own, Name: hex.EncodeToString(token), Staging: true})
	if err != nil {
		return Info{}, err
	}
	// The staged object is scratch whatever happens next, and its removal
	// must not depend on a context that may already be cancelled.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), stagingCleanup)
		defer cancel()
		_, _ = s.client.Delete(cleanup, &storagev0.DeleteRequest{Key: staging, Scope: s.in(binding, place, maintenanceSession)})
	}()
	size, digest, _, err := s.send(ctx, s.in(binding, place, ingestSession), staging, body, PutOptions{ContentType: options.ContentType, Metadata: options.Metadata, Size: options.Size}, true)
	if err != nil {
		return Info{}, err
	}
	content := objectkey.Key{Partition: binding.Partition, Scope: place, Slot: objectkey.Content(), Name: digest}
	key, err := s.keys.Key(content)
	if err != nil {
		return Info{}, err
	}
	info := Info{Ref: refOf(content), Size: size, ContentType: options.ContentType}
	// Promotion is maintenance: it reads what ingest wrote, which an ingest
	// session may not.
	promotion := s.in(binding, place, maintenanceSession)
	existing, err := s.client.Stat(ctx, &storagev0.StatRequest{Key: key, Scope: promotion})
	switch {
	case err == nil:
		info.ETag, info.VersionID = existing.GetInfo().GetEtag(), existing.GetInfo().GetVersionId()
		return info, nil
	case status.Code(err) != codes.NotFound:
		return Info{}, err
	}
	copied, err := s.client.Copy(ctx, &storagev0.CopyRequest{SourceKey: staging, DestKey: key, Scope: promotion})
	if err != nil {
		return Info{}, translate(err)
	}
	info.ETag, info.VersionID = copied.GetEtag(), copied.GetVersionId()
	return info, nil
}

func (s *Store) readerSession(ctx context.Context, binding Binding, scope string) (*storagev0.Binding, error) {
	if s.config.readerIssuer == nil {
		return nil, nil
	}
	session, err := s.config.readerIssuer.Issue(ctx, s.base, binding, scope)
	if err == nil && session == nil {
		return nil, errors.New("reader issuer returned no session")
	}
	return session, err
}

// Get opens an object the binding may read. The caller closes the reader.
func (s *Store) Get(ctx context.Context, ref Ref) (io.ReadCloser, Info, error) {
	binding, err := s.bind(ctx)
	if err != nil {
		return nil, Info{}, err
	}
	at, err := s.resolve(binding, ref, binding.mayRead)
	if err != nil {
		return nil, Info{}, err
	}
	session, err := s.readerSession(ctx, binding, at.scope)
	if err != nil {
		return nil, Info{}, err
	}
	return open(ctx, s.client, session, s.in(binding, at.scope, readSession), at.key, objectkey.Key{Scope: at.scope, Slot: at.slot, Name: ref.Name})
}

func open(ctx context.Context, client storagev0.ObjectStorageClient, session *storagev0.Binding, scope *storagev0.Scope, physical string, key objectkey.Key) (io.ReadCloser, Info, error) {
	streaming, cancel := context.WithCancel(ctx)
	stream, err := client.Get(streaming, &storagev0.GetRequest{Key: physical, Binding: session, Scope: scope})
	if err != nil {
		cancel()
		return nil, Info{}, translate(err)
	}
	// The first frame is the header; a missing object surfaces here.
	first, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, Info{}, translate(err)
	}
	return &reader{stream: stream, cancel: cancel, pending: first.GetData()}, infoOf(key, first.GetHeader().GetInfo()), nil
}

type reader struct {
	stream  storagev0.ObjectStorage_GetClient
	cancel  context.CancelFunc
	pending []byte
	err     error
}

func (r *reader) Read(into []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		frame, err := r.stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				err = translate(err)
			}
			r.err = err
			return 0, err
		}
		r.pending = frame.GetData()
	}
	copied := copy(into, r.pending)
	r.pending = r.pending[copied:]
	return copied, nil
}

func (r *reader) Close() error {
	r.cancel()
	return nil
}

// Head describes an object the binding may read.
func (s *Store) Head(ctx context.Context, ref Ref) (Info, error) {
	binding, err := s.bind(ctx)
	if err != nil {
		return Info{}, err
	}
	at, err := s.resolve(binding, ref, binding.mayRead)
	if err != nil {
		return Info{}, err
	}
	session, err := s.readerSession(ctx, binding, at.scope)
	if err != nil {
		return Info{}, err
	}
	header, err := s.client.Stat(ctx, &storagev0.StatRequest{Key: at.key, Binding: session, Scope: s.in(binding, at.scope, readSession)})
	if err != nil {
		return Info{}, translate(err)
	}
	return infoOf(objectkey.Key{Scope: at.scope, Slot: at.slot, Name: ref.Name}, header.GetInfo()), nil
}

// Delete removes an object the binding may write. Removing what is not there
// is not an error.
func (s *Store) Delete(ctx context.Context, ref Ref) error {
	binding, err := s.bind(ctx)
	if err != nil {
		return err
	}
	at, err := s.resolve(binding, ref, binding.mayWrite)
	if err != nil {
		return err
	}
	if _, err := s.client.Delete(ctx, &storagev0.DeleteRequest{Key: at.key, Scope: s.in(binding, at.scope, maintenanceSession)}); err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	return nil
}

// CopyOptions are the properties of a copy.
type CopyOptions struct {
	// IfAbsent refuses the copy with ErrExists when the destination is there.
	IfAbsent bool
}

// Copy copies, inside the store, an object the binding may read to a
// reference it may write. Both ends are checked: a copy is a read of its
// source.
func (s *Store) Copy(ctx context.Context, from, to Ref, options CopyOptions) (Info, error) {
	binding, err := s.bind(ctx)
	if err != nil {
		return Info{}, err
	}
	source, err := s.resolve(binding, from, binding.mayRead)
	if err != nil {
		return Info{}, fmt.Errorf("copy source: %w", err)
	}
	destination, err := s.resolve(binding, to, binding.mayWrite)
	if err != nil {
		return Info{}, fmt.Errorf("copy destination: %w", err)
	}
	// One session is one scope: the store cannot be asked to copy across two.
	if s.config.gatewaySessions && source.scope != destination.scope {
		return Info{}, fmt.Errorf("%w: a copy under gateway sessions stays inside one scope", ErrOutsideReach)
	}
	request := &storagev0.CopyRequest{SourceKey: source.key, DestKey: destination.key, Scope: s.in(binding, destination.scope, maintenanceSession)}
	if options.IfAbsent {
		request.IfNoneMatch = "*"
	}
	copied, err := s.client.Copy(ctx, request)
	if err != nil {
		return Info{}, translate(err)
	}
	return Info{
		Ref:  refOf(objectkey.Key{Scope: destination.scope, Slot: destination.slot, Name: to.Name}),
		ETag: copied.GetEtag(), VersionID: copied.GetVersionId(),
	}, nil
}

// ListOptions page a listing.
type ListOptions struct {
	// Token continues a listing from Page.Next.
	Token string
	// Limit is the most objects a page holds; zero means the gateway's own.
	Limit int
}

// Page is one page of a listing.
type Page struct {
	Objects []Info
	// Next continues the listing; empty means it is complete.
	Next string
	// Skipped counts keys under the listed prefix that are not objects the
	// binding may see by name: uploads in flight, and keys the convention did
	// not write. The latter is what the layout gate reports.
	Skipped int
}

// List enumerates one slot the binding may list: its own, the shared one, or
// under a privileged binding another principal's or the content slot. in.Name
// is a directory of whole name segments, and empty means the slot. Every
// object is returned by logical reference, and only after its key has been
// read back and found to be where the binding may look.
func (s *Store) List(ctx context.Context, in Ref, options ListOptions) (Page, error) {
	binding, err := s.bind(ctx)
	if err != nil {
		return Page{}, err
	}
	scope, slot, err := binding.place(in)
	if err != nil {
		return Page{}, err
	}
	if err := binding.mayList(slot); err != nil {
		return Page{}, err
	}
	prefix, err := s.keys.DirectoryPrefix(binding.Partition, scope, slot, in.Name)
	if err != nil {
		return Page{}, err
	}
	return s.list(ctx, binding, scope, prefix, options, func(key objectkey.Key) bool {
		return key.Scope == scope && key.Slot == slot
	})
}

// ListScope enumerates every slot of a scope, for a privileged binding.
func (s *Store) ListScope(ctx context.Context, scope string, options ListOptions) (Page, error) {
	binding, err := s.bind(ctx)
	if err != nil {
		return Page{}, err
	}
	place, _, err := binding.place(Ref{Scope: scope})
	if err != nil {
		return Page{}, err
	}
	if binding.Mode != Privileged {
		return Page{}, ErrForeignPrincipal
	}
	prefix, err := s.keys.ScopePrefix(binding.Partition, place)
	if err != nil {
		return Page{}, err
	}
	return s.list(ctx, binding, place, prefix, options, func(key objectkey.Key) bool { return key.Scope == place })
}

func (s *Store) list(ctx context.Context, binding Binding, scope, prefix string, options ListOptions, within func(objectkey.Key) bool) (Page, error) {
	session, err := s.readerSession(ctx, binding, scope)
	if err != nil {
		return Page{}, err
	}
	listed, err := s.client.List(ctx, &storagev0.ListRequest{Binding: session, Scope: s.in(binding, scope, readSession), Prefix: prefix, PageToken: options.Token, Limit: int32(max(options.Limit, 0))})
	if err != nil {
		return Page{}, translate(err)
	}
	page := Page{Next: listed.GetNextPageToken()}
	for _, object := range listed.GetObjects() {
		key, err := s.keys.Parse(object.GetKey())
		// The prefix confined the request. This confines the answer: a key is
		// returned only if it reads back as an object of this partition, in
		// the place that was listed, that the binding may read.
		if err != nil || key.Staging || key.Partition != binding.Partition || !within(key) || binding.mayRead(key.Slot) != nil {
			page.Skipped++
			continue
		}
		page.Objects = append(page.Objects, infoOf(key, object))
	}
	return page, nil
}

// Presigned is a URL that reads or writes exactly one object, for as long as
// it lives, with no credential. Whoever holds it holds that.
type Presigned struct {
	URL    string
	Method string
	// Headers are headers the request must carry as given: a bound content
	// type is one.
	Headers map[string]string
	// Fields, for a POST, are the form fields to send before the file, which
	// goes last under the name "file".
	Fields  map[string]string
	Expires time.Time
}

// UploadBounds are what a presigned upload binds beyond its key and its
// lifetime. The object store checks them when the URL is used.
type UploadBounds struct {
	// ContentType is the exact content type the upload must carry.
	ContentType string
	// MaxBytes is the most bytes a POST may carry; MinBytes the fewest.
	MaxBytes int64
	MinBytes int64
}

// PresignGet signs a URL that reads one object the binding may read.
func (s *Store) PresignGet(ctx context.Context, ref Ref, lifetime time.Duration) (Presigned, error) {
	return s.presign(ctx, ref, lifetime, storagev0.PresignMethod_PRESIGN_METHOD_GET, UploadBounds{})
}

// PresignPut signs a URL that writes one object the binding may write. The
// gateway signs the key and the lifetime and nothing else: neither a size nor
// a content type is bound to the URL. PresignPutTyped binds the type and
// PresignPost the size.
func (s *Store) PresignPut(ctx context.Context, ref Ref, lifetime time.Duration) (Presigned, error) {
	return s.presign(ctx, ref, lifetime, storagev0.PresignMethod_PRESIGN_METHOD_PUT, UploadBounds{})
}

// PresignPutTyped signs a URL that writes one object the binding may write,
// and only with the given content type: the upload sends the returned header
// as is, and the store refuses any other. It bounds no size; PresignPost does.
func (s *Store) PresignPutTyped(ctx context.Context, ref Ref, lifetime time.Duration, contentType string) (Presigned, error) {
	if contentType == "" {
		return Presigned{}, errors.New("object store: a typed upload names its content type")
	}
	return s.presign(ctx, ref, lifetime, storagev0.PresignMethod_PRESIGN_METHOD_PUT, UploadBounds{ContentType: contentType})
}

// PresignPost signs a form upload of one object the binding may write, whose
// size, and content type when given, the store checks. It is the one upload
// URL that bounds how much is written. A gateway whose store cannot sign one
// refuses; it never signs an unbounded URL in its place.
func (s *Store) PresignPost(ctx context.Context, ref Ref, lifetime time.Duration, bounds UploadBounds) (Presigned, error) {
	if bounds.MaxBytes <= 0 || bounds.MinBytes < 0 || bounds.MinBytes > bounds.MaxBytes {
		return Presigned{}, errors.New("object store: a form upload states the most bytes it may carry")
	}
	return s.presign(ctx, ref, lifetime, storagev0.PresignMethod_PRESIGN_METHOD_POST, bounds)
}

func (s *Store) presign(ctx context.Context, ref Ref, lifetime time.Duration, method storagev0.PresignMethod, bounds UploadBounds) (Presigned, error) {
	binding, err := s.bind(ctx)
	if err != nil {
		return Presigned{}, err
	}
	may, verb, mode := binding.mayRead, "GET", readSession
	switch method {
	case storagev0.PresignMethod_PRESIGN_METHOD_PUT:
		may, verb, mode = binding.mayWrite, "PUT", ingestSession
	case storagev0.PresignMethod_PRESIGN_METHOD_POST:
		may, verb, mode = binding.mayWrite, "POST", ingestSession
	}
	at, err := s.resolve(binding, ref, may)
	if err != nil {
		return Presigned{}, err
	}
	// The gateway reads a missing lifetime as its maximum, which is days.
	if lifetime < time.Second || lifetime > s.config.presignLimit {
		return Presigned{}, fmt.Errorf("%w: asked for %s, the limit is %s", ErrPresignExpiry, lifetime, s.config.presignLimit)
	}
	signed, err := s.client.Presign(ctx, &storagev0.PresignRequest{
		Key: at.key, Method: method, ExpirySeconds: int64(lifetime / time.Second), Scope: s.in(binding, at.scope, mode),
		ContentType: bounds.ContentType, ContentLengthMax: bounds.MaxBytes, ContentLengthMin: bounds.MinBytes,
	})
	if err != nil {
		return Presigned{}, translate(err)
	}
	return Presigned{URL: signed.GetUrl(), Method: verb, Headers: signed.GetHeaders(), Fields: signed.GetFormFields(), Expires: time.UnixMilli(signed.GetExpiresAtUnixMs())}, nil
}
