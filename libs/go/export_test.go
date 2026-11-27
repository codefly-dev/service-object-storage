package objectstorage

import storagev1 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v1"

// Layout is the key convention a Store is built on, exported to the tests so
// they can break it.
type Layout = layout

// WithLayout returns a copy of the Store that builds its keys another way.
func (s *Store) WithLayout(keys Layout) *Store {
	broken := *s
	broken.keys = keys
	return &broken
}

// WithBinder returns a copy of the Store that resolves its Binding another way.
func (s *Store) WithBinder(binder Binder) *Store {
	broken := *s
	broken.binder = binder
	return &broken
}

// Client is the gateway client the Store never hands out.
func (s *Store) Client() storagev1.ObjectStorageClient { return s.client }
