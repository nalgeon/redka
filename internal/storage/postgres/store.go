package postgres

import "github.com/nalgeon/redka/internal/storage"

// Store is the SQLite-backed [storage.Storage] implementation.
type Store struct{}

// New returns a SQLite storage backend.
func New() storage.Storage {
	return Store{}
}

func (Store) Keys() storage.KeyStorage       { return keyStore{} }
func (Store) Strings() storage.StringStorage { return stringStore{} }
func (Store) Hashes() storage.HashStorage    { return hashStore{} }
func (Store) Lists() storage.ListStorage     { return listStore{} }
func (Store) Sets() storage.SetStorage       { return setStore{} }
func (Store) ZSets() storage.ZSetStorage     { return zsetStore{} }
