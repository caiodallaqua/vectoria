// Package vectoria provides an embedded vector database for simple use cases.
package vectoria

import (
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/caiodallaqua/vectoria/internal/lsh"
	"github.com/caiodallaqua/vectoria/internal/storage"
	"github.com/google/uuid"
)

type DB struct {
	log bool
	stg storage.Contract

	// index ID -> index pointer
	indexRef safeMap

	// Keeps track of option functions called in Open to avoid duplication
	called map[string]bool
}

type Options func(*DB) error

func newDB(stg storage.Contract) *DB {
	db := &DB{
		stg:    stg,
		called: make(map[string]bool),
	}

	db.indexRef.make()

	return db
}

type safeMap struct {
	mu    sync.RWMutex
	items map[string]index
}

func (sm *safeMap) make() {
	sm.items = make(map[string]index)
}

func (sm *safeMap) add(key string, value index) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.items[key] = value
}

func (sm *safeMap) get(key string) (index, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	idx, ok := sm.items[key] // Compiler does not allow direct return

	return idx, ok
}

func (sm *safeMap) del(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.items, key)
}

func (sm *safeMap) keyExists(key string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	_, exists := sm.items[key]

	return exists
}

func (sm *safeMap) len() int {
	return len(sm.items)
}

// =================================== API ===================================

type DBConfig struct {
	Path string
	log  bool

	LSH []LSHConfig
}

func (conf LSHConfig) indexName() string {
	return conf.IndexName
}

func New(config DBConfig) (db *DB, err error) {
	stg, err := storage.New(config.Path)
	if err != nil {
		return nil, err
	}

	db = newDB(stg)

	if err := db.addLSH(config.LSH...); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) addLSH(configs ...LSHConfig) error {
	for i, config := range configs {
		if exists := db.indexExists(config.IndexName); exists {
			db.addLSHRollback(configs[:i+1]...)
			return &indexAlreadyExistsError{config.IndexName}
		}

		if config.IndexName == "" {
			config.IndexName = uuid.NewString()
		}

		locality, err := lsh.New(config.IndexName, db.stg, config.NumRounds, config.NumHyperPlanes, config.SpaceDim)
		if err != nil {
			return err
		}

		db.indexRef.add(config.IndexName, &lshIndex{locality: locality})
	}

	return nil
}

func (db *DB) addLSHRollback(configs ...LSHConfig) {
	for _, config := range configs {
		db.indexRef.del(config.IndexName)
	}
}

func (db *DB) Indexes() []string {
	return slices.Collect(maps.Keys(db.indexRef.items))
}

func (db *DB) NumIndexes() uint32 {
	return uint32(len(db.Indexes()))
}

// TODO: rollback on err
func (db *DB) Add(itemID string, itemVec []float64, indexNames ...string) error {
	if db.NumIndexes() == 0 {
		return &dbHasNoIndexError{}
	}

	if len(indexNames) == 0 {
		indexNames = db.Indexes()
	}

	for _, indexName := range indexNames {
		idx, ok := db.indexRef.get(indexName)
		if !ok {
			return &indexDoesNotExistError{name: indexName}
		}

		if err := idx.add(itemID, itemVec); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Get(queryVec []float64, threshold float64, k uint32, indexNames ...string) (res map[string][]string, err error) {
	if len(indexNames) == 0 {
		indexNames = db.Indexes()
	}

	res = make(map[string][]string, len(indexNames))

	for _, indexName := range indexNames {
		idx, ok := db.indexRef.get(indexName)
		if !ok {
			return nil, &indexDoesNotExistError{name: indexName}
		}

		ids, err := idx.get(queryVec, threshold, k)
		if err != nil {
			return nil, err
		}

		res[indexName] = ids
	}

	return res, nil
}

func (db *DB) clean(itemID string, indexNames ...string) {
	// TODO: delete all
}

func (db *DB) indexExists(indexName string) bool {
	return db.indexRef.keyExists(indexName)
}

// =================================== INDEXES ===================================

type index interface {
	add(itemID string, itemVec []float64) error
	get(queryVec []float64, threshold float64, k uint32) (ids []string, err error)
	info() map[string]any
}

type LSHConfig struct {
	IndexName string `json:"index_name"`

	// Number of rounds. More rounds improve quality, but also adds computation overhead.
	// It must be at least 1.
	// If invalid value is given, default value is used.
	NumRounds uint32 `json:"num_rounds"`

	// Number of hyperplanes to split the space on. It must be at least 1.
	// If invalid value is given, default value is used.
	NumHyperPlanes uint32 `json:"num_hyper_planes"`

	// Dimension of the space (vector length). It must be at least 2.
	// If invalid value is given, default value is used.
	SpaceDim uint32 `json:"space_dim"`
}

type lshIndex struct {
	locality *lsh.LSH
}

func (l *lshIndex) add(itemID string, itemVec []float64) error {
	return l.locality.Add(itemID, itemVec)
}

func (l *lshIndex) get(queryVec []float64, threshold float64, k uint32) (ids []string, err error) {
	return l.locality.Get(queryVec, threshold, k)
}

func (l *lshIndex) info() map[string]any {
	return l.locality.Info()
}

// =================================== ERRORS ===================================

type indexAlreadyExistsError struct {
	name string
}

func (e *indexAlreadyExistsError) Error() string {
	return fmt.Sprintf("index %s already exists.", e.name)
}

type indexDoesNotExistError struct {
	name string
}

func (e *indexDoesNotExistError) Error() string {
	return fmt.Sprintf("index %s does not exist.", e.name)
}

type dbHasNoIndexError struct{}

func (e *dbHasNoIndexError) Error() string {
	return "database has no index."
}
