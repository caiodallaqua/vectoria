// Package vectoria provides an embedded vector database for simple use cases.
package vectoria

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/mastrasec/vectoria/internal/lsh"
	"github.com/mastrasec/vectoria/internal/storage"
	"golang.org/x/exp/maps"
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

func (sm *safeMap) len() int {
	return len(sm.items)
}

// =================================== API ===================================

func New(path string, opts ...Options) (db *DB, err error) {
	stg, err := storage.New(path)
	if err != nil {
		return nil, err
	}

	db = newDB(stg)

	for _, opt := range opts {
		if err = opt(db); err != nil {
			return nil, err
		}
	}

	// Handle case when no index is passed to opts
	if len(db.indexRef.items) == 0 {
		if err = WithIndexLSH()(db); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func WithLog() Options {
	return func(db *DB) error {
		if _, ok := db.called["WithLog"]; ok {
			return new(withLogDuplicationError)
		}
		db.called["WithLog"] = true

		db.log = true

		return nil
	}
}

func WithIndexLSH(confs ...*LSHConfig) Options {
	return func(db *DB) error {
		funcName := "WithIndexLSH"

		if _, ok := db.called[funcName]; ok {
			return new(withIndexLSHDuplicationError)
		}
		db.called[funcName] = true

		if len(confs) == 0 {
			confs = append(confs, new(LSHConfig))
		}

		for _, conf := range confs {
			if conf == nil {
				continue
			}

			if conf.IndexName == "" {
				conf.IndexName = uuid.NewString()
			}

			locality, err := lsh.New(db.stg, conf.NumRounds, conf.NumHyperPlanes, conf.SpaceDim)
			if err != nil {
				return err
			}

			newIndex := &lshIndex{
				numRounds:      conf.NumRounds,
				numHyperPlanes: conf.NumHyperPlanes,
				spaceDim:       conf.SpaceDim,
				locality:       locality,
			}

			db.indexRef.add(conf.IndexName, newIndex)
		}

		return nil
	}
}

func (db *DB) Indexes() []string {
	return maps.Keys(db.indexRef.items)
}

// TODO: rollback on err
func (db *DB) Add(itemID string, itemVec []float64, indexNames ...string) error {
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

// =================================== INDEXES ===================================

type index interface {
	add(itemID string, itemVec []float64) error
	get(queryVec []float64, threshold float64, k uint32) (ids []string, err error)
}

type LSHConfig struct {
	IndexName string

	// Number of rounds. More rounds improve quality, but also adds computation overhead.
	// It must be at least 1.
	// If invalid value is given, default value is used.
	NumRounds uint32

	// Number of hyperplanes to split the space on. It must be at least 1.
	// If invalid value is given, default value is used.
	NumHyperPlanes uint32

	// Dimension of the space (vector length). It must be at least 2.
	// If invalid value is given, default value is used.
	SpaceDim uint32
}

type lshIndex struct {
	numRounds      uint32
	numHyperPlanes uint32
	spaceDim       uint32
	locality       *lsh.LSH
}

func (l *lshIndex) add(itemID string, itemVec []float64) error {
	return l.locality.Add(itemID, itemVec)
}

func (l *lshIndex) get(queryVec []float64, threshold float64, k uint32) (ids []string, err error) {
	return l.locality.Get(queryVec, threshold, k)
}

// =================================== ERRORS ===================================

type withLogDuplicationError struct{}

func (e *withLogDuplicationError) Error() string {
	return "WithLog() duplication."
}

type withIndexLSHDuplicationError struct{}

func (e *withIndexLSHDuplicationError) Error() string {
	return "WithIndexLSH() duplication."
}

type indexDoesNotExistError struct {
	name string
}

func (e *indexDoesNotExistError) Error() string {
	return fmt.Sprintf("index %s does not exist.", e.name)
}
