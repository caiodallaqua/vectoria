package kv

import (
	"context"
	"log/slog"

	"github.com/dgraph-io/badger/v3"
)

type Contract interface {
	CloseDB() error
	Add(key, val []byte) error
	Get(key []byte) (val []byte, err error)
	getValuesWithPrefix(prefix []byte) (values [][]byte, err error)
}

type KV struct {
	db *badger.DB
}

func New(path string) (*KV, error) {
	var inMemory bool
	if len(path) == 0 {
		inMemory = true
	}

	opts := badger.DefaultOptions(path).WithInMemory(inMemory)

	db, err := badger.Open(opts)
	if err != nil {
		logErr(err, "New")
		return nil, err
	}

	return &KV{
		db: db,
	}, nil
}

func (stg *KV) CloseDB() error {
	if err := stg.db.Close(); err != nil {
		logErr(err, "CloseDB")
		return err
	}

	return nil
}

func (stg *KV) Add(key, val []byte) error {
	err := stg.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
	if err != nil {
		logErr(err, "Add")
		return err
	}

	return nil
}

func (stg *KV) Get(key []byte) (val []byte, err error) {
	err = stg.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		logErr(err, "Get")
		return nil, err
	}

	return val, nil
}

func (stg *KV) getValuesWithPrefix(prefix []byte) (values [][]byte, err error) {
	var (
		val  []byte
		item *badger.Item
	)

	err = stg.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item = it.Item()
			val, err = item.ValueCopy(nil)
			if err != nil {
				return err
			}

			if err != nil {
				return err
			}

			values = append(values, val)
		}

		return nil
	})

	if err != nil {
		logErr(err, "getValuesWithPrefix")
		return nil, err
	}

	return values, nil
}

func logErr(err error, trace string) {
	slog.LogAttrs(
		context.TODO(),
		slog.LevelError,
		err.Error(),
		slog.String("trace", "vectoria:src:internal:kv:"+trace),
	)
}
