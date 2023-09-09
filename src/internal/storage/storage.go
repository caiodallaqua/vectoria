package storage

import (
	"context"
	"log/slog"

	"github.com/dgraph-io/badger/v3"
)

type Contract interface {
	CloseDB() error
	Add(data map[string][]byte) error
	Get(key string) (val []byte, err error)
	GetWithPrefix(prefix string) (values [][]byte, err error)
	Del(keys ...string) error
}

type Storage struct {
	db *badger.DB
}

func New(path string) (*Storage, error) {
	var inMemory bool
	if len(path) == 0 {
		inMemory = true
	}

	opts := badger.DefaultOptions(path).WithInMemory(inMemory).WithLogger(nil)

	db, err := badger.Open(opts)
	if err != nil {
		logErr(err, "New")
		return nil, err
	}

	return &Storage{
		db: db,
	}, nil
}

func (stg *Storage) CloseDB() error {
	if err := stg.db.Close(); err != nil {
		logErr(err, "CloseDB")
		return err
	}

	return nil
}

func (stg *Storage) Add(data map[string][]byte) error {
	err := stg.db.Update(func(txn *badger.Txn) (err error) {
		for key, val := range data {
			if err = txn.Set([]byte(key), val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		logErr(err, "Add")
		return err
	}

	return nil
}

func (stg *Storage) Get(key string) (val []byte, err error) {
	err = stg.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
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

func (stg *Storage) GetWithPrefix(prefix string) (values [][]byte, err error) {
	var (
		val           []byte
		item          *badger.Item
		encodedPrefix = []byte(prefix)
	)

	err = stg.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(encodedPrefix); it.ValidForPrefix(encodedPrefix); it.Next() {
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

func (stg *Storage) Del(keys ...string) error {
	err := stg.db.Update(func(txn *badger.Txn) (err error) {
		for _, key := range keys {
			if err = txn.Delete([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		logErr(err, "Del")
		return err
	}

	return nil
}

func logErr(err error, trace string) {
	slog.LogAttrs(
		context.TODO(),
		slog.LevelError,
		err.Error(),
		slog.String("trace", "vectoria:src:internal:kv:"+trace),
	)
}
