package storage

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	// Overwrites the logger to keep tests outputs clean
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	slog.SetDefault(logger)

	os.Exit(m.Run())
}

// TODO: Add tests for nil storage receivers

func TestNew(t *testing.T) {
	path := "/tmp/badger"
	stg, err := New(path)
	assert.NoError(t, err)
	assert.NotNil(t, stg)
	assert.DirExists(t, path)
}

func TestCloseDB(t *testing.T) {
	stg := setup(t)

	err := stg.CloseDB()
	assert.NoError(t, err)
	assert.True(t, stg.db.IsClosed())
}

func setup(t *testing.T) *Storage {
	stg, err := New("")
	assert.NoError(t, err)
	assert.NotNil(t, stg)

	return stg
}
