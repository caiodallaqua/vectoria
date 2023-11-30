package storage

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/brianvoe/gofakeit"
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
	path := t.TempDir()
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

func TestKeyExists(t *testing.T) {
	key := gofakeit.Name()

	testCases := []struct {
		name       string
		data       map[string][]byte
		shouldFind bool
	}{
		{
			name: "key found",
			data: map[string][]byte{
				key: []byte(""),
			},
			shouldFind: true,
		},
		{
			name: "key not found with suffix",
			data: map[string][]byte{
				key + "-suffix": []byte(""),
			},
			shouldFind: false,
		},
		{
			name: "key not found with prefix",
			data: map[string][]byte{
				"prefix-" + key: []byte(""),
			},
			shouldFind: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stg, err := New("")
			assert.NoError(t, err)
			assert.NotNil(t, stg)

			err = stg.Add(tc.data)
			assert.NoError(t, err)

			exists, err := stg.KeyExists(key)
			assert.NoError(t, err)

			if tc.shouldFind {
				assert.True(t, exists)
			} else {
				assert.False(t, exists)
			}
		})
	}
}

func setup(t *testing.T) *Storage {
	stg, err := New(t.TempDir())
	assert.NoError(t, err)
	assert.NotNil(t, stg)

	return stg
}
