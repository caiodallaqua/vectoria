package vectoria

import (
	"io"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/mastrasec/vectoria/internal/storage"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slog"
)

func TestMain(m *testing.M) {
	// Overwrites the logger to keep tests outputs clean
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	slog.SetDefault(logger)

	os.Exit(m.Run())
}

func TestNew(t *testing.T) {
	testCases := []struct {
		name           string
		opts           []Options
		wantNumIndexes int
		err            error
	}{
		{
			name:           "no options",
			opts:           []Options{},
			wantNumIndexes: 1,
			err:            nil,
		},
		{
			name: "WithLog",
			opts: []Options{
				WithLog(),
			},
			wantNumIndexes: 1,
			err:            nil,
		},
		{
			name: "WithLog WithIndexLSH",
			opts: []Options{
				WithLog(),
				WithIndexLSH(),
			},
			wantNumIndexes: 1,
			err:            nil,
		},
		{
			name: "WithLog WithIndexLSH",
			opts: []Options{
				WithLog(),
				WithIndexLSH(),
				WithIndexLSH(),
			},
			wantNumIndexes: 0,
			err:            new(withIndexLSHDuplicationError),
		},
		{
			name: "WithLog WithIndexLSH",
			opts: []Options{
				WithLog(),
				WithLog(),
				WithIndexLSH(),
			},
			wantNumIndexes: 0,
			err:            new(withLogDuplicationError),
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				db, err := New("", tc.opts...)
				assert.IsType(t, err, tc.err)

				if db != nil {
					assert.Equal(t, db.indexRef.len(), tc.wantNumIndexes)
				}
			},
		)
	}
}

func TestWithLog(t *testing.T) {
	db := newDB(nil)
	f := WithLog()

	err := f(db)
	assert.NoError(t, err)
	assert.True(t, db.log)
	assert.True(t, db.called["WithLog"])
}

func TestWithIndexLSH(t *testing.T) {
	testCases := []struct {
		name           string
		confs          []*LSHConfig
		wantNumIndexes int
	}{
		{
			// Creates a default LSH index if no args are passed.
			name:           "no args",
			confs:          []*LSHConfig{},
			wantNumIndexes: 1,
		},
		{
			// Creates a default LSH index to overwrite zero values.
			name:           "one arg: empty",
			confs:          []*LSHConfig{{}},
			wantNumIndexes: 1,
		},
		{
			// Nil args are ignored. This is a protection against misuse.
			name:           "two args: empty, nil",
			confs:          []*LSHConfig{{}, nil},
			wantNumIndexes: 1,
		},
		{
			name: "one arg: non-empty",
			confs: []*LSHConfig{{
				NumRounds:      12,
				NumHyperPlanes: 20,
				SpaceDim:       100,
			}},
			wantNumIndexes: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				stg, err := storage.New("")
				assert.NoError(t, err)

				db := newDB(stg)

				f := WithIndexLSH(tc.confs...)
				err = f(db)
				assert.NoError(t, err)

				assert.Equal(t, db.indexRef.len(), tc.wantNumIndexes)
			},
		)
	}
}

func TestAdd(t *testing.T) {
	testCases := []struct {
		name       string
		opts       []Options
		indexNames []string
		itemID     string
		itemVec    []float64
		err        error
	}{
		{
			name:       "no options, all indexes",
			opts:       []Options{},
			indexNames: []string{},
			itemID:     uuid.NewString(),
			itemVec:    []float64{1, 2},
			err:        nil,
		},
		{
			name:       "no options, wrong index",
			opts:       []Options{},
			indexNames: []string{"wrong-index-name"},
			itemID:     uuid.NewString(),
			itemVec:    []float64{},
			err:        new(indexDoesNotExistError),
		},
		{
			name: "empty WithIndexLSH, all indexes",
			opts: []Options{
				WithIndexLSH(),
			},
			indexNames: []string{},
			itemID:     uuid.NewString(),
			itemVec:    []float64{1, 2},
			err:        nil,
		},
		{
			name: "empty WithIndexLSH, wrong index",
			opts: []Options{
				WithIndexLSH(),
			},
			indexNames: []string{"wrong-index-name"},
			itemID:     uuid.NewString(),
			itemVec:    []float64{1, 2},
			err:        new(indexDoesNotExistError),
		},
		{
			name: "happy path, all indexes",
			opts: []Options{
				WithIndexLSH(&LSHConfig{
					SpaceDim: 3,
				}),
			},
			indexNames: []string{},
			itemID:     uuid.NewString(),
			itemVec:    []float64{1, 2, 3},
			err:        nil,
		},
		{
			name: "happy path, specific index",
			opts: []Options{
				WithIndexLSH(&LSHConfig{
					IndexName: "dumb-index-name",
					SpaceDim:  3,
				}),
			},
			indexNames: []string{"dumb-index-name"},
			itemID:     uuid.NewString(),
			itemVec:    []float64{1, 2, 3},
			err:        nil,
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				db, err := New("", tc.opts...)
				assert.NoError(t, err)

				err = db.Add(tc.itemID, tc.itemVec, tc.indexNames...)
				assert.IsType(t, tc.err, err)
			},
		)
	}
}

// TODO: Add TestGet.
// Instead of computing the probability for each entry by running several instances,
// here it's better to have a single DB instance where many items are added
// and the acceptance is given by the % of agreement with the ground truth.
