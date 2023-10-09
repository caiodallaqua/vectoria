package vectoria

import (
	"io"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/maps"
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
				db := newDB(nil)

				f := WithIndexLSH(tc.confs...)
				err := f(db)
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

func TestGet_HappyPath(t *testing.T) {
	var (
		indexName string = "dumb"
		spaceDim  uint32 = 3
	)

	testCases := []struct {
		name       string
		queryVec   []float64
		candidates map[string][]float64
		threshold  float64
		k          uint32
		wantIDs    []string
	}{
		{
			name:       "empty candidates",
			queryVec:   []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{},
			threshold:  0.5,
			k:          0,
			wantIDs:    []string{},
		},
		{
			name:     "exact match above threshold",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {1.0, 2.0, 3.0}, // sim ~ 0.9999
				"b": {4.0, 5.0, 6.0}, // sim ~ 0.9746
			},
			threshold: 0.99,
			k:         0,
			wantIDs:   []string{"a"},
		},
		{
			name:     "match above threshold",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {1.0, 2.0, 3.0}, // sim ~ 0.9999
				"b": {4.0, 5.0, 6.0}, // sim ~ 0.9746
			},
			threshold: 0.97,
			k:         0,
			wantIDs:   []string{"a", "b"},
		},
		{
			name:     "partial match above threshold",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {1.0, 2.0, 3.0}, // sim ~ 0.9999
				"b": {4.0, 5.0, 6.0}, // sim ~ 0.9746
				"c": {7.0, 8.0, 9.0}, // sim ~ 0.9594
			},
			threshold: 0.96,
			k:         0,
			wantIDs:   []string{"a", "b"},
		},
		{
			name:     "no match above threshold",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {4.0, 5.0, 6.0}, // sim ~ 0.9746
				"b": {7.0, 8.0, 9.0}, // sim ~ 0.9594
			},
			threshold: 0.99,
			k:         0,
			wantIDs:   []string{},
		},
		{
			name:     "query vector with negative values",
			queryVec: []float64{-1.0, -2.0, -3.0},
			candidates: map[string][]float64{
				"a": {-1.0, -2.0, -3.0}, // sim ~ 0.9999
				"b": {1.0, 2.0, 3.0},    // sim ~ -0.9999
			},
			threshold: 0.5,
			k:         0,
			wantIDs:   []string{"a"},
		},
		{
			name:       "empty candidates k > len(wantIDs)",
			queryVec:   []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{},
			threshold:  0.5,
			k:          1,
			wantIDs:    []string{},
		},
		{
			name:     "exact match above threshold k > len(wantIDs)",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {1.0, 2.0, 3.0}, // sim ~ 0.9999
				"b": {4.0, 5.0, 6.0}, // sim ~ 0.9746
			},
			threshold: 0.99,
			k:         2,
			wantIDs:   []string{"a"},
		},
		{
			name:     "match above threshold k < len(wantIDs)",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {1.0, 2.0, 3.0}, // sim ~ 0.9999
				"b": {4.0, 5.0, 6.0}, // sim ~ 0.9746
			},
			threshold: 0.97,
			k:         1,
			wantIDs:   []string{"a"},
		},
		{
			name:     "partial match above threshold k < len(wantIDs)",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {1.0, 2.0, 3.0}, // sim ~ 0.9999
				"b": {4.0, 5.0, 6.0}, // sim ~ 0.9746
				"c": {7.0, 8.0, 9.0}, // sim ~ 0.9594
			},
			threshold: 0.96,
			k:         1,
			wantIDs:   []string{"a"},
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				db := setup(t, indexName, spaceDim)

				for id, vec := range tc.candidates {
					err := db.Add(id, vec, indexName)
					assert.NoError(t, err)
				}

				ids, err := db.Get(tc.queryVec, tc.threshold, tc.k, indexName)
				assert.NoError(t, err)
				assert.ElementsMatch(t, tc.wantIDs, maps.Values(ids)[0])
			},
		)
	}
}

// TODO: TestGet_HappyPath_MultiIndex

// TODO: TestGet_Errors

func setup(t *testing.T, indexName string, spaceDim uint32) *DB {
	db, err := New("", WithIndexLSH(&LSHConfig{
		IndexName: indexName,
		SpaceDim:  spaceDim,
		// TODO: Add seed or accept tolerable deviations when NumHyperPlanes > 1.
	}))
	assert.NoError(t, err)

	return db
}
