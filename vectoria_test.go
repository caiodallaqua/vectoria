package vectoria

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/caiodallaqua/vectoria/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	// Overwrites the logger to keep tests outputs clean
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	slog.SetDefault(logger)

	os.Exit(m.Run())
}

// TODO: Add TestGet.
// Instead of computing the probability for each entry by running several instances,
// here it's better to have a single DB instance where many items are added
// and the acceptance is given by the % of agreement with the ground truth.

func TestNew(t *testing.T) {
	testCases := []struct {
		testName       string
		dbConfig       DBConfig
		wantNumIndexes int
		err            error
	}{
		{
			testName: "no index",
			dbConfig: DBConfig{
				Path: "",
				LSH:  []LSHConfig{},
			},
			wantNumIndexes: 0,
			err:            nil,
		},
		{
			testName: "single index",
			dbConfig: DBConfig{
				Path: "",
				LSH: []LSHConfig{
					{
						IndexName:      "fake-index-name",
						NumRounds:      3,
						NumHyperPlanes: 4,
						SpaceDim:       5,
					},
				},
			},
			wantNumIndexes: 1,
			err:            nil,
		},
		{
			testName: "index duplication",
			dbConfig: DBConfig{
				Path: "",
				LSH: []LSHConfig{
					{
						IndexName:      "duplicated-index-name",
						NumRounds:      3,
						NumHyperPlanes: 4,
						SpaceDim:       5,
					},
					{
						IndexName: "duplicated-index-name",
					},
				},
			},
			wantNumIndexes: 0,
			err:            &indexAlreadyExistsError{},
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.testName,
			func(t *testing.T) {
				db, err := New(tc.dbConfig)
				assert.IsType(t, err, tc.err)
				if tc.err == nil {
					assert.NotNil(t, db)
					assert.Equal(t, db.indexRef.len(), tc.wantNumIndexes)
				}
			},
		)
	}
}

func TestAddLSH(t *testing.T) {
	testCases := []struct {
		testName       string
		configs        []LSHConfig
		wantNumIndexes int
		err            error
	}{
		{
			testName: "happy path",
			configs: []LSHConfig{
				{
					IndexName:      "fake-index-name",
					NumRounds:      3,
					NumHyperPlanes: 4,
					SpaceDim:       5,
				},
			},
			err:            nil,
			wantNumIndexes: 1,
		},
		{
			testName: "duplicated index",
			configs: []LSHConfig{
				{
					IndexName: "duplicated-index-name",
				},
				{
					IndexName: "duplicated-index-name",
				},
			},
			err:            &indexAlreadyExistsError{},
			wantNumIndexes: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.testName,
			func(t *testing.T) {
				stg, err := storage.New("")
				assert.NoError(t, err)

				db := newDB(stg)

				err = db.addLSH(tc.configs...)
				assert.IsType(t, tc.err, err)

				assert.Equal(t, db.indexRef.len(), tc.wantNumIndexes)

				if tc.err == nil {
					for _, config := range tc.configs {
						idx, ok := db.indexRef.get(config.IndexName)
						assert.True(t, ok)
						assert.NotNil(t, idx)

						info := idx.info()

						assert.Equal(t, config.NumRounds, info["numRounds"])
						assert.Equal(t, config.NumHyperPlanes, info["numHyperPlanes"])
						assert.Equal(t, config.SpaceDim, info["spaceDim"])
					}
				}
			},
		)
	}
}

func TestAdd(t *testing.T) {
	testCases := []struct {
		testName   string
		dbConfig   DBConfig
		indexNames []string
		itemID     string
		itemVec    []float64
		err        error
	}{
		{
			testName:   "empty config, all indexes",
			dbConfig:   DBConfig{},
			indexNames: []string{},
			itemID:     uuid.NewString(),
			itemVec:    []float64{1, 2},
			err:        &dbHasNoIndexError{},
		},
		{
			testName:   "empty config, specific index",
			dbConfig:   DBConfig{},
			indexNames: []string{"some-index-name"},
			itemID:     uuid.NewString(),
			itemVec:    []float64{},
			err:        &dbHasNoIndexError{},
		},
		{
			testName: "happy path, all indexes",
			dbConfig: DBConfig{
				LSH: []LSHConfig{
					{
						SpaceDim: 3,
					},
				},
			},
			indexNames: []string{},
			itemID:     uuid.NewString(),
			itemVec:    []float64{1, 2, 3},
			err:        nil,
		},
		{
			testName: "happy path, specific index",
			dbConfig: DBConfig{
				LSH: []LSHConfig{
					{
						IndexName: "dumb-index-name",
						SpaceDim:  3,
					},
				},
			},
			indexNames: []string{"dumb-index-name"},
			itemID:     uuid.NewString(),
			itemVec:    []float64{1, 2, 3},
			err:        nil,
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.testName,
			func(t *testing.T) {
				db, err := New(tc.dbConfig)
				assert.NoError(t, err)

				err = db.Add(tc.itemID, tc.itemVec, tc.indexNames...)
				assert.IsType(t, tc.err, err)

				if tc.err == nil {
					res, err := db.Get(tc.itemVec, 0.9, 1, tc.indexNames...)
					assert.NoError(t, err)

					for index, keys := range res {
						if len(tc.indexNames) != 0 {
							assert.Contains(t, tc.indexNames, index)
						}

						assert.Contains(t, keys, tc.itemID)
					}
				}
			},
		)
	}
}
