package lsh

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/mastrasec/vectoria/internal/storage"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	// Overwrites the logger to keep tests outputs clean
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	slog.SetDefault(logger)

	os.Exit(m.Run())
}

func TestNew(t *testing.T) {
	var (
		numRounds      = MIN_NUM_ROUNDS
		numHyperPlanes = MIN_NUM_HYPERPLANES
		spaceDim       = MIN_SPACE_DIM
	)

	kv, err := storage.New("")
	assert.NoError(t, err)

	l, err := New(kv, numRounds, numHyperPlanes, spaceDim)
	assert.NoError(t, err)

	assert.NotNil(t, l)
	assert.NotNil(t, l.hashes)
	assert.Equal(t, numRounds, uint32(len(l.hashes)))
	for _, elem := range l.hashes {
		assert.NotNil(t, elem.Hyperplanes)
	}

	assert.Equal(t, numRounds, l.numRounds)
	assert.Equal(t, numHyperPlanes, l.numHyperPlanes)
	assert.Equal(t, spaceDim, l.spaceDim)
}

func TestValidateHyperParams_NumRounds(t *testing.T) {
	testCases := []struct {
		name           string
		numRounds      uint32
		numHyperPlanes uint32
		spaceDim       uint32
		err            error
	}{
		{
			name:           "lowerBound",
			numRounds:      MIN_NUM_ROUNDS,
			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
			spaceDim:       MIN_SPACE_DIM + 1,
			err:            nil,
		},
		{
			name:           "lessThanMin",
			numRounds:      MIN_NUM_ROUNDS - 1,
			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
			spaceDim:       MIN_SPACE_DIM + 1,
			err:            new(errNumRounds),
		},
		{
			name:           "upperBound",
			numRounds:      MAX_NUM_ROUNDS,
			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
			spaceDim:       MIN_SPACE_DIM + 1,
			err:            nil,
		},
		{
			name:           "moreThanMax",
			numRounds:      MAX_NUM_ROUNDS + 1,
			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
			spaceDim:       MIN_SPACE_DIM + 1,
			err:            new(errNumRounds),
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				err := validateHyperParams(tc.numRounds, tc.numHyperPlanes, tc.spaceDim)
				assert.True(t, errors.Is(err, tc.err))
			},
		)
	}
}

func TestValidateHyperParams_NumHyperPlanes(t *testing.T) {
	testCases := []struct {
		name           string
		numRounds      uint32
		numHyperPlanes uint32
		spaceDim       uint32
		err            error
	}{
		{
			name:           "lowerBound",
			numRounds:      MIN_NUM_ROUNDS + 1,
			numHyperPlanes: MIN_NUM_HYPERPLANES,
			spaceDim:       MIN_SPACE_DIM + 1,
			err:            nil,
		},
		{
			name:           "lessThanMin",
			numRounds:      MIN_NUM_ROUNDS + 1,
			numHyperPlanes: MIN_NUM_HYPERPLANES - 1,
			spaceDim:       MIN_SPACE_DIM + 1,
			err:            new(errNumHyperPlanes),
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				err := validateHyperParams(tc.numRounds, tc.numHyperPlanes, tc.spaceDim)
				assert.True(t, errors.Is(err, tc.err))
			},
		)
	}
}

func TestValidateHyperParams_SpaceDim(t *testing.T) {
	testCases := []struct {
		name           string
		numRounds      uint32
		numHyperPlanes uint32
		spaceDim       uint32
		err            error
	}{
		{
			name:           "lowerBound",
			numRounds:      MIN_NUM_ROUNDS + 1,
			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
			spaceDim:       MIN_SPACE_DIM,
			err:            nil,
		},
		{
			name:           "lessThanMin",
			numRounds:      MIN_NUM_ROUNDS + 1,
			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
			spaceDim:       MIN_SPACE_DIM - 1,
			err:            new(errSpaceDim),
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				err := validateHyperParams(tc.numRounds, tc.numHyperPlanes, tc.spaceDim)
				assert.True(t, errors.Is(err, tc.err))
			},
		)
	}
}

func TestSketches(t *testing.T) {
	tc := struct {
		numRounds      uint32
		numHyperPlanes uint32
		spaceDim       uint32
		embedding      []float64
	}{4, 5, 3, []float64{-9.5, 0.7, 6.2}}

	opts := Opts{numRounds: tc.numRounds, numHyperPlanes: tc.numHyperPlanes, spaceDim: tc.spaceDim}
	l := setup(t, opts)

	sks, err := l.getSketches(tc.embedding)
	assert.NoError(t, err)

	for _, sk := range sks {
		assert.Len(t, sk, int(tc.numHyperPlanes))
		assert.True(t, validSketchChars(sk))
	}
}

func validSketchChars(sketch string) bool {
	for _, char := range sketch {
		if char != '0' && char != '1' {
			return false
		}
	}

	return true
}

func TestAdd(t *testing.T) {
	tc := struct {
		numRounds      uint32
		numHyperPlanes uint32
		spaceDim       uint32
		id             string
		embedding      []float64
	}{
		numRounds:      4,
		numHyperPlanes: 10,
		spaceDim:       3,
		id:             uuid.NewString(),
		embedding:      []float64{3.66, 8.5, 99},
	}
	opts := Opts{numRounds: tc.numRounds, numHyperPlanes: tc.numHyperPlanes, spaceDim: tc.spaceDim}
	l := setup(t, opts)
	err := l.Add(tc.id, tc.embedding)
	assert.NoError(t, err)
}

func TestPrepareEmbedding(t *testing.T) {
	tc := struct {
		id        string
		embedding []float64
	}{uuid.NewString(), []float64{1.3, -0.89}}

	l := setup(t, Opts{})
	data, err := l.prepareEmbedding(tc.id, tc.embedding)
	assert.NoError(t, err)

	k := key("embedding", tc.id)
	gotEncoded, ok := data[k]
	if !ok {
		t.Errorf("expected key to exist: %v", k)
	}

	got, err := decodeFloat64Slice(gotEncoded)
	assert.NoError(t, err)
	assert.ElementsMatch(t, tc.embedding, got)
}

func TestPrepareSketches(t *testing.T) {
	testCases := []struct {
		name           string
		numRounds      uint32
		numHyperPlanes uint32
		id             string
		sks            []string
	}{
		{
			name:           "singleSketch",
			numRounds:      1,
			numHyperPlanes: 3,
			id:             uuid.NewString(),
			sks:            []string{"101"},
		},
		{
			name:           "multipleSketches",
			numRounds:      2,
			numHyperPlanes: 3,
			id:             uuid.NewString(),
			sks:            []string{"101", "011"},
		},
	}

	for _, tc := range testCases {
		l := setup(t, Opts{numRounds: tc.numRounds, numHyperPlanes: tc.numHyperPlanes})

		t.Run(
			tc.name,
			func(t *testing.T) {
				data, err := l.prepareSketches(tc.id, tc.sks)
				assert.NoError(t, err)
				assert.NotNil(t, data)

				for _, sk := range tc.sks {
					k := key(sk, tc.id)
					got, ok := data[k]
					if !ok {
						t.Errorf("expected key to exist: %v", k)
					}
					assert.Equal(t, tc.id, string(got))
				}
			},
		)
	}
}

func TestCheckSketches(t *testing.T) {
	testCases := []struct {
		name           string
		numRounds      uint32
		numHyperPlanes uint32
		sks            []string
		err            error
	}{
		{
			name:           "numSketches_equalsToNumRounds_SingleRound",
			numRounds:      1,
			numHyperPlanes: 3,
			sks:            []string{"101"},
			err:            nil,
		},
		{
			name:           "numSketches_equalsToNumRounds_MultipleRounds",
			numRounds:      3,
			numHyperPlanes: 3,
			sks:            []string{"101", "110", "001"},
			err:            nil,
		},
		{
			name:           "numSketches_greaterThanNumRounds",
			numRounds:      1,
			numHyperPlanes: 3,
			sks:            []string{"101", "111"},
			err:            new(errInvalidNumSketches),
		},
		{
			name:           "numSketches_smallerThanNumRounds",
			numRounds:      2,
			numHyperPlanes: 3,
			sks:            []string{"101"},
			err:            new(errInvalidNumSketches),
		},
		{
			name:           "sketchLen_smallerThanNumHyperPlanes",
			numRounds:      1,
			numHyperPlanes: 3,
			sks:            []string{"10"},
			err:            new(errInvalidSketchLen),
		},
		{
			name:           "sketchLen_greaterThanNumHyperPlanes",
			numRounds:      1,
			numHyperPlanes: 2,
			sks:            []string{"101"},
			err:            new(errInvalidSketchLen),
		},
	}

	for _, tc := range testCases {
		l := setup(t, Opts{numRounds: tc.numRounds, numHyperPlanes: tc.numHyperPlanes})

		t.Run(
			tc.name,
			func(t *testing.T) {
				err := l.checkSketches(tc.sks)
				assert.IsType(t, tc.err, err)
			},
		)
	}
}

func TestCheckEmbedding(t *testing.T) {
	testCases := []struct {
		name      string
		spaceDim  uint32
		embedding []float64
		err       error
	}{
		{
			name:      "embedLen_equalsToSpaceDim",
			spaceDim:  2,
			embedding: []float64{1.2, 3.67},
			err:       nil,
		},
		{
			name:      "embedLen_smallerThanSpaceDim",
			spaceDim:  3,
			embedding: []float64{1.2, 3.67},
			err:       new(errEmbeddingLen),
		},
		{
			name:      "embedLen_greaterThanSpaceDim",
			spaceDim:  2,
			embedding: []float64{1.2, 3.67, -8.44},
			err:       new(errEmbeddingLen),
		},
	}

	for _, tc := range testCases {
		l := setup(t, Opts{spaceDim: tc.spaceDim})

		t.Run(
			tc.name,
			func(t *testing.T) {
				err := l.checkEmbedding(tc.embedding)
				assert.IsType(t, tc.err, err)
			},
		)
	}
}

func TestEncodeFloat64Slice(t *testing.T) {
	testCases := []struct {
		name  string
		slice []float64
	}{
		{"empty", []float64{}},
		{"not_empty", []float64{1.23, 4.56, 7.89}},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				data, err := encodeFloat64Slice(tc.slice)
				assert.NoError(t, err)

				got, err := decodeFloat64Slice(data)
				assert.NoError(t, err)
				assert.ElementsMatch(t, tc.slice, got)
			},
		)
	}
}

func TestGetNeighbors(t *testing.T) {
	testCases := []struct {
		name       string
		spaceDim   uint32
		queryVec   []float64
		threshold  float64
		k          uint32
		storedVecs map[string][]float64
		err        error
		want       []string
	}{
		{
			name:       "empty queryVec with no storedVecs",
			spaceDim:   2,
			queryVec:   []float64{},
			threshold:  0.8,
			k:          0,
			storedVecs: map[string][]float64{},
			err:        new(errEmbeddingLen),
			want:       nil,
		},
		{
			name:       "queryVec length smaller than spaceDim with no storedVecs",
			spaceDim:   2,
			queryVec:   []float64{1.0},
			threshold:  0.8,
			k:          0,
			storedVecs: map[string][]float64{},
			err:        new(errEmbeddingLen),
			want:       nil,
		},
		{
			name:       "queryVec length greater than spaceDim with no storedVecs",
			spaceDim:   2,
			queryVec:   []float64{1.0, 2.0, 3.0},
			threshold:  0.8,
			k:          0,
			storedVecs: map[string][]float64{},
			err:        new(errEmbeddingLen),
			want:       nil,
		},
		{
			name:       "queryVec length matches spaceDim with no storedVecs",
			spaceDim:   2,
			queryVec:   []float64{1.0, 2.0},
			threshold:  0.8,
			k:          0,
			storedVecs: map[string][]float64{},
			err:        nil,
			want:       []string{},
		},
		{
			name:       "exact match above threshold",
			spaceDim:   2,
			queryVec:   []float64{1.0, 2.0},
			threshold:  0.8,
			k:          0,
			storedVecs: map[string][]float64{"a": {1.0, 2.0}},
			err:        nil,
			want:       []string{"a"},
		},
		{
			name:      "match above threshold",
			spaceDim:  2,
			queryVec:  []float64{1.0, 2.0},
			threshold: 0.98,
			k:         0,
			storedVecs: map[string][]float64{
				"a": {1.0, 2.0}, // sim ~ 0.9999
				"b": {3.0, 4.0}, // sim ~ 0.9838
			},
			err:  nil,
			want: []string{"a", "b"},
		},
		{
			name:      "partial match above threshold",
			spaceDim:  2,
			queryVec:  []float64{1.0, 2.0},
			threshold: 0.99,
			k:         0,
			storedVecs: map[string][]float64{
				"a": {1.0, 2.0}, // sim ~ 0.9999
				"b": {3.0, 4.0}, // sim ~ 0.9838
			},
			err:  nil,
			want: []string{"a"},
		},
		{
			name:      "no match above threshold",
			spaceDim:  2,
			queryVec:  []float64{1.0, 2.0},
			threshold: 0.99,
			k:         0,
			storedVecs: map[string][]float64{
				"a": {3.0, 4.0}, // sim ~ 0.9838
				"b": {5.0, 6.0}, // sim ~ 0.9734
			},
			err:  nil,
			want: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				l := setup(t, Opts{spaceDim: tc.spaceDim})

				for k, v := range tc.storedVecs {
					err := l.Add(k, v)
					assert.NoError(t, err)
				}

				got, err := l.GetNeighbors(tc.queryVec, tc.threshold, tc.k)
				assert.IsType(t, tc.err, err)
				assert.ElementsMatch(t, tc.want, got)
			},
		)
	}
}

func TestGetBucketIDs(t *testing.T) {
	tc := struct {
		id        string
		embedding []float64
	}{uuid.NewString(), []float64{1.31, 4.6}}

	l := setup(t, Opts{numHyperPlanes: 10})

	err := l.Add(tc.id, tc.embedding)
	assert.NoError(t, err)

	sks, err := l.getSketches(tc.embedding)
	assert.NoError(t, err)

	for _, sk := range sks {
		ids, err := l.getBucketIDs(sk)
		assert.NoError(t, err)
		assert.Contains(t, ids, tc.id)
	}
}

func TestGetEmbedding(t *testing.T) {
	tc := struct {
		id        string
		embedding []float64
	}{uuid.NewString(), []float64{1.31, 4.6}}

	l := setup(t, Opts{})

	err := l.Add(tc.id, tc.embedding)
	assert.NoError(t, err)

	got, err := l.getEmbedding(tc.id)
	assert.NoError(t, err)
	assert.ElementsMatch(t, tc.embedding, got)
}

func TestGetEmbeddingsFromBuckets(t *testing.T) {
	tc := struct {
		id        string
		embedding []float64
	}{uuid.NewString(), []float64{1.31, 4.6}}

	l := setup(t, Opts{})

	err := l.Add(tc.id, tc.embedding)
	assert.NoError(t, err)

	sks, err := l.getSketches(tc.embedding)
	assert.NoError(t, err)

	got, err := l.getEmbeddingsFromBuckets(sks)
	assert.NoError(t, err)

	assert.Contains(t, got, tc.id)
	assert.ElementsMatch(t, got[tc.id], tc.embedding)
}

type Opts struct {
	numRounds      uint32
	numHyperPlanes uint32
	spaceDim       uint32
}

func setup(t *testing.T, opts Opts) *LSH {
	var (
		l   *LSH
		err error
	)

	assert.NotNil(t, opts)

	if opts.numRounds == 0 {
		opts.numRounds = MIN_NUM_ROUNDS
	}

	if opts.numHyperPlanes == 0 {
		opts.numHyperPlanes = MIN_NUM_HYPERPLANES
	}

	if opts.spaceDim == 0 {
		opts.spaceDim = MIN_SPACE_DIM
	}

	storage, err := storage.New("")
	assert.NoError(t, err)

	l, err = New(storage, opts.numRounds, opts.numHyperPlanes, opts.spaceDim)
	assert.NoError(t, err)
	assert.NotNil(t, l)

	return l
}
