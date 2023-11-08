package lsh

import (
	"io"
	"log/slog"
	"math"
	"os"
	"testing"

	"github.com/mastrasec/vectoria/internal/storage"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	// Overwrites the logger to keep tests outputs clean.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	slog.SetDefault(logger)

	os.Exit(m.Run())
}

func TestNew(t *testing.T) {
	testCases := []struct {
		name           string
		numRounds      uint32
		numHyperPlanes uint32
		spaceDim       uint32
		want           *LSH
	}{
		{
			name:           "none missing", // min + 1 to show that it works wit non-default values
			numRounds:      MIN_NUM_ROUNDS + 1,
			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
			spaceDim:       MIN_SPACE_DIM + 1,
			want: &LSH{
				numRounds:      MIN_NUM_ROUNDS + 1,
				numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
				spaceDim:       MIN_SPACE_DIM + 1,
			},
		},
		{
			name:           "all missing",
			numRounds:      0,
			numHyperPlanes: 0,
			spaceDim:       0,
			want: &LSH{
				numRounds:      MIN_NUM_ROUNDS,
				numHyperPlanes: MIN_NUM_HYPERPLANES,
				spaceDim:       MIN_SPACE_DIM,
			},
		},
		{
			name:           "numRounds missing",
			numRounds:      0,
			numHyperPlanes: MIN_NUM_HYPERPLANES,
			spaceDim:       MIN_SPACE_DIM,
			want: &LSH{
				numRounds:      MIN_NUM_ROUNDS,
				numHyperPlanes: MIN_NUM_HYPERPLANES,
				spaceDim:       MIN_SPACE_DIM,
			},
		},
		{
			name:           "numHyperPlanes missing",
			numRounds:      MIN_NUM_ROUNDS,
			numHyperPlanes: 0,
			spaceDim:       MIN_SPACE_DIM,
			want: &LSH{
				numRounds:      MIN_NUM_ROUNDS,
				numHyperPlanes: MIN_NUM_HYPERPLANES,
				spaceDim:       MIN_SPACE_DIM,
			},
		},
		{
			name:           "spaceDim missing",
			numRounds:      MIN_NUM_ROUNDS,
			numHyperPlanes: MIN_NUM_HYPERPLANES,
			spaceDim:       0,
			want: &LSH{
				numRounds:      MIN_NUM_ROUNDS,
				numHyperPlanes: MIN_NUM_HYPERPLANES,
				spaceDim:       MIN_SPACE_DIM,
			},
		},
	}

	kv, err := storage.New("")
	assert.NoError(t, err)

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				l, err := New(kv, tc.numRounds, tc.numHyperPlanes, tc.spaceDim)
				assert.NoError(t, err)

				assert.Equal(t, tc.want.numRounds, l.numRounds)
				assert.Equal(t, tc.want.numHyperPlanes, l.numHyperPlanes)
				assert.Equal(t, tc.want.spaceDim, l.spaceDim)

				assert.NotNil(t, l.hashes)
				assert.Equal(t, tc.want.numRounds, uint32(len(l.hashes)))
				for _, elem := range l.hashes {
					assert.NotNil(t, elem.Hyperplanes)
				}
			},
		)
	}
}

// func TestValidateHyperParams_NumRounds(t *testing.T) {
// 	testCases := []struct {
// 		name           string
// 		numRounds      uint32
// 		numHyperPlanes uint32
// 		spaceDim       uint32
// 		err            error
// 	}{
// 		{
// 			name:           "lowerBound",
// 			numRounds:      MIN_NUM_ROUNDS,
// 			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
// 			spaceDim:       MIN_SPACE_DIM + 1,
// 			err:            nil,
// 		},
// 		{
// 			name:           "lessThanMin",
// 			numRounds:      MIN_NUM_ROUNDS - 1,
// 			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
// 			spaceDim:       MIN_SPACE_DIM + 1,
// 			err:            new(errNumRounds),
// 		},
// 		{
// 			name:           "upperBound",
// 			numRounds:      MAX_NUM_ROUNDS,
// 			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
// 			spaceDim:       MIN_SPACE_DIM + 1,
// 			err:            nil,
// 		},
// 		{
// 			name:           "moreThanMax",
// 			numRounds:      MAX_NUM_ROUNDS + 1,
// 			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
// 			spaceDim:       MIN_SPACE_DIM + 1,
// 			err:            new(errNumRounds),
// 		},
// 	}

// 	for _, tc := range testCases {
// 		t.Run(
// 			tc.name,
// 			func(t *testing.T) {
// 				err := validateHyperParams(tc.numRounds, tc.numHyperPlanes, tc.spaceDim)
// 				assert.True(t, errors.Is(err, tc.err))
// 			},
// 		)
// 	}
// }

// func TestValidateHyperParams_NumHyperPlanes(t *testing.T) {
// 	testCases := []struct {
// 		name           string
// 		numRounds      uint32
// 		numHyperPlanes uint32
// 		spaceDim       uint32
// 		err            error
// 	}{
// 		{
// 			name:           "lowerBound",
// 			numRounds:      MIN_NUM_ROUNDS + 1,
// 			numHyperPlanes: MIN_NUM_HYPERPLANES,
// 			spaceDim:       MIN_SPACE_DIM + 1,
// 			err:            nil,
// 		},
// 		{
// 			name:           "lessThanMin",
// 			numRounds:      MIN_NUM_ROUNDS + 1,
// 			numHyperPlanes: MIN_NUM_HYPERPLANES - 1,
// 			spaceDim:       MIN_SPACE_DIM + 1,
// 			err:            new(errNumHyperPlanes),
// 		},
// 	}

// 	for _, tc := range testCases {
// 		t.Run(
// 			tc.name,
// 			func(t *testing.T) {
// 				err := validateHyperParams(tc.numRounds, tc.numHyperPlanes, tc.spaceDim)
// 				assert.True(t, errors.Is(err, tc.err))
// 			},
// 		)
// 	}
// }

// func TestValidateHyperParams_SpaceDim(t *testing.T) {
// 	testCases := []struct {
// 		name           string
// 		numRounds      uint32
// 		numHyperPlanes uint32
// 		spaceDim       uint32
// 		err            error
// 	}{
// 		{
// 			name:           "lowerBound",
// 			numRounds:      MIN_NUM_ROUNDS + 1,
// 			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
// 			spaceDim:       MIN_SPACE_DIM,
// 			err:            nil,
// 		},
// 		{
// 			name:           "lessThanMin",
// 			numRounds:      MIN_NUM_ROUNDS + 1,
// 			numHyperPlanes: MIN_NUM_HYPERPLANES + 1,
// 			spaceDim:       MIN_SPACE_DIM - 1,
// 			err:            new(errSpaceDim),
// 		},
// 	}

// 	for _, tc := range testCases {
// 		t.Run(
// 			tc.name,
// 			func(t *testing.T) {
// 				err := validateHyperParams(tc.numRounds, tc.numHyperPlanes, tc.spaceDim)
// 				assert.True(t, errors.Is(err, tc.err))
// 			},
// 		)
// 	}
// }

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
			err:            new(invalidNumSketchesError),
		},
		{
			name:           "numSketches_smallerThanNumRounds",
			numRounds:      2,
			numHyperPlanes: 3,
			sks:            []string{"101"},
			err:            new(invalidNumSketchesError),
		},
		{
			name:           "sketchLen_smallerThanNumHyperPlanes",
			numRounds:      1,
			numHyperPlanes: 3,
			sks:            []string{"10"},
			err:            new(invalidSketchLenError),
		},
		{
			name:           "sketchLen_greaterThanNumHyperPlanes",
			numRounds:      1,
			numHyperPlanes: 2,
			sks:            []string{"101"},
			err:            new(invalidSketchLenError),
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
			err:       new(embeddingLenError),
		},
		{
			name:      "embedLen_greaterThanSpaceDim",
			spaceDim:  2,
			embedding: []float64{1.2, 3.67, -8.44},
			err:       new(embeddingLenError),
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

func TestGetNeighbors_HappyPath(t *testing.T) {
	var (
		spaceDim       uint32 = 3
		numHyperPlanes uint32 = 1
		numRounds      uint32 = 10

		numRuns           uint32  = 100
		acceptedDeviation float64 = 0.1
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
				probMap := probByCandidate(t, tc.queryVec, tc.candidates, numHyperPlanes, numRounds)
				countMap := make(map[string]uint32, len(tc.candidates))

				for i := uint32(0); i < numRuns; i++ {
					l := setup(t, Opts{
						numHyperPlanes: numHyperPlanes,
						numRounds:      numRounds,
						spaceDim:       spaceDim,
					})

					for id, vec := range tc.candidates {
						err := l.Add(id, vec)
						assert.NoError(t, err)
					}

					ids, err := l.Get(tc.queryVec, tc.threshold, tc.k)
					assert.NoError(t, err)

					for _, id := range ids {
						countMap[id] += 1

						// False positives should not be returned since we remove them by measuring similarity directly.
						assert.Contains(t, tc.wantIDs, id)
					}
				}

				for id, count := range countMap {
					got := float64(count) / float64(numRuns)
					deviation := math.Abs(got - probMap[id])

					if got < probMap[id] && deviation > acceptedDeviation {
						t.Errorf("observed: %0.5f, expected: %0.5f, deviation: %0.5f", got, probMap[id], deviation)
					}
				}
			},
		)
	}
}

func TestGetNeighbors_Errors(t *testing.T) {
	testCases := []struct {
		name      string
		spaceDim  uint32
		queryVec  []float64
		threshold float64
		k         uint32
		err       error
	}{
		{
			name:      "empty queryVec",
			spaceDim:  2,
			queryVec:  []float64{},
			threshold: 0.8,
			k:         0,
			err:       &embeddingLenError{},
		},
		{
			name:      "queryVec length smaller than spaceDim",
			spaceDim:  2,
			queryVec:  []float64{1.0},
			threshold: 0.8,
			k:         0,
			err:       &embeddingLenError{},
		},
		{
			name:      "queryVec length greater than spaceDim",
			spaceDim:  2,
			queryVec:  []float64{1.0, 2.0, 3.0},
			threshold: 0.8,
			k:         0,
			err:       &embeddingLenError{},
		},
		{
			name:      "threshold greater than one",
			spaceDim:  2,
			queryVec:  []float64{1.0, 2.0},
			threshold: 1.1,
			k:         0,
			err:       &invalidThresholdError{},
		},
		{
			name:      "threshold smaller than zero",
			spaceDim:  2,
			queryVec:  []float64{1.0, 2.0},
			threshold: -0.1,
			k:         0,
			err:       &invalidThresholdError{},
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				l := setup(t, Opts{spaceDim: tc.spaceDim})

				_, err := l.Get(tc.queryVec, tc.threshold, tc.k)
				assert.IsType(t, tc.err, err)
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

	// TODO: Add seed or accept tolerable deviations when NumHyperPlanes > 1.
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

func probByCandidate(
	t *testing.T,
	queryVec []float64,
	candidates map[string][]float64,
	numHyperPlanes,
	numRounds uint32,
) map[string]float64 {
	probMap := make(map[string]float64, len(candidates))

	for id, vec := range candidates {
		prob := probSimilar(t, queryVec, vec, numHyperPlanes, numRounds)
		probMap[id] = prob
	}

	return probMap
}

func probSimilar(t *testing.T, vecA, vecB []float64, numHyperPlanes, numRounds uint32) float64 {
	theta := angleBetween(t, vecA, vecB)

	// probability of both vectors being mapped to the same bucket in each projection / hyperplane
	p := 1 - theta/math.Pi

	// probability of both vectors being considered similar to each other by the LSH algorithm
	return 1 - math.Pow(1-math.Pow(p, float64(numHyperPlanes)), float64(numRounds))
}

func angleBetween(t *testing.T, vecA, vecB []float64) float64 {
	if len(vecA) != len(vecB) {
		t.Fatalf("to compute angle between, vectors must have same len")
	}

	normA := vecNorm(vecA)
	normB := vecNorm(vecB)

	if normA*normB == 0 {
		t.Fatalf("norm cannot be zero")
	}

	return math.Acos(dotProduct(vecA, vecB) / (normA * normB))
}

func dotProduct(vecA, vecB []float64) float64 {
	dotProd := 0.0
	for i := 0; i < len(vecA); i++ {
		dotProd += vecA[i] * vecB[i]
	}

	return dotProd
}

func vecNorm(vec []float64) float64 {
	norm := 0.0
	for _, component := range vec {
		norm += component * component
	}

	return math.Sqrt(norm)
}
