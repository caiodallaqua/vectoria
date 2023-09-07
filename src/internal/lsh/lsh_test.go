package lsh

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"vectoria/src/internal/kv"

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

	storage, err := kv.New("")
	assert.NoError(t, err)

	l, err := New(storage, numRounds, numHyperPlanes, spaceDim)
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
	testCase := struct {
		numRounds      uint32
		numHyperPlanes uint32
		spaceDim       uint32
		embedding      []float64
	}{4, 5, 3, []float64{-9.5, 0.7, 6.2}}

	l := setup(t, testCase.numRounds, testCase.numHyperPlanes, testCase.spaceDim)

	sks, err := l.sketches(testCase.embedding)
	assert.NoError(t, err)

	for _, sk := range sks {
		assert.Len(t, sk, int(testCase.numHyperPlanes))
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
	l := setup(t, tc.numRounds, tc.numHyperPlanes, tc.spaceDim)
	err := l.Add(tc.id, tc.embedding)
	assert.NoError(t, err)
}

func TestPrepareEmbedding(t *testing.T) {
	tc := struct {
		id        string
		embedding []float64
	}{uuid.NewString(), []float64{1.3, -0.89}}

	l := setup(t)
	data, err := l.prepareEmbedding(tc.id, tc.embedding)
	assert.NoError(t, err)

	k := key("embedding", tc.id)
	gotEncoded, exists := data[k]
	if !exists {
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
		l := setup(t, tc.numRounds, tc.numHyperPlanes, MIN_SPACE_DIM)

		t.Run(
			tc.name,
			func(t *testing.T) {
				data, err := l.prepareSketches(tc.id, tc.sks)
				assert.NoError(t, err)
				assert.NotNil(t, data)

				for _, sk := range tc.sks {
					k := key(sk, tc.id)
					got, exists := data[k]
					if !exists {
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
		l := setup(t, tc.numRounds, tc.numHyperPlanes, MIN_SPACE_DIM)

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
		l := setup(t, MIN_NUM_ROUNDS, MIN_NUM_HYPERPLANES, tc.spaceDim)

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

// TODO: make it ergonomic, recalling parameter order sucks
func setup(t *testing.T, params ...uint32) *LSH {
	var (
		l   *LSH
		err error
	)

	storage, err := kv.New("")
	assert.NoError(t, err)

	lenParams := len(params)

	switch lenParams {
	case 0:
		l, err = New(storage, MIN_NUM_ROUNDS, MIN_NUM_HYPERPLANES, MIN_SPACE_DIM)
	case 3:
		l, err = New(storage, params[0], params[1], params[2])
	default:
		err = errors.New("invalid number of parameters for setup")
	}

	assert.NotNil(t, l)
	assert.NoError(t, err)

	return l
}
