package simhash

import (
	"fmt"
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

func TestSketch(t *testing.T) {
	testCases := []struct {
		hyperplanes [][]float64
		embedding   []float64
		sk          string
		err         error
	}{
		{
			hyperplanes: [][]float64{
				{1, -1, 1, 1},
				{-1, 1, -1, 1},
				{1, 1, -1, -1},
			},
			embedding: []float64{3, 4, 5, 6},
			sk:        "110",
			err:       nil,
		},
		{
			hyperplanes: [][]float64{
				{1, -1, 1, 1},
				{-1, 1, -1, 1},
				{1, 1, -1, -1},
			},
			embedding: []float64{4, 3, 2, 1},
			sk:        "101",
			err:       nil,
		},
		{
			hyperplanes: [][]float64{
				{1, -1},
				{-1, 1},
			},
			embedding: []float64{4, 3, 2, 1},
			sk:        "",
			err:       new(vectorsNotSameLenError),
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("sketch=\"%s\"", tc.sk), func(t *testing.T) {
			l := setup(t, len(tc.hyperplanes), len(tc.embedding))

			// overwrites random generated hyperplanes for testing
			l.Hyperplanes = tc.hyperplanes

			sk, err := l.Sketch(tc.embedding)
			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.sk, sk)
		})
	}
}

func TestGenerateHyperplanes(t *testing.T) {
	testCases := []struct {
		numHyperPlanes uint32
		spaceDim       uint32
		err            error
	}{
		{2, 3, nil},
		{3, 4, nil},
		{0, 5, new(numHyperPlanesError)},
		{1, 0, new(spaceDimError)},
	}

	for _, tc := range testCases {
		t.Run(
			fmt.Sprintf("numHyperPlanes=%d_spaceDim=%d_err=%v", tc.numHyperPlanes, tc.spaceDim, tc.err),
			func(t *testing.T) {
				hyperplanes, err := generateHyperplanes(tc.numHyperPlanes, tc.spaceDim)

				assert.Equal(t, tc.err, err)

				if tc.err == nil {
					assert.Len(t, hyperplanes, int(tc.numHyperPlanes))
					assert.Len(t, hyperplanes[0], int(tc.spaceDim))
				}
			},
		)
	}
}

func TestDotProduct(t *testing.T) {
	testCases := []struct {
		name   string
		vecA   []float64
		vecB   []float64
		result float64
		err    error
	}{
		{
			name:   "EqualLengthVectors",
			vecA:   []float64{1, 2, 3},
			vecB:   []float64{4, 5, 6},
			result: 32,
			err:    nil,
		},
		{
			name:   "UnequalLengthVectors",
			vecA:   []float64{1, 2},
			vecB:   []float64{3, 4, 5},
			result: 0,
			err:    new(vectorsNotSameLenError),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := dotProduct(tc.vecA, tc.vecB)

			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.result, res)
		})
	}
}

func setup(t *testing.T, numHyperplanes, spaceDim int) *SimHash {
	l, err := New(uint32(numHyperplanes), uint32(spaceDim))
	assert.NoError(t, err)

	return l
}
