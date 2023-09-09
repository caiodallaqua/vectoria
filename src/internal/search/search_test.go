package search

import (
	"fmt"
	"io"
	"log/slog"
	"math"
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

func TestSimSearch(t *testing.T) {
	testCases := []struct {
		name       string
		queryVec   []float64
		candidates map[string][]float64
		threshold  float64
		want       []string
		err        error
	}{
		{
			name:       "empty candidates",
			queryVec:   []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{},
			threshold:  0.5,
			want:       []string{},
			err:        nil,
		},
		{
			name:     "exact match above threshold",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {1.0, 2.0, 3.0}, // sim ~ 0.9999
				"b": {4.0, 5.0, 6.0}, // sim ~ 0.9746
			},
			threshold: 0.99,
			want:      []string{"a"},
			err:       nil,
		},
		{
			name:     "match above threshold",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {1.0, 2.0, 3.0}, // sim ~ 0.9999
				"b": {4.0, 5.0, 6.0}, // sim ~ 0.9746
			},
			threshold: 0.97,
			want:      []string{"a", "b"},
			err:       nil,
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
			want:      []string{"a", "b"},
			err:       nil,
		},
		{
			name:     "no match above threshold",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {4.0, 5.0, 6.0}, // sim ~ 0.9746
				"b": {7.0, 8.0, 9.0}, // sim ~ 0.9594
			},
			threshold: 0.99,
			want:      []string{},
			err:       nil,
		},
		{
			name:     "query vector with negative values",
			queryVec: []float64{-1.0, -2.0, -3.0},
			candidates: map[string][]float64{
				"a": {-1.0, -2.0, -3.0}, // sim ~ 0.9999
				"b": {1.0, 2.0, 3.0},    // sim ~ -0.9999
			},
			threshold: 0.5,
			want:      []string{"a"},
			err:       nil,
		},
		{
			name:     "empty query vector",
			queryVec: []float64{},
			candidates: map[string][]float64{
				"a": {1.0, 2.0, 3.0},
				"b": {4.0, 5.0, 6.0},
			},
			threshold: 0.9,
			want:      []string{},
			err:       new(errEmptyVector),
		},
		{
			name:     "empty candidate vector",
			queryVec: []float64{1.0, 2.0, 3.0},
			candidates: map[string][]float64{
				"a": {},
				"b": {4.0, 5.0, 6.0},
			},
			threshold: 0.9,
			want:      []string{},
			err:       new(errEmptyVector),
		},
	}

	s := New()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.SimSearch(tc.queryVec, tc.candidates, tc.threshold)
			assert.IsType(t, tc.err, err)
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestCosineSim(t *testing.T) {
	testCases := []struct {
		name  string
		vecA  []float64
		normA float64
		vecB  []float64
		normB float64
		want  float64
		err   error
	}{
		{
			name:  "normA is zero",
			vecA:  []float64{0.0, 0.0},
			normA: 0,
			vecB:  []float64{0.0, 1.0},
			normB: 1.0,
			want:  -1.0,
			err:   nil,
		},
		{
			name:  "normB is zero",
			vecA:  []float64{1.0, 0.0},
			normA: 1.0,
			vecB:  []float64{0.0, 0.0},
			normB: 0.0,
			want:  -1.0,
			err:   nil,
		},
		{
			name:  "vecA and vecB are orthogonal",
			vecA:  []float64{1.0, 0.0},
			normA: 1.0,
			vecB:  []float64{0.0, 1.0},
			normB: 1.0,
			want:  0.0,
			err:   nil,
		},
		{
			name:  "vecA and vecB are parallel",
			vecA:  []float64{1.0, 0.0},
			normA: 1.0,
			vecB:  []float64{2.0, 0.0},
			normB: 2.0,
			want:  1.0,
			err:   nil,
		},
		{
			name:  "vecA and vecB have different sizes",
			vecA:  []float64{1.0, 0.0},
			normA: 1.0,
			vecB:  []float64{2.0, 0.0, 3.6},
			normB: 2.0,
			want:  0.0,
			err:   new(errVectorsNotSameLen),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cosineSim(tc.vecA, tc.vecB, tc.normA, tc.normB)
			assert.IsType(t, tc.err, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEuclideanNorm(t *testing.T) {
	testCases := []struct {
		vec  []float64
		want float64
		err  error
	}{
		{[]float64{3.0, 4.0}, 5.0, nil},
		{[]float64{0.0, 0.0, 0.0}, 0.0, nil},
		{[]float64{1.0, 2.0, 3.0}, math.Sqrt(14.0), nil},
		{[]float64{}, 0.0, new(errEmptyVector)},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("vec: %v", tc.vec), func(t *testing.T) {
			got, err := euclideanNorm(tc.vec)
			assert.IsType(t, tc.err, err)
			assert.Equal(t, tc.want, got)
		})
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
			name:   "equal length vectors",
			vecA:   []float64{1, 2, 3},
			vecB:   []float64{4, 5, 6},
			result: 32,
			err:    nil,
		},
		{
			name:   "unequal length vectors",
			vecA:   []float64{1, 2},
			vecB:   []float64{3, 4, 5},
			result: 0,
			err:    new(errVectorsNotSameLen),
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
