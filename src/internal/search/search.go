package search

import (
	"context"
	"log/slog"
	"math"
	"slices"
)

const EPSILON = 1e-10

type Search struct{}

type Contract interface {
	SimSearch(queryVec []float64, candidates map[string][]float64, threshold float64) (res []string, err error)
}

func New() *Search {
	return &Search{}
}

func (s *Search) SimSearch(queryVec []float64, candidates map[string][]float64, threshold float64) (res []string, err error) {
	res = make([]string, 0, len(candidates))

	queryVecNorm, err := euclideanNorm(queryVec)
	if err != nil {
		logErr(err, "SimSearch")
		return nil, err
	}

	for id, candidate := range candidates {
		candidateNorm, err := euclideanNorm(candidate)
		if err != nil {
			logErr(err, "SimSearch")
			return nil, err
		}

		sim, err := cosineSim(queryVec, candidate, queryVecNorm, candidateNorm)
		if err != nil {
			logErr(err, "SimSearch")
			return nil, err
		}

		if sim >= threshold {
			res = append(res, id)
		}
	}

	return slices.Clip(res), nil
}

func cosineSim(vecA, vecB []float64, normA, normB float64) (sim float64, err error) {
	if normA <= EPSILON || normB <= EPSILON {
		return -1, nil
	}

	dp, err := dotProduct(vecA, vecB)
	if err != nil {
		return 0, err
	}

	sim = dp / (normA * normB)

	return sim, nil
}

func euclideanNorm(vec []float64) (float64, error) {
	var err error

	if len(vec) == 0 {
		err = new(errEmptyVector)
		logErr(err, "dotProduct")
		return 0, err
	}

	squaredEuclideanNorm, err := dotProduct(vec, vec)
	if err != nil {
		logErr(err, "EuclideanNorm")
		return 0, err
	}

	return math.Sqrt(squaredEuclideanNorm), nil
}

func dotProduct(vecA, vecB []float64) (res float64, err error) {
	if len(vecA) != len(vecB) {
		err = new(errVectorsNotSameLen)
		logErr(err, "dotProduct")
		return 0, err
	}

	for i := range vecA {
		res += vecA[i] * vecB[i]
	}

	return res, nil
}

func logErr(err error, trace string) {
	slog.LogAttrs(
		context.TODO(),
		slog.LevelError,
		err.Error(),
		slog.String("trace", "vectoria:src:internal:similarity:"+trace),
	)
}
