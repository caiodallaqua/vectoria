package semantic

import (
	"context"
	"log/slog"
	"math"
	"slices"
	"sort"
)

const EPSILON = 1e-10

type Semantic struct{}

type Contract interface {
	Search(queryVec []float64, candidates map[string][]float64, threshold float64, k uint32) (res []string, err error)
}

func New() *Semantic {
	return &Semantic{}
}

func (s *Semantic) Search(queryVec []float64, candidates map[string][]float64, threshold float64, k uint32) (ids []string, err error) {
	numCandidates := len(candidates)
	ids = make([]string, 0, numCandidates)
	simMap := make(map[string]float64, numCandidates)

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
			simMap[id] = sim
			ids = append(ids, id)
		}
	}

	sort.Slice(ids, func(i, j int) bool {
		return simMap[ids[i]] > simMap[ids[j]]
	})

	ids = slices.Clip(ids)

	if k == 0 || k > uint32(len(ids)) {
		return ids, nil
	}

	return ids[:k], nil
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
		err = new(emptyVectorError)
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
		err = new(vectorsNotSameLenError)
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
