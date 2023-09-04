package simhash

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"strings"
)

const (
	POSITIVE_SIDE string = "1"
	NEGATIVE_SIDE string = "0"
)

var (
	errNumHyperPlanes    = errors.New("numHyperPlanes cannot be zero")
	errSpaceDim          = errors.New("spaceDim cannot be zero")
	errVectorsNotSameLen = errors.New("vectors must have the same length")
)

type Contract interface {
	Sketch(embedding []float64) (string, error)
}

type SimHash struct {
	Hyperplanes [][]float64
}

func New(numHyperPlanes, spaceDim uint32) (*SimHash, error) {
	hyperplanes, err := generateHyperplanes(numHyperPlanes, spaceDim)
	if err != nil {
		logErr(err, "New")
		return nil, err
	}

	return &SimHash{
		Hyperplanes: hyperplanes,
	}, nil
}

func generateHyperplanes(numHyperPlanes, spaceDim uint32) ([][]float64, error) {
	if numHyperPlanes == 0 {
		logErr(errNumHyperPlanes, "generateHyperplanes")
		return nil, errNumHyperPlanes
	}

	if spaceDim == 0 {
		logErr(errSpaceDim, "generateHyperplanes")
		return nil, errSpaceDim
	}

	hyperplanes := make([][]float64, numHyperPlanes)

	for i := uint32(0); i < numHyperPlanes; i++ {
		hyperplanes[i] = make([]float64, spaceDim)
		for j := uint32(0); j < spaceDim; j++ {
			hyperplanes[i][j] = rand.NormFloat64()
		}
	}

	return hyperplanes, nil
}

func (sh *SimHash) Sketch(embedding []float64) (string, error) {
	sk := make([]string, len(sh.Hyperplanes))

	for idx, projectionVector := range sh.Hyperplanes {
		res, err := hash(projectionVector, embedding)
		if err != nil {
			logErr(err, "Sketch")
			return "", err
		}

		sk[idx] = res
	}

	return strings.Join(sk, ""), nil
}

func hash(projectionVector, embedding []float64) (string, error) {
	res, err := dotProduct(projectionVector, embedding)
	if err != nil {
		logErr(err, "hash")
		return "", err
	}

	if res >= 0 {
		return POSITIVE_SIDE, nil
	}

	return NEGATIVE_SIDE, nil
}

func dotProduct(vecA, vecB []float64) (float64, error) {
	var res float64

	if len(vecA) != len(vecB) {
		logErr(errVectorsNotSameLen, "dotProduct")
		return 0, errVectorsNotSameLen
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
		slog.String("trace", "vectoria:src:internal:simhash:"+trace),
	)
}
