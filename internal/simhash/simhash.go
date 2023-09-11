package simhash

import (
	"context"
	"log/slog"
	"math/rand"
	"strings"
)

const (
	POSITIVE_SIDE string = "1"
	NEGATIVE_SIDE string = "0"
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

func generateHyperplanes(numHyperPlanes, spaceDim uint32) (hyperPlanes [][]float64, err error) {
	if numHyperPlanes == 0 {
		err = new(numHyperPlanesError)
		logErr(err, "generateHyperplanes")
		return nil, err
	}

	if spaceDim == 0 {
		err = new(spaceDimError)
		logErr(err, "generateHyperplanes")
		return nil, err
	}

	hyperPlanes = make([][]float64, numHyperPlanes)

	for i := uint32(0); i < numHyperPlanes; i++ {
		hyperPlanes[i] = make([]float64, spaceDim)
		for j := uint32(0); j < spaceDim; j++ {
			hyperPlanes[i][j] = rand.NormFloat64()
		}
	}

	return hyperPlanes, nil
}

func (sh *SimHash) Sketch(embedding []float64) (string, error) {
	sk := make([]string, len(sh.Hyperplanes))

	for i, projectionVector := range sh.Hyperplanes {
		res, err := hash(projectionVector, embedding)
		if err != nil {
			logErr(err, "Sketch")
			return "", err
		}

		sk[i] = res
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
		slog.String("trace", "vectoria:src:internal:simhash:"+trace),
	)
}
