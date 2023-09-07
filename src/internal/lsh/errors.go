package lsh

import "fmt"

type errNumRounds struct{}

func (e *errNumRounds) Error() string {
	return fmt.Sprintf("invalid value for numRounds (it must be between %d and %d)", MIN_NUM_ROUNDS, MAX_NUM_ROUNDS)
}

type errNumHyperPlanes struct{}

func (e *errNumHyperPlanes) Error() string {
	return fmt.Sprintf("invalid value for numHyperplanes (it must be at least %d)", MIN_NUM_HYPERPLANES)
}

type errSpaceDim struct{}

func (e *errSpaceDim) Error() string {
	return fmt.Sprintf("invalid value for spaceDim (it must be at least %d)", MIN_SPACE_DIM)
}

type errInvalidIDLen struct {
	idLen int
}

func (e *errInvalidIDLen) Error() string {
	return fmt.Sprintf("invalid ID length: %d", e.idLen)
}

type errInvalidNumSketches struct {
	expected uint32
	got      uint32
}

func (e *errInvalidNumSketches) Error() string {
	return fmt.Sprintf("invalid number of sketches (expected: %v, got: %v)", e.expected, e.got)
}

type errInvalidSketchLen struct {
	expected uint32
	got      uint32
}

func (e *errInvalidSketchLen) Error() string {
	return fmt.Sprintf("number of hyperplanes must match sketch length (expected: %v, got: %v)", e.expected, e.got)
}

type errEmbeddingLen struct {
	expected uint32
	got      uint32
}

func (e *errEmbeddingLen) Error() string {
	return fmt.Sprintf("space dimension must match embedding length(expected: %v, got: %v)", e.expected, e.got)
}
