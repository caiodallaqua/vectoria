package lsh

import "fmt"

type invalidIDLenError struct {
	idLen int
}

func (e *invalidIDLenError) Error() string {
	return fmt.Sprintf("invalid ID length: %d", e.idLen)
}

type invalidNumSketchesError struct {
	expected uint32
	got      uint32
}

func (e *invalidNumSketchesError) Error() string {
	return fmt.Sprintf("invalid number of sketches (expected: %v, got: %v)", e.expected, e.got)
}

type invalidSketchLenError struct {
	expected uint32
	got      uint32
}

func (e *invalidSketchLenError) Error() string {
	return fmt.Sprintf("number of hyperplanes must match sketch length (expected: %v, got: %v)", e.expected, e.got)
}

type embeddingLenError struct {
	expected uint32
	got      uint32
}

func (e *embeddingLenError) Error() string {
	return fmt.Sprintf("embedding length must match space dimension (expected: %v, got: %v)", e.expected, e.got)
}

type invalidThresholdError struct {
	got float64
}

func (e *invalidThresholdError) Error() string {
	return fmt.Sprintf("expected threshold to be between 0 and 1, but got: %v", e.got)
}
