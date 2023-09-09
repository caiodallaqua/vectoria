package search

type errVectorsNotSameLen struct{}

func (e *errVectorsNotSameLen) Error() string {
	return "vectors must have the same length"
}

type errEmptyVector struct{}

func (e *errEmptyVector) Error() string {
	return "vector cannot be empty"
}
