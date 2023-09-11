package semantic

type vectorsNotSameLenError struct{}

func (e *vectorsNotSameLenError) Error() string {
	return "vectors must have the same length"
}

type emptyVectorError struct{}

func (e *emptyVectorError) Error() string {
	return "vector cannot be empty"
}
