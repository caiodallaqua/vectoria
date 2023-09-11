package simhash

type numHyperPlanesError struct{}

func (e *numHyperPlanesError) Error() string {
	return "numHyperPlanes cannot be zero"
}

type spaceDimError struct{}

func (e *spaceDimError) Error() string {
	return "spaceDim cannot be zero"
}

type vectorsNotSameLenError struct{}

func (e *vectorsNotSameLenError) Error() string {
	return "vectors must have the same length"
}
