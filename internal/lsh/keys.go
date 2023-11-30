package lsh

import (
	"strconv"
	"strings"
)

func key(elems ...string) string {
	return strings.Join(elems, "/")
}

func getIndexKey(indexName string) string {
	return key("index", indexName)
}

func getEmbeddingKey(indexName string, id string) string {
	return key(getIndexKey(indexName), "embedding", id)
}

func getSketchKey(indexName, sketch, id string) string {
	return key(getSketchPrefixKey(indexName, sketch), id)
}

func getSketchPrefixKey(indexName, sketch string) string {
	return key(getIndexKey(indexName), "sketch", sketch)
}

func getNumRoundsKey(indexName string) string {
	return key(getIndexKey(indexName), "num_rounds")
}

func getNumHyperPlanesKey(indexName string) string {
	return key(getIndexKey(indexName), "num_hyperplanes")
}

func getSpaceDimKey(indexName string) string {
	return key(getIndexKey(indexName), "space_dim")
}

func getHyperPlanesKey(indexName string, hashIdx int) string {
	return key(getIndexKey(indexName), "hash", strconv.Itoa(hashIdx), "hyperplanes")
}
