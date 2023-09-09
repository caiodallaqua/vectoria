package lsh

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"maps"
	"strings"

	"vectoria/src/internal/search"
	"vectoria/src/internal/simhash"
	"vectoria/src/internal/storage"
)

const (
	MIN_NUM_ROUNDS uint32 = 1
	MAX_NUM_ROUNDS uint32 = 100

	MIN_NUM_HYPERPLANES uint32 = 1

	MIN_SPACE_DIM uint32 = 2

	// MAX_BUCKET_SIZE = 100
)

type LSH struct {
	hashes []simhash.SimHash
	kv     storage.Contract
	srch   search.Contract

	numRounds      uint32
	numHyperPlanes uint32
	spaceDim       uint32
}

func New(storage storage.Contract, numRounds, numHyperPlanes, spaceDim uint32) (*LSH, error) {
	var (
		sh  *simhash.SimHash
		err error
	)

	if err = validateHyperParams(numRounds, numHyperPlanes, spaceDim); err != nil {
		logErr(err, "New")
		return nil, err
	}

	hashes := make([]simhash.SimHash, numRounds)
	for idx := uint32(0); idx < numRounds; idx++ {
		sh, err = simhash.New(numHyperPlanes, spaceDim)
		if err != nil {
			logErr(err, "New")
			return nil, err
		}

		hashes[idx] = *sh
	}

	return &LSH{
		hashes:         hashes,
		kv:             storage,
		srch:           search.New(),
		numRounds:      numRounds,
		numHyperPlanes: numHyperPlanes,
		spaceDim:       spaceDim,
	}, nil
}

func (l *LSH) Add(id string, embedding []float64) error {
	embedData, err := l.prepareEmbedding(id, embedding)
	if err != nil {
		logErr(err, "Add")
		return err
	}

	sks, err := l.getSketches(embedding)
	if err != nil {
		logErr(err, "Add")
		return err
	}

	sksData, err := l.prepareSketches(id, sks)
	if err != nil {
		logErr(err, "Add")
		return err
	}

	data := make(map[string][]byte, len(embedData)+len(sksData))
	maps.Copy(data, embedData)
	maps.Copy(data, sksData)

	if err = l.kv.Add(data); err != nil {
		logErr(err, "Add")
		return err
	}

	return nil
}

func (l *LSH) GetNeighbors(queryVec []float64, threshold float64) (neighbors []string, err error) {
	if err = l.checkEmbedding(queryVec); err != nil {
		logErr(err, "GetNeighbors")
		return nil, err
	}

	sks, err := l.getSketches(queryVec)
	if err != nil {
		logErr(err, "GetNeighbors")
		return nil, err
	}

	candidates, err := l.getEmbeddingsFromBuckets(sks)
	if err != nil {
		logErr(err, "GetNeighbors")
		return nil, err
	}

	neighbors, err = l.srch.SimSearch(queryVec, candidates, threshold)
	if err != nil {
		logErr(err, "GetNeighbors")
		return nil, err
	}

	return neighbors, nil
}

func (l *LSH) getEmbeddingsFromBuckets(sks []string) (map[string][]float64, error) {
	var (
		ids    []string
		err    error
		exists bool
		embed  []float64
	)

	data := make(map[string][]float64)

	for _, sk := range sks {
		ids, err = l.getBucketIDs(sk)
		if err != nil {
			logErr(err, "getEmbeddingsFromBuckets")
			return nil, err
		}

		for _, id := range ids {
			if _, exists = data[id]; !exists {
				embed, err = l.getEmbedding(id)
				if err != nil {
					logErr(err, "getEmbeddingsFromBuckets")
					return nil, err
				}
				data[id] = embed
			}
		}
	}

	return data, nil
}

func (l *LSH) getBucketIDs(sk string) ([]string, error) {
	encodedIDs, err := l.kv.GetWithPrefix(sk)
	if err != nil {
		logErr(err, "getBucketIDs")
		return nil, err
	}

	ids := make([]string, len(encodedIDs))

	for idx, encodedID := range encodedIDs {
		ids[idx] = string(encodedID)
	}

	return ids, nil
}

func (l *LSH) getEmbedding(id string) ([]float64, error) {
	encodedEmbed, err := l.kv.Get(key("embedding", id))
	if err != nil {
		logErr(err, "getEmbedding")
		return nil, err
	}

	embed, err := decodeFloat64Slice(encodedEmbed)
	if err != nil {
		logErr(err, "getEmbedding")
		return nil, err
	}

	return embed, nil
}

func key(elems ...string) string {
	return strings.Join(elems, "/")
}

func (l *LSH) prepareEmbedding(id string, embedding []float64) (data map[string][]byte, err error) {
	if err = l.checkEmbedding(embedding); err != nil {
		logErr(err, "prepareEmbedding")
		return nil, err
	}

	encodedEmbed, err := encodeFloat64Slice(embedding)
	if err != nil {
		logErr(err, "prepareEmbedding")
		return nil, err
	}

	data = make(map[string][]byte, 1)
	data[key("embedding", id)] = encodedEmbed

	return data, nil
}

func (l *LSH) prepareSketches(id string, sks []string) (data map[string][]byte, err error) {
	if len(id) == 0 {
		err = &errInvalidIDLen{len(id)}
		logErr(err, "prepareSketches")
		return nil, err
	}

	if err = l.checkSketches(sks); err != nil {
		logErr(err, "prepareSketches")
		return nil, err
	}

	data = make(map[string][]byte, len(sks))

	for _, sk := range sks {
		data[key(sk, id)] = []byte(id)
	}

	return data, nil
}

func (l *LSH) checkEmbedding(embedding []float64) (err error) {
	lenEmbedding := uint32(len(embedding))

	if l.spaceDim != lenEmbedding {
		err = &errEmbeddingLen{l.spaceDim, lenEmbedding}
		logErr(err, "checkEmbedding")
		return err
	}

	return nil
}

func (l *LSH) checkSketches(sks []string) (err error) {
	var (
		lenSk  uint32
		lenSks = uint32(len(sks))
	)

	if l.numRounds != lenSks {
		err = &errInvalidNumSketches{l.numRounds, lenSks}
		logErr(err, "checkSketches")
		return err
	}

	for _, sk := range sks {
		lenSk = uint32(len(sk))
		if l.numHyperPlanes != lenSk {
			err = &errInvalidSketchLen{l.numHyperPlanes, lenSk}
			logErr(err, "checkSketches")
			return err
		}
	}

	return nil
}

func encodeFloat64Slice(slice []float64) ([]byte, error) {
	var err error
	buf := new(bytes.Buffer)

	for _, val := range slice {
		if err = binary.Write(buf, binary.LittleEndian, val); err != nil {
			logErr(err, "encodeFloat64Slice")
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func decodeFloat64Slice(data []byte) ([]float64, error) {
	var (
		result []float64
		val    float64
	)

	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
			logErr(err, "decodeFloat64Slice")
			return nil, err
		}
		result = append(result, val)
	}

	return result, nil
}

func (l *LSH) getSketches(embedding []float64) ([]string, error) {
	var (
		sk  string
		err error
	)

	sketches := make([]string, len(l.hashes))

	for idx, hash := range l.hashes {
		sk, err = hash.Sketch(embedding)
		if err != nil {
			logErr(err, "getSketches")
			return nil, err
		}

		sketches[idx] = sk
	}

	return sketches, nil
}

func validateHyperParams(numRounds, numHyperplanes, spaceDim uint32) (err error) {
	if numRounds < MIN_NUM_ROUNDS || numRounds > MAX_NUM_ROUNDS {
		err = errors.Join(new(errNumRounds), err)
	}

	if numHyperplanes < MIN_NUM_HYPERPLANES {
		err = errors.Join(new(errNumHyperPlanes))
	}

	if spaceDim < MIN_SPACE_DIM {
		err = errors.Join(new(errSpaceDim), err)
	}

	if err != nil {
		logErr(err, "validateHyperParams")
		return err
	}

	return nil
}

func logErr(err error, trace string) {
	slog.LogAttrs(
		context.TODO(),
		slog.LevelError,
		err.Error(),
		slog.String("trace", "vectoria:src:internal:lsh:"+trace),
	)
}
