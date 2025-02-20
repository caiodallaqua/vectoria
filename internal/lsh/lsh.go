package lsh

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"maps"

	"github.com/caiodallaqua/vectoria/internal/semantic"
	"github.com/caiodallaqua/vectoria/internal/simhash"
	"github.com/caiodallaqua/vectoria/internal/storage"
)

const (
	MIN_NUM_ROUNDS uint32 = 1

	MIN_NUM_HYPERPLANES uint32 = 1

	MIN_SPACE_DIM uint32 = 2

	// MAX_BUCKET_SIZE = 100
)

type LSH struct {
	indexName string
	hashes    []simhash.SimHash
	kv        storage.Contract
	sem       semantic.Contract

	numRounds      uint32
	numHyperPlanes uint32
	spaceDim       uint32
}

func New(indexName string, kv storage.Contract, numRounds, numHyperPlanes, spaceDim uint32) (l *LSH, err error) {
	var sh *simhash.SimHash

	l = &LSH{
		indexName: indexName,
		kv:        kv,
		sem:       semantic.New(),
	}

	exists, err := l.indexExists()
	if err != nil {
		return nil, err
	}

	if exists {
		if err := l.getStoredConfig(); err != nil {
			return nil, err
		}

		return l, nil
	}

	l.setHyperParams(numRounds, numHyperPlanes, spaceDim)

	hashes := make([]simhash.SimHash, l.numRounds)
	for i := uint32(0); i < l.numRounds; i++ {
		sh, err = simhash.New(l.numHyperPlanes, l.spaceDim)
		if err != nil {
			logErr(err, "New")
			return nil, err
		}

		hashes[i] = *sh
	}

	l.hashes = hashes

	if err := l.storeConfig(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *LSH) indexExists() (exists bool, err error) {
	exists, err = l.kv.KeyExists(getIndexKey(l.indexName))
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (l *LSH) storeConfig() (err error) {
	var data = make(map[string][]byte, 4+len(l.hashes))

	data = map[string][]byte{
		getIndexKey(l.indexName):          []byte(""),
		getNumRoundsKey(l.indexName):      encodeUInt32(l.numRounds),
		getNumHyperPlanesKey(l.indexName): encodeUInt32(l.numHyperPlanes),
		getSpaceDimKey(l.indexName):       encodeUInt32(l.spaceDim),
	}

	for i, hash := range l.hashes {
		hyperplanes, err := encodeFloat64Slice2D(hash.Hyperplanes)
		if err != nil {
			return err
		}
		data[getHyperPlanesKey(l.indexName, i)] = hyperplanes
	}

	if err := l.kv.Add(data); err != nil {
		return err
	}

	return nil
}

func (l *LSH) getStoredConfig() error {
	numRoundsKey := getNumRoundsKey(l.indexName)
	encodedNumRounds, err := l.kv.Get(numRoundsKey)
	if err != nil {
		return err
	}

	numHyperPlanesKey := getNumHyperPlanesKey(l.indexName)
	encodedNumHyperPlanes, err := l.kv.Get(numHyperPlanesKey)
	if err != nil {
		return err
	}

	spaceDimKey := getSpaceDimKey(l.indexName)
	encodedSpaceDim, err := l.kv.Get(spaceDimKey)
	if err != nil {
		return err
	}

	l.numHyperPlanes = binary.LittleEndian.Uint32(encodedNumHyperPlanes)
	l.numRounds = binary.LittleEndian.Uint32(encodedNumRounds)
	l.spaceDim = binary.LittleEndian.Uint32(encodedSpaceDim)

	l.hashes = make([]simhash.SimHash, l.numRounds)

	for i := 0; i < int(l.numRounds); i++ {
		encodedHyperPlanes, err := l.kv.Get(getHyperPlanesKey(l.indexName, i))
		if err != nil {
			return err
		}

		hyperPlanes, err := decodeFloat64Slice2D(encodedHyperPlanes, l.spaceDim)
		if err != nil {
			return err
		}

		l.hashes[i].Hyperplanes = hyperPlanes
	}

	return nil
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

func (l *LSH) Get(queryVec []float64, threshold float64, k uint32) (neighbors []string, err error) {
	if err := l.checkGetParams(queryVec, threshold); err != nil {
		logErr(err, "Get")
		return nil, err
	}

	sks, err := l.getSketches(queryVec)
	if err != nil {
		logErr(err, "Get")
		return nil, err
	}

	candidates, err := l.getEmbeddingsFromBuckets(sks)
	if err != nil {
		logErr(err, "Get")
		return nil, err
	}

	neighbors, err = l.sem.Search(queryVec, candidates, threshold, k)
	if err != nil {
		logErr(err, "Get")
		return nil, err
	}

	return neighbors, nil
}

func (l *LSH) checkGetParams(queryVec []float64, threshold float64) error {
	if err := l.checkEmbedding(queryVec); err != nil {
		logErr(err, "checkGetParams")
		return err
	}

	if err := l.checkThreshold(threshold); err != nil {
		logErr(err, "checkGetParams")
		return err
	}

	return nil
}

func (l *LSH) Info() map[string]any {
	return map[string]any{
		"numRounds":      l.numRounds,
		"numHyperPlanes": l.numHyperPlanes,
		"spaceDim":       l.spaceDim,
	}
}

func (l *LSH) getEmbeddingsFromBuckets(sks []string) (map[string][]float64, error) {
	var (
		ids   []string
		err   error
		ok    bool
		embed []float64
	)

	data := make(map[string][]float64)

	for _, sk := range sks {
		ids, err = l.getBucketIDs(sk)
		if err != nil {
			logErr(err, "getEmbeddingsFromBuckets")
			return nil, err
		}

		for _, id := range ids {
			if _, ok = data[id]; !ok {
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
	encodedIDs, err := l.kv.GetWithPrefix(getSketchPrefixKey(l.indexName, sk))
	if err != nil {
		logErr(err, "getBucketIDs")
		return nil, err
	}

	ids := make([]string, len(encodedIDs))

	for i, encodedID := range encodedIDs {
		ids[i] = string(encodedID)
	}

	return ids, nil
}

func (l *LSH) getEmbedding(id string) ([]float64, error) {
	encodedEmbed, err := l.kv.Get(getEmbeddingKey(l.indexName, id))
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
	data[getEmbeddingKey(l.indexName, id)] = encodedEmbed

	return data, nil
}

func (l *LSH) prepareSketches(id string, sks []string) (data map[string][]byte, err error) {
	if len(id) == 0 {
		err = &invalidIDLenError{len(id)}
		logErr(err, "prepareSketches")
		return nil, err
	}

	if err = l.checkSketches(sks); err != nil {
		logErr(err, "prepareSketches")
		return nil, err
	}

	data = make(map[string][]byte, len(sks))

	for _, sk := range sks {
		data[getSketchKey(l.indexName, sk, id)] = []byte(id)
	}

	return data, nil
}

func (l *LSH) checkEmbedding(embedding []float64) error {
	lenEmbedding := uint32(len(embedding))

	if l.spaceDim != lenEmbedding {
		err := &embeddingLenError{l.spaceDim, lenEmbedding}
		logErr(err, "checkEmbedding")
		return err
	}

	return nil
}

func (l *LSH) checkThreshold(threshold float64) error {
	if threshold < 0 || threshold > 1 {
		err := &invalidThresholdError{threshold}
		logErr(err, "checkThreshold")
		return err
	}

	return nil
}

func (l *LSH) checkSketches(sks []string) error {
	var (
		lenSk  uint32
		lenSks = uint32(len(sks))
		err    error
	)

	if l.numRounds != lenSks {
		err = &invalidNumSketchesError{l.numRounds, lenSks}
		logErr(err, "checkSketches")
		return err
	}

	for _, sk := range sks {
		lenSk = uint32(len(sk))
		if l.numHyperPlanes != lenSk {
			err = &invalidSketchLenError{l.numHyperPlanes, lenSk}
			logErr(err, "checkSketches")
			return err
		}
	}

	return nil
}

func encodeUInt32(val uint32) []byte {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, val)

	return data
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

func encodeFloat64Slice2D(slice [][]float64) ([]byte, error) {
	var err error
	buf := new(bytes.Buffer)

	for _, row := range slice {
		for _, val := range row {
			if err = binary.Write(buf, binary.LittleEndian, val); err != nil {
				logErr(err, "encodeFloat64Slice2D")
				return nil, err
			}
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

func decodeFloat64Slice2D(data []byte, numCols uint32) ([][]float64, error) {
	var (
		result [][]float64
		val    float64
	)

	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		var row []float64
		for i := uint32(0); i < numCols; i++ {
			if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
				logErr(err, "decodeFloat64Slice2D")
				return nil, err
			}
			row = append(row, val)
		}
		result = append(result, row)
	}

	return result, nil
}

func (l *LSH) getSketches(embedding []float64) ([]string, error) {
	var (
		sk  string
		err error
	)

	sketches := make([]string, len(l.hashes))

	for i, hash := range l.hashes {
		sk, err = hash.Sketch(embedding)
		if err != nil {
			logErr(err, "getSketches")
			return nil, err
		}

		sketches[i] = sk
	}

	return sketches, nil
}

func (l *LSH) setHyperParams(numRounds, numHyperplanes, spaceDim uint32) {
	if numRounds < MIN_NUM_ROUNDS {
		numRounds = MIN_NUM_ROUNDS
	}

	if numHyperplanes < MIN_NUM_HYPERPLANES {
		numHyperplanes = MIN_NUM_HYPERPLANES
	}

	if spaceDim < MIN_NUM_HYPERPLANES {
		spaceDim = MIN_SPACE_DIM
	}

	l.numRounds = numRounds
	l.numHyperPlanes = numHyperplanes
	l.spaceDim = spaceDim
}

func logErr(err error, trace string) {
	slog.LogAttrs(
		context.TODO(),
		slog.LevelError,
		err.Error(),
		slog.String("trace", "vectoria:src:internal:lsh:"+trace),
	)
}
