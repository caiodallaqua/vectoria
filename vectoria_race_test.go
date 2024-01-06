//go:build race

package vectoria

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRaceAdd(t *testing.T) {
	var (
		spaceDim  uint32 = 3
		indexName string = "fake-index-name"
	)

	db, err := New(DBConfig{
		Path: "",
		LSH: []LSHConfig{{
			IndexName: indexName,
			SpaceDim:  spaceDim,
		}},
	})
	assert.NoError(t, err)

	for i := 0; i <= 1000; i++ {
		go func() {
			itemVec := make([]float64, spaceDim)
			gofakeit.Slice(&itemVec)
			itemID := uuid.NewString()

			err := db.Add(itemID, itemVec, indexName)
			assert.NoError(t, err)
		}()
	}
}
