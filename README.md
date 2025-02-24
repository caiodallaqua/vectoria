# Vectoria

Vectoria is an embedded vector database for simple use cases. It implements LSH (Locality-Sensitive Hashing).

Here’s a complete example:
```go
package main

import (
	"log"

	"github.com/caiodallaqua/vectoria"
)

func main() {
	var (
		indexName = "hello-world"
		// Vector to be stored.
		vec = []float64{1.1, 0.2}
		// Key to be retrieved.
		key = "my-key"

		// Let's add a small deviation to vec,
		// so we can see a match.
		queryVec = []float64{vec[0] + 0.1, vec[1] + 0.1}
		// Only similarities >= thresold.
		threshold = 0.9
		// Upper limit on how many items to retrieve.
		k uint32 = 1
	)

	dbConfig := vectoria.DBConfig{
		Path: "", // In-memory storage.
		LSH: []vectoria.LSHConfig{{
			IndexName:      indexName,
			NumRounds:      5,
			NumHyperPlanes: 2,
			SpaceDim:       2, // Length of your vectors.
		}},
	}

	db, err := vectoria.New(dbConfig)
	if err != nil {
		log.Fatalf("unable to create database: %s", err)
		return
	}

	if err = db.Add(key, vec, indexName); err != nil {
		log.Fatalf("unable to add key: %s", err)
		return
	}

	res, err := db.Get(queryVec, threshold, k, indexName)
	if err != nil {
		log.Fatalf("unable to get key: %s", err)
		return
	}

	log.Println("retrieval results:")
	for index, key := range res {
		log.Printf("\tindex: %v", index)
		log.Printf("\tkeys: %v", key)
	}
}
```

### License

[Apache License 2.0](LICENSE)