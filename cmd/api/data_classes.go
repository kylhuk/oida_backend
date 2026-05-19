package main

import (
	"encoding/json"
	"log"
	"os"
)

type dataClassEntry struct {
	Kind        string `json:"kind"`
	DataClass   string `json:"data_class"`
	Category    string `json:"category,omitempty"`
	Description string `json:"description,omitempty"`
}

// loadDataClassesSeed loads operator-curated metadata for entity/place data classes.
// Returns an empty map (and logs once) if the file is missing or unreadable.
func loadDataClassesSeed(path string) map[string]dataClassEntry {
	result := make(map[string]dataClassEntry)
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("data_classes seed not found at %s (category/description will be absent): %v", path, err)
		return result
	}
	var entries []dataClassEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		log.Printf("data_classes seed parse error: %v", err)
		return result
	}
	for _, e := range entries {
		result[e.Kind+":"+e.DataClass] = e
	}
	return result
}
