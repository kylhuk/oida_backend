package aviation

import (
	"strings"
	"testing"
)

func TestDecodeStateVectorsUsesCanonicalSourceID(t *testing.T) {
	payload := `{"time":1710172800,"states":[["abc123","TEST123 ","Country",1710172790,1710172800,2.3522,48.8566,1000.0,false,250.0,90.0,0.0,null,1100.0,"7000",false,0,0]]}`
	vectors, err := DecodeStateVectors(strings.NewReader(payload))
	if err != nil {
		t.Fatalf("decode state vectors: %v", err)
	}
	if len(vectors) != 1 {
		t.Fatalf("expected one vector, got %d", len(vectors))
	}
	if len(vectors[0].Evidence) == 0 {
		t.Fatalf("expected evidence on decoded vector")
	}
	if vectors[0].Evidence[0].SourceID != DefaultOpenSkySourceID {
		t.Fatalf("expected canonical opensky source id %q, got %q", DefaultOpenSkySourceID, vectors[0].Evidence[0].SourceID)
	}
}

func TestDecodeStateVectorsWithSourceIDUsesProvidedSourceID(t *testing.T) {
	payload := `{"time":1710172800,"states":[["def456","TEST456 ","Country",1710172790,1710172800,8.6821,50.1109,1000.0,false,250.0,90.0,0.0,null,1100.0,"7000",false,0,0]]}`
	vectors, err := decodeStateVectorsWithSourceID(strings.NewReader(payload), "catalog:auto:aviation-airports-drones-and-mobility-opensky-network")
	if err != nil {
		t.Fatalf("decode state vectors with source id: %v", err)
	}
	if len(vectors) != 1 {
		t.Fatalf("expected one vector, got %d", len(vectors))
	}
	if vectors[0].Evidence[0].SourceID != "catalog:auto:aviation-airports-drones-and-mobility-opensky-network" {
		t.Fatalf("expected explicit query source id to be preserved")
	}
}
