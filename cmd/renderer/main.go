package main

import (
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	log.Println("renderer controller listening on :8090")
	log.Fatal(http.ListenAndServe(":8090", nil))
}
