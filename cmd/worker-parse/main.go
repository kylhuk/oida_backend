package main

import (
	"log"
	"time"
)

func main() {
	log.Println("worker-parse started")
	for {
		time.Sleep(30 * time.Second)
		log.Println("worker-parse idle")
	}
}
