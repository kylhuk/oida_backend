package main

import (
	"log"
	"time"
)

func main() {
	log.Println("worker-fetch started")
	for {
		time.Sleep(30 * time.Second)
		log.Println("worker-fetch idle")
	}
}
