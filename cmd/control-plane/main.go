package main

import (
	"log"
	"time"
)

func main() {
	log.Println("control-plane started")
	for {
		time.Sleep(30 * time.Second)
		log.Println("control-plane tick")
	}
}
