package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/aybabtme/sumjson"
)

func main() {
	in := os.Stdin
	data, err := ioutil.ReadAll(in)
	if err != nil {
		log.Fatalf("reading data: %v", err)
	}
	log.Printf("decoded %d bytes, summarizing it", len(data))
	summary, err := sumjson.Summarize(data, reporter{})
	if err != nil {
		log.Fatalf("reading data: %v", err)
	}
	err = json.NewEncoder(os.Stdout).Encode(summary)
	if err != nil {
		log.Fatalf("encoding json: %v", err)
	}
}

type reporter struct{}

func (rp reporter) ObjectRead(from, to, total int) {
	boundary := total / 100
	fromBoundary := from % boundary
	toBoundary := to % boundary
	ridesOverBoundary := fromBoundary > toBoundary

	if ridesOverBoundary {
		percent := 100 * to / total
		log.Printf(`%d percent done (%d/%d)`, percent, to, total)
	}
}
