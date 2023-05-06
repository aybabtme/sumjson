package sumjson

import (
	"io/ioutil"
	"log"
	"testing"
)

func TestSummarize(t *testing.T) {
	data, err := ioutil.ReadFile("../dump.json")
	if err != nil {
		t.Fatal(err)
	}
	sum, err := Summarize(data, &reporter{})
	if err != nil {
		t.Fatal(err)
	}
	_ = sum
}

type reporter struct{}

func (rp *reporter) ObjectRead(from, to, total int) {
	boundary := total / 100
	fromBoundary := from % boundary
	toBoundary := to % boundary
	ridesOverBoundary := fromBoundary > toBoundary

	if ridesOverBoundary {
		percent := 100 * to / total
		log.Printf(`%d percent done (%d/%d)`, percent, to, total)
	}
}
