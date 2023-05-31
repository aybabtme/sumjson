package sumjson

import (
	"io/ioutil"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSummarize(t *testing.T) {
	data, err := ioutil.ReadFile("dump.json")
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
func (rp *reporter) Summarized(done, total int) {
	percent := 100 * done / total
	log.Printf(`%d percent done (%d/%d)`, percent, done, total)
}

func TestSummaryScalars(t *testing.T) {
	tests := []struct {
		name        string
		node        *Node
		bucketCount int
		topCount    int
		want        *SummaryNode
	}{
		{
			name: "all numbers",
			node: &Node{
				ScalarNumbers: []float64{1.0, 1.1},
			},
			bucketCount: 2,
			want: &SummaryNode{
				Numbers: &NumberSummaryNode{
					Freq:    2,
					AllInts: false,
					Min:     1,
					Max:     1.1,
					Unique:  2,

					Distribution: []BucketRange{
						{From: 1.0, To: 1.0, Freq: 1},
						{From: 1.1, To: 1.1, Freq: 1},
					},
				},
			},
		},
		{
			name: "all ints",
			node: &Node{
				ScalarNumbers: []float64{1.0, 2.0, 2.0},
			},
			bucketCount: 2,
			want: &SummaryNode{
				Numbers: &NumberSummaryNode{
					Freq:    3,
					AllInts: true,
					Min:     1,
					Max:     2,
					Unique:  2,
					Distribution: []BucketRange{
						{From: 1.0, To: 1.0, Freq: 1},
						{From: 2.0, To: 2.0, Freq: 2},
					},
				},
			},
		},
		{
			name: "one int",
			node: &Node{
				ScalarNumbers: []float64{2.0},
			},
			bucketCount: 2,
			want: &SummaryNode{
				Numbers: &NumberSummaryNode{
					Freq:    1,
					AllInts: true,
					Min:     2,
					Max:     2,
					Unique:  1,
					Distribution: []BucketRange{
						{From: 2.0, To: 2.0, Freq: 1},
					},
				},
			},
		},
		{
			name: "one number",
			node: &Node{
				ScalarNumbers: []float64{2.43},
			},
			bucketCount: 2,
			want: &SummaryNode{
				Numbers: &NumberSummaryNode{
					Freq:    1,
					AllInts: false,
					Min:     2.43,
					Max:     2.43,
					Unique:  1,
					Distribution: []BucketRange{
						{From: 2.43, To: 2.43, Freq: 1},
					},
				},
			},
		},
		{
			name: "larger dist",
			node: &Node{ScalarNumbers: []float64{
				1.0,
				2.0,
				3.0,
				4.0,
				5.0,
				6.0,
				7.0,
				8.0,
				9.0,
			}},
			bucketCount: 3,
			want: &SummaryNode{
				Numbers: &NumberSummaryNode{
					Freq:    9,
					AllInts: true,
					Min:     1.0,
					Max:     9.0,
					Unique:  9,
					Distribution: []BucketRange{
						{From: 1.0, To: 3.0, Freq: 3},
						{From: 4.0, To: 6.0, Freq: 3},
						{From: 7.0, To: 9.0, Freq: 3},
					},
				},
			},
		},
		{
			name: "skewed dist",
			node: &Node{ScalarNumbers: []float64{
				1.0,
				2.0,
				3.0,
				1.0,
				2.0,
				3.0,
				1.0,
				2.0,
				9.0,
			}},
			bucketCount: 10,
			want: &SummaryNode{
				Numbers: &NumberSummaryNode{
					Freq:    9,
					AllInts: true,
					Min:     1.0,
					Max:     9.0,
					Unique:  4,
					Distribution: []BucketRange{
						{From: 1.0, To: 1.0, Freq: 3},
						{From: 2.0, To: 2.0, Freq: 3},
						{From: 3.0, To: 3.0, Freq: 2},
						{From: 9.0, To: 9.0, Freq: 1},
					},
				},
			},
		},
		{
			name: "skewed dist - few buckets",
			node: &Node{ScalarNumbers: []float64{
				1.0,
				2.0,
				3.0,
				1.0,
				2.0,
				3.0,
				1.0,
				2.0,
				9.0,
			}},
			bucketCount: 2,
			want: &SummaryNode{
				Numbers: &NumberSummaryNode{
					Freq:    9,
					AllInts: true,
					Min:     1.0,
					Max:     9.0,
					Unique:  4,
					Distribution: []BucketRange{
						{From: 1.0, To: 2.0, Freq: 6},
						{From: 3.0, To: 9.0, Freq: 3},
					},
				},
			},
		},
		{
			name: "one string",
			node: &Node{ScalarStrings: []string{
				"hello",
			}},
			topCount: 5,
			want: &SummaryNode{
				Strings: &StringSummaryNode{
					Freq:   1,
					Unique: 1,
					MinLen: len("hello"),
					MaxLen: len("hello"),
					Top: []StringSample{
						{
							Value: "hello",
							Freq:  1,
						},
					},
				},
			},
		},
		{
			name: "less than top strings",
			node: &Node{ScalarStrings: []string{
				"hello",
				"hello",
				"hello",
				"world",
				"world",
				"la",
				"la",
				"planete",
			}},
			topCount: 5,
			want: &SummaryNode{
				Strings: &StringSummaryNode{
					Freq:   8,
					Unique: 4,
					MinLen: len("la"),
					MaxLen: len("planete"),
					Top: []StringSample{
						{
							Value: "hello",
							Freq:  3,
						},
						{
							Value: "world",
							Freq:  2,
						},
						{
							Value: "la",
							Freq:  2,
						},
						{
							Value: "planete",
							Freq:  1,
						},
					},
				},
			},
		},
		{
			name: "more than top strings",
			node: &Node{ScalarStrings: []string{
				"hello",
				"hello",
				"hello",
				"world",
				"world",
				"la",
				"la",
				"planete",
			}},
			topCount: 2,
			want: &SummaryNode{
				Strings: &StringSummaryNode{
					Freq:   8,
					Unique: 4,
					MinLen: len("la"),
					MaxLen: len("planete"),
					Top: []StringSample{
						{
							Value: "hello",
							Freq:  3,
						},
						{
							Value: "world",
							Freq:  2,
						},
					},
				},
			},
		},
		{
			name: "all same frequencies, by order of length",
			node: &Node{ScalarStrings: []string{
				"bonjour",
				"le",
				"monde",
				"bonjour",
				"le",
				"monde",
				"bonjour",
				"le",
				"monde",
				"bonjour",
				"le",
				"monde",
				"bonjour",
				"le",
				"monde",
				"bonjour",
				"le",
				"monde",
			}},
			topCount: 2,
			want: &SummaryNode{
				Strings: &StringSummaryNode{
					Freq:   18,
					Unique: 3,
					MinLen: len("le"),
					MaxLen: len("bonjour"),
					Top: []StringSample{
						{
							Value: "bonjour",
							Freq:  6,
						},
						{
							Value: "monde",
							Freq:  6,
						},
					},
				},
			},
		},
		{
			name: "bools",
			node: &Node{ScalarBools: []bool{
				true,
				true,
				false,
				true,
			}},
			want: &SummaryNode{
				Bools: &BoolSummaryNode{
					Freq:      4,
					TrueFreq:  3,
					FalseFreq: 1,
				},
			},
		},
		{
			name: "nulls",
			node: &Node{ScalarNulls: 4},
			want: &SummaryNode{
				Nulls: &NullSummaryNode{
					Freq: 4,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := summarizeNode(tt.node, tt.bucketCount, tt.topCount)
			require.Equal(t, tt.want, got)
		})
	}
}
