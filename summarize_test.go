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

func TestSummaryLeaves(t *testing.T) {
	tests := []struct {
		name        string
		values      []*Leaf
		bucketCount int
		topCount    int
		want        *SummaryLeaf
	}{
		{
			name: "all numbers",
			values: []*Leaf{
				{Value: &TypeValue{Number: p(1.0)}},
				{Value: &TypeValue{Number: p(1.1)}},
			},
			bucketCount: 2,
			want: &SummaryLeaf{
				Numbers: &NumberSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{Number: p(1.0)}},
				{Value: &TypeValue{Number: p(2.0)}},
				{Value: &TypeValue{Number: p(2.0)}},
			},
			bucketCount: 2,
			want: &SummaryLeaf{
				Numbers: &NumberSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{Number: p(2.0)}},
			},
			bucketCount: 2,
			want: &SummaryLeaf{
				Numbers: &NumberSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{Number: p(2.43)}},
			},
			bucketCount: 2,
			want: &SummaryLeaf{
				Numbers: &NumberSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{Number: p(1.0)}},
				{Value: &TypeValue{Number: p(2.0)}},
				{Value: &TypeValue{Number: p(3.0)}},
				{Value: &TypeValue{Number: p(4.0)}},
				{Value: &TypeValue{Number: p(5.0)}},
				{Value: &TypeValue{Number: p(6.0)}},
				{Value: &TypeValue{Number: p(7.0)}},
				{Value: &TypeValue{Number: p(8.0)}},
				{Value: &TypeValue{Number: p(9.0)}},
			},
			bucketCount: 3,
			want: &SummaryLeaf{
				Numbers: &NumberSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{Number: p(1.0)}},
				{Value: &TypeValue{Number: p(2.0)}},
				{Value: &TypeValue{Number: p(3.0)}},
				{Value: &TypeValue{Number: p(1.0)}},
				{Value: &TypeValue{Number: p(2.0)}},
				{Value: &TypeValue{Number: p(3.0)}},
				{Value: &TypeValue{Number: p(1.0)}},
				{Value: &TypeValue{Number: p(2.0)}},
				{Value: &TypeValue{Number: p(9.0)}},
			},
			bucketCount: 10,
			want: &SummaryLeaf{
				Numbers: &NumberSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{Number: p(1.0)}},
				{Value: &TypeValue{Number: p(2.0)}},
				{Value: &TypeValue{Number: p(3.0)}},
				{Value: &TypeValue{Number: p(1.0)}},
				{Value: &TypeValue{Number: p(2.0)}},
				{Value: &TypeValue{Number: p(3.0)}},
				{Value: &TypeValue{Number: p(1.0)}},
				{Value: &TypeValue{Number: p(2.0)}},
				{Value: &TypeValue{Number: p(9.0)}},
			},
			bucketCount: 2,
			want: &SummaryLeaf{
				Numbers: &NumberSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{String: p("hello")}},
			},
			topCount: 5,
			want: &SummaryLeaf{
				Strings: &StringSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{String: p("hello")}},
				{Value: &TypeValue{String: p("hello")}},
				{Value: &TypeValue{String: p("hello")}},
				{Value: &TypeValue{String: p("world")}},
				{Value: &TypeValue{String: p("world")}},
				{Value: &TypeValue{String: p("la")}},
				{Value: &TypeValue{String: p("la")}},
				{Value: &TypeValue{String: p("planete")}},
			},
			topCount: 5,
			want: &SummaryLeaf{
				Strings: &StringSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{String: p("hello")}},
				{Value: &TypeValue{String: p("hello")}},
				{Value: &TypeValue{String: p("hello")}},
				{Value: &TypeValue{String: p("world")}},
				{Value: &TypeValue{String: p("world")}},
				{Value: &TypeValue{String: p("la")}},
				{Value: &TypeValue{String: p("la")}},
				{Value: &TypeValue{String: p("planete")}},
			},
			topCount: 2,
			want: &SummaryLeaf{
				Strings: &StringSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{String: p("bonjour")}},
				{Value: &TypeValue{String: p("le")}},
				{Value: &TypeValue{String: p("monde")}},
				{Value: &TypeValue{String: p("bonjour")}},
				{Value: &TypeValue{String: p("le")}},
				{Value: &TypeValue{String: p("monde")}},
				{Value: &TypeValue{String: p("bonjour")}},
				{Value: &TypeValue{String: p("le")}},
				{Value: &TypeValue{String: p("monde")}},
				{Value: &TypeValue{String: p("bonjour")}},
				{Value: &TypeValue{String: p("le")}},
				{Value: &TypeValue{String: p("monde")}},
				{Value: &TypeValue{String: p("bonjour")}},
				{Value: &TypeValue{String: p("le")}},
				{Value: &TypeValue{String: p("monde")}},
				{Value: &TypeValue{String: p("bonjour")}},
				{Value: &TypeValue{String: p("le")}},
				{Value: &TypeValue{String: p("monde")}},
			},
			topCount: 2,
			want: &SummaryLeaf{
				Strings: &StringSummaryLeaf{
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
			values: []*Leaf{
				{Value: &TypeValue{Bool: p(true)}},
				{Value: &TypeValue{Bool: p(true)}},
				{Value: &TypeValue{Bool: p(false)}},
				{Value: &TypeValue{Bool: p(true)}},
			},
			want: &SummaryLeaf{
				Bools: &BoolSummaryLeaf{
					Freq:      4,
					TrueFreq:  3,
					FalseFreq: 1,
				},
			},
		},
		{
			name: "nulls",
			values: []*Leaf{
				{Value: &TypeValue{Null: p(struct{}{})}},
				{Value: &TypeValue{Null: p(struct{}{})}},
				{Value: &TypeValue{Null: p(struct{}{})}},
				{Value: &TypeValue{Null: p(struct{}{})}},
			},
			want: &SummaryLeaf{
				Nulls: &NullSummaryLeaf{
					Freq: 4,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := summarizeLeaves(tt.values, tt.bucketCount, tt.topCount)
			require.Equal(t, tt.want, got)
		})
	}
}

func p[t any](v t) *t {
	return &v
}
