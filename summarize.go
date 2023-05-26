package sumjson

import (
	"sort"

	"github.com/aybabtme/flatjson"
	"golang.org/x/exp/maps"
)

type Summary struct {
	Root *Node `json:"root"`
}

type Reporter interface {
	ObjectRead(from, to, total int)
}

func Summarize(data []byte, reporter Reporter) (*Summary, error) {
	summary := &Summary{Root: new(Node)}
	cb := &flatjson.Callbacks{
		MaxDepth: 99,
		OnNumber: func(prefixes flatjson.Prefixes, val flatjson.Number) {
			name := val.Name.String(data)
			summary.atKey(data, prefixes, name, func(node *Node) {
				tval := &TypeValue{Number: &val.Value}
				node.atLeaf(tval, func(l *Leaf) {
					// do something?
				})
			})
		},
		OnString: func(prefixes flatjson.Prefixes, val flatjson.String) {
			name := val.Name.String(data)
			summary.atKey(data, prefixes, name, func(node *Node) {
				v := val.Value.String(data)
				tval := &TypeValue{String: &v}
				node.atLeaf(tval, func(l *Leaf) {
					// do something?
				})
			})
		},
		OnBoolean: func(prefixes flatjson.Prefixes, val flatjson.Bool) {
			name := val.Name.String(data)
			summary.atKey(data, prefixes, name, func(node *Node) {
				tval := &TypeValue{Bool: &val.Value}
				node.atLeaf(tval, func(l *Leaf) {
					// do something?
				})
			})
		},
		OnNull: func(prefixes flatjson.Prefixes, val flatjson.Null) {
			name := val.Name.String(data)
			summary.atKey(data, prefixes, name, func(node *Node) {
				tval := &TypeValue{Null: &struct{}{}}
				node.atLeaf(tval, func(l *Leaf) {
					// do something?
				})
			})
		},
	}
	for i := 0; i < len(data); {
		pos, ok, err := flatjson.ScanObject(data, i, cb)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		summary.Root.Freq++
		if reporter != nil {
			reporter.ObjectRead(i, pos.To, len(data))
		}
		i = pos.To
	}
	return summary, nil
}

func (sum *Summary) atKey(data []byte, prefixes flatjson.Prefixes, key string, action func(node *Node)) {
	sum.atKeyIter(sum.Root, data, prefixes, key, action)
}

func (sum *Summary) atKeyIter(parent *Node, data []byte, prefixes flatjson.Prefixes, key string, action func(node *Node)) {
	if len(prefixes) != 0 {
		next := prefixes[0]
		nextKey := next.String(data)
		for _, child := range parent.Children {
			if child.Key == nextKey {
				child.Freq++
				sum.atKeyIter(child, data, prefixes[1:], key, action)
				return
			}
		}
		child := &Node{
			Freq: 1,
			Key:  nextKey,
		}
		parent.Children = append(parent.Children, child)
		sum.atKeyIter(child, data, prefixes[1:], key, action)
		return
	}

	for _, child := range parent.Children {
		if child.Key == key {
			child.Freq++
			action(child)
			return
		}
	}

	child := &Node{
		Freq: 1,
		Key:  key,
	}
	parent.Children = append(parent.Children, child)
	action(child)
}

type Node struct {
	Key      string  `json:"k,omitempty"`
	Children []*Node `json:"c,omitempty"`

	// stats
	Freq   int     `json:"n"`
	Leaves []*Leaf `json:"lvs,omitempty"`
}

func (nd *Node) atLeaf(tval *TypeValue, action func(*Leaf)) {
	for _, leaf := range nd.Leaves {
		if leaf.Value.Equal(tval) {
			leaf.Freq++
			action(leaf)
			return
		}
	}
	missLeaf := &Leaf{Freq: 1, Value: tval}
	nd.Leaves = append(nd.Leaves, missLeaf)
	action(missLeaf)
}

type Leaf struct {
	Freq  int        `json:"n"`
	Value *TypeValue `json:"v"`
}

type TypeValue struct {
	Number *float64  `json:"f,omitempty"`
	String *string   `json:"s,omitempty"`
	Bool   *bool     `json:"b,omitempty"`
	Null   *struct{} `json:"n,omitempty"`
}

func (tv *TypeValue) Equal(other *TypeValue) bool {
	if tv.Number != nil && other.Number != nil {
		return *tv.Number == *other.Number
	}
	if tv.String != nil && other.String != nil {
		return *tv.String == *other.String
	}
	if tv.Bool != nil && other.Bool != nil {
		return *tv.Bool == *other.Bool
	}
	return tv.Null != nil && other.Null != nil
}

type SummaryLeaf struct {
	Numbers *NumberSummaryLeaf `json:"number,omitempty"`
	Strings *StringSummaryLeaf `json:"string,omitempty"`
	Bools   *BoolSummaryLeaf   `json:"bool,omitempty"`
	Nulls   *NullSummaryLeaf   `json:"null,omitempty"`
}

func summarizeLeaves(leafs []*Leaf, bucketCount, topCount int) *SummaryLeaf {
	var (
		numbers []float64
		strings []string
		bools   []bool
		nulls   int
		summary = &SummaryLeaf{}
	)
	for _, leaf := range leafs {
		switch {
		case leaf.Value.Number != nil:
			numbers = append(numbers, *leaf.Value.Number)
		case leaf.Value.String != nil:
			strings = append(strings, *leaf.Value.String)
		case leaf.Value.Bool != nil:
			bools = append(bools, *leaf.Value.Bool)
		case leaf.Value.Null != nil:
			nulls++
		}
	}
	if len(numbers) > 0 {
		summary.Numbers = summarizeNumbers(numbers, bucketCount)
	}
	if len(strings) > 0 {
		summary.Strings = summarizeStrings(strings, topCount)
	}
	if len(bools) > 0 {
		summary.Bools = summarizeBools(bools)
	}
	if nulls > 0 {
		summary.Nulls = &NullSummaryLeaf{Freq: nulls}
	}
	return summary
}

type NumberSummaryLeaf struct {
	Freq    int     `json:"freq"`
	Unique  int     `json:"unique"`
	AllInts bool    `json:"all_ints"`
	Min     float64 `json:"min"`
	Max     float64 `json:"max"`

	Distribution []BucketRange `json:"distribution"`
}

func summarizeNumbers(numbers []float64, bucketCount int) *NumberSummaryLeaf {
	out := &NumberSummaryLeaf{
		Freq:    len(numbers),
		AllInts: isInt(numbers[0]), // assume until proven wrong
		Unique:  1,
		Min:     numbers[0],
		Max:     numbers[0],
	}

	uniq := map[float64]int{
		numbers[0]: 1,
	}
	for _, v := range numbers[1:] {
		uniq[v]++
		if v > out.Max {
			out.Max = v
		}
		if v < out.Min {
			out.Min = v
		}
		if out.AllInts {
			out.AllInts = isInt(v)
		}
	}
	out.Unique = len(uniq)
	uniques := maps.Keys(uniq)
	sort.Float64s(uniques)
	if bucketCount > 1 {
		if len(uniques) < bucketCount {
			bucketCount = len(uniques)
		}
		dist := make([]BucketRange, 0, bucketCount)
		bucketStep := len(uniques) / bucketCount
		for i := 0; i < len(uniques); i += bucketStep {
			fromIdx := i
			rg := BucketRange{
				From: uniques[fromIdx],
			}
			for _, un := range uniques[fromIdx:imin(i+bucketStep, len(uniques))] {
				freq := uniq[un]
				rg.Freq += freq
				rg.To = un
			}
			dist = append(dist, rg)
		}
		out.Distribution = dist
	}
	return out
}

func imin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type BucketRange struct {
	From float64 `json:"from"`
	To   float64 `json:"to"`
	Freq int     `json:"freq"`
}

func isInt(v float64) bool {
	return v == float64(int(v))
}

type StringSummaryLeaf struct {
	Freq   int            `json:"freq"`
	Unique int            `json:"unique"`
	MinLen int            `json:"min_len"`
	MaxLen int            `json:"max_len"`
	Top    []StringSample `json:"top"`
}

type StringSample struct {
	Value string `json:"val"`
	Freq  int    `json:"freq"`
}

func summarizeStrings(strings []string, topCount int) *StringSummaryLeaf {
	out := &StringSummaryLeaf{
		Freq:   len(strings),
		Unique: 1,
		MinLen: len(strings[0]),
		MaxLen: len(strings[0]),
	}
	uniq := map[string]int{
		strings[0]: 1,
	}
	for _, v := range strings[1:] {
		uniq[v]++
		if len(v) > out.MaxLen {
			out.MaxLen = len(v)
		}
		if len(v) < out.MinLen {
			out.MinLen = len(v)
		}
	}
	out.Unique = len(uniq)
	if len(uniq) < topCount {
		topCount = len(uniq)
	}
	samples := make([]StringSample, 0, topCount)
	for v, freq := range uniq {
		if len(samples) < topCount {
			samples = append(samples, StringSample{
				Value: v, Freq: freq,
			})
			continue
		}
		var (
			minLessFrequent  = freq
			minLessVal       = v
			minLessFreqIndex = -1
		)
		// if there are samples with less frequency, find the least frequent
		// one and knock it off
		for i, sample := range samples {
			if sample.Freq < freq || (freq == sample.Freq && isLonger(v, sample.Value)) {
				isLeastFrequent := sample.Freq < minLessFrequent
				isSameFrequentButShorter := sample.Freq == minLessFrequent && isLonger(minLessVal, sample.Value)
				if isLeastFrequent {
					minLessFrequent = sample.Freq
					minLessVal = sample.Value
					minLessFreqIndex = i
				} else if isSameFrequentButShorter {
					minLessFrequent = sample.Freq
					minLessVal = sample.Value
					minLessFreqIndex = i
				}
			}
		}
		if minLessFreqIndex >= 0 {
			// replace it
			samples[minLessFreqIndex] = StringSample{
				Value: v, Freq: freq,
			}
		}
	}
	sort.Slice(samples, func(i, j int) bool {
		is := samples[i]
		js := samples[j]
		return is.Freq > js.Freq || (is.Freq == js.Freq && isLonger(is.Value, js.Value))
	})
	out.Top = samples
	return out
}

func isLonger(left, right string) bool {
	if len(left) > len(right) {
		return true
	}
	if len(right) > len(left) {
		return false
	}
	return left > right
}

type BoolSummaryLeaf struct {
	Freq      int `json:"freq"`
	TrueFreq  int `json:"trues"`
	FalseFreq int `json:"falses"`
}

func summarizeBools(bools []bool) *BoolSummaryLeaf {
	out := &BoolSummaryLeaf{
		Freq: len(bools),
	}
	for _, v := range bools {
		if v {
			out.TrueFreq++
		} else {
			out.FalseFreq++
		}
	}
	return out
}

type NullSummaryLeaf struct {
	Freq int `json:"freq"`
}
