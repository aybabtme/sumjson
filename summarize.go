package sumjson

import (
	"sort"
	"strconv"

	"github.com/aybabtme/flatjson"
	"golang.org/x/exp/maps"
)

type Summary struct {
	Root *Node `json:"root"`
}

type Reporter interface {
	ObjectRead(from, to, total int)
	Summarized(done, total int)
}

func Summarize(data []byte, reporter Reporter) (*Summary, error) {
	summary := &Summary{Root: new(Node)}
	leaves := 0
	cb := &flatjson.Callbacks{
		MaxDepth: 99,
		OnNumber: func(prefixes flatjson.Prefixes, val flatjson.Number) {
			name := val.Name.String(data)
			summary.atKey(data, prefixes, name, func(node *Node) {
				leaves++
				node.ScalarNumbers = append(node.ScalarNumbers, val.Value)
			})
		},
		OnString: func(prefixes flatjson.Prefixes, val flatjson.String) {
			name := val.Name.String(data)
			summary.atKey(data, prefixes, name, func(node *Node) {
				leaves++
				node.ScalarStrings = append(node.ScalarStrings, val.Value.String(data))
			})
		},
		OnBoolean: func(prefixes flatjson.Prefixes, val flatjson.Bool) {
			name := val.Name.String(data)
			summary.atKey(data, prefixes, name, func(node *Node) {
				leaves++
				node.ScalarBools = append(node.ScalarBools, val.Value)
			})
		},
		OnNull: func(prefixes flatjson.Prefixes, val flatjson.Null) {
			name := val.Name.String(data)
			summary.atKey(data, prefixes, name, func(node *Node) {
				leaves++
				node.ScalarNulls++
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
	leavesDone := 0
	summary.Root.walk(func(n *Node) bool {
		leavesDone++
		n.Summarized = summarizeNode(n, 10, 5)

		// don't encode individual values
		n.ScalarNumbers = nil
		n.ScalarStrings = nil
		n.ScalarBools = nil
		n.ScalarNulls = 0

		reporter.Summarized(leavesDone, leaves)
		return true
	})
	return summary, nil
}

func (sum *Summary) atKey(data []byte, prefixes flatjson.Prefixes, key string, action func(node *Node)) {
	sum.atKeyIter(sum.Root, data, prefixes, key, action)
}

func (sum *Summary) atKeyIter(parent *Node, data []byte, prefixes flatjson.Prefixes, key string, action func(node *Node)) {
	if len(prefixes) != 0 {
		next := prefixes[0]
		var child *Node
		if next.IsObjectKey() {
			nextKey := next.String(data)
			for _, child := range parent.Children {
				if child.Key == nextKey {
					child.Freq++
					sum.atKeyIter(child, data, prefixes[1:], key, action)
					return
				}
			}
			child = &Node{
				Freq: 1,
				Key:  nextKey,
			}
			parent.Children = append(parent.Children, child)
		} else if next.IsArrayIndex() {
			idx := next.Index()
			switch {
			case idx < len(parent.Elems)-1:
				child = parent.Elems[idx]
				child.Freq++
			case idx == len(parent.Elems)-1:
				child = &Node{Freq: 1, Key: strconv.Itoa(idx)}
				parent.Elems = append(parent.Elems, child)
			case idx > len(parent.Elems)-1:
				for i := len(parent.Elems); i < idx; i++ {
					parent.Elems = append(parent.Elems, &Node{Freq: 0, Key: strconv.Itoa(i)})
				}
				child = &Node{Freq: 1, Key: strconv.Itoa(idx)}
				parent.Elems = append(parent.Elems, child)
			}
		}
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
	Key      string  `json:"key,omitempty"`
	Children []*Node `json:"objects,omitempty"`
	Elems    []*Node `json:"arrays,omitempty"`

	ScalarNumbers []float64 `json:"numbers,omitempty"`
	ScalarStrings []string  `json:"strings,omitempty"`
	ScalarBools   []bool    `json:"bools,omitempty"`
	ScalarNulls   int       `json:"nulls,omitempty"`

	// stats
	Freq       int          `json:"freq"`
	Summarized *SummaryNode `json:"summary,omitempty"`
}

func (nd *Node) walk(eachNode func(*Node) bool) bool {
	for _, ch := range nd.Children {
		if !ch.walk(eachNode) {
			return false
		}
	}
	for _, ch := range nd.Elems {
		if !ch.walk(eachNode) {
			return false
		}
	}
	return eachNode(nd)
}

type SummaryNode struct {
	Objects *ObjectSummaryNode `json:"object,omitempty"`
	Arrays  *ArraySummaryNode  `json:"array,omitempty"`
	Numbers *NumberSummaryNode `json:"number,omitempty"`
	Strings *StringSummaryNode `json:"string,omitempty"`
	Bools   *BoolSummaryNode   `json:"bool,omitempty"`
	Nulls   *NullSummaryNode   `json:"null,omitempty"`
}

func summarizeNode(nd *Node, bucketCount, topCount int) *SummaryNode {
	var summary = &SummaryNode{}
	if len(nd.Children) > 0 {
		summary.Objects = summarizeObjects(nd.Children)
	}
	if len(nd.Elems) > 0 {
		summary.Arrays = summarizeArrays(nd.Elems)
	}
	if len(nd.ScalarNumbers) > 0 {
		summary.Numbers = summarizeNumbers(nd.ScalarNumbers, bucketCount)
	}
	if len(nd.ScalarStrings) > 0 {
		summary.Strings = summarizeStrings(nd.ScalarStrings, topCount)
	}
	if len(nd.ScalarBools) > 0 {
		summary.Bools = summarizeBools(nd.ScalarBools)
	}
	if nd.ScalarNulls > 0 {
		summary.Nulls = &NullSummaryNode{Freq: nd.ScalarNulls}
	}
	return summary
}

type ObjectSummaryNode struct {
	Keys []Key `json:"keys"`
}

type Key struct {
	Name string `json:"key"`
	Freq int    `json:"freq"`
}

func summarizeObjects(objs []*Node) *ObjectSummaryNode {
	var ordered []string
	keys := make(map[string]Key, 0)
	for _, obj := range objs {
		k, ok := keys[obj.Key]
		if !ok {
			k = Key{Name: obj.Key}
			ordered = append(ordered, obj.Key)
		}
		k.Freq += obj.Freq
		keys[obj.Key] = k
	}
	var out []Key
	for _, k := range ordered {
		out = append(out, keys[k])
	}

	return &ObjectSummaryNode{Keys: out}
}

type ArraySummaryNode struct {
	Freq int `json:"freq"`
}

func summarizeArrays(elems []*Node) *ArraySummaryNode {
	return &ArraySummaryNode{Freq: len(elems)}
}

type NumberSummaryNode struct {
	Freq    int     `json:"freq"`
	Unique  int     `json:"unique"`
	AllInts bool    `json:"all_ints"`
	Min     float64 `json:"min"`
	Max     float64 `json:"max"`

	Distribution []BucketRange `json:"distribution"`
}

func summarizeNumbers(numbers []float64, bucketCount int) *NumberSummaryNode {
	out := &NumberSummaryNode{
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

type StringSummaryNode struct {
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

func summarizeStrings(strings []string, topCount int) *StringSummaryNode {
	out := &StringSummaryNode{
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

type BoolSummaryNode struct {
	Freq      int `json:"freq"`
	TrueFreq  int `json:"trues"`
	FalseFreq int `json:"falses"`
}

func summarizeBools(bools []bool) *BoolSummaryNode {
	out := &BoolSummaryNode{
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

type NullSummaryNode struct {
	Freq int `json:"freq"`
}
