package sumjson

import (
	"github.com/aybabtme/flatjson"
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
			summary.atKey(data, prefixes, val.Name.String(data), func(node *Node) {
				tval := &TypeValue{Number: &val.Value}
				node.atLeaf(tval, func(l *Leaf) {
					// do something?
				})
			})
		},
		OnString: func(prefixes flatjson.Prefixes, val flatjson.String) {
			summary.atKey(data, prefixes, val.Name.String(data), func(node *Node) {
				v := val.Value.String(data)
				tval := &TypeValue{String: &v}
				node.atLeaf(tval, func(l *Leaf) {
					// do something?
				})
			})
		},
		OnBoolean: func(prefixes flatjson.Prefixes, val flatjson.Bool) {
			summary.atKey(data, prefixes, val.Name.String(data), func(node *Node) {
				tval := &TypeValue{Bool: &val.Value}
				node.atLeaf(tval, func(l *Leaf) {
					// do something?
				})
			})
		},
		OnNull: func(prefixes flatjson.Prefixes, val flatjson.Null) {
			summary.atKey(data, prefixes, val.Name.String(data), func(node *Node) {
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
