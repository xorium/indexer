package numerator

import (
	"sort"
)

type Numerator interface {
	Numerate(tokens []string) map[string][]int
}

type numerator struct{}

func NewNumerator() Numerator {
	return &numerator{}
}

func (n *numerator) Numerate(tokens []string) map[string][]int {
	m := make(map[string][]int)

	for i, v := range tokens {
		m[v] = append(m[v], i)
	}

	for _, v := range m {
		sort.Ints(v)
	}

	return m
}
