package dag

import (
	"cicd-engine/internal/workflow"
	"container/heap"
	"fmt"
)

type Graph struct {
	Needs    map[string][]string
	Children map[string][]string
}

func Build(w *workflow.Workflow) (*Graph, error) {
	if err := w.Validate(); err != nil {
		return nil, err
	}
	g := &Graph{map[string][]string{}, map[string][]string{}}
	for _, id := range w.IDs() {
		g.Needs[id] = append([]string{}, w.Jobs[id].Needs...)
		for _, n := range w.Jobs[id].Needs {
			g.Children[n] = append(g.Children[n], id)
		}
	}
	return g, nil
}

type Queue []string

func (q Queue) Len() int           { return len(q) }
func (q Queue) Less(i, j int) bool { return q[i] < q[j] }
func (q Queue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *Queue) Push(x any)        { *q = append(*q, x.(string)) }
func (q *Queue) Pop() any          { a := *q; x := a[len(a)-1]; *q = a[:len(a)-1]; return x }
func (g *Graph) Topological() ([]string, error) {
	counts := map[string]int{}
	q := &Queue{}
	heap.Init(q)
	for id, ns := range g.Needs {
		counts[id] = len(ns)
		if len(ns) == 0 {
			heap.Push(q, id)
		}
	}
	var out []string
	for q.Len() > 0 {
		id := heap.Pop(q).(string)
		out = append(out, id)
		for _, c := range g.Children[id] {
			counts[c]--
			if counts[c] == 0 {
				heap.Push(q, c)
			}
		}
	}
	if len(out) != len(g.Needs) {
		return nil, fmt.Errorf("dependency cycle detected")
	}
	return out, nil
}
