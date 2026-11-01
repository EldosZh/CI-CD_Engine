package dag

import (
	"cicd-engine/internal/workflow"
	"reflect"
	"testing"
)

func TestTopological(t *testing.T) {
	w := &workflow.Workflow{Version: 1, Name: "diamond", Jobs: map[string]workflow.Job{}}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		w.Jobs[id] = workflow.Job{Steps: []workflow.Step{{Run: "ok"}}}
	}
	j := w.Jobs["c"]
	j.Needs = []string{"a", "b"}
	w.Jobs["c"] = j
	j = w.Jobs["d"]
	j.Needs = []string{"c"}
	w.Jobs["d"] = j
	for i := 0; i < 20; i++ {
		g, err := Build(w)
		if err != nil {
			t.Fatal(err)
		}
		order, err := g.Topological()
		if err != nil || !reflect.DeepEqual(order, []string{"a", "b", "c", "d", "e"}) {
			t.Fatalf("%v %v", order, err)
		}
	}
}
