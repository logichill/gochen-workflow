package jsonclone

import (
	"reflect"
	"testing"
)

type snapshot struct {
	Labels map[string]string
	Items  []string
}

type linkedSnapshot struct {
	Name string
	Next *linkedSnapshot
}

func TestAnyClonesNestedContainers(t *testing.T) {
	input := map[string]any{
		"items": []map[string]any{
			{"value": "original"},
		},
	}

	copied := Map(input)
	copied["items"].([]map[string]any)[0]["value"] = "changed"

	if got := input["items"].([]map[string]any)[0]["value"]; got != "original" {
		t.Fatalf("input item value = %v, want original", got)
	}
}

func TestAnyClonesTypedMapsAndSlices(t *testing.T) {
	input := map[string]any{
		"labels": map[string]string{"state": "original"},
		"counts": []int{1, 2},
	}

	copied := Map(input)
	copied["labels"].(map[string]string)["state"] = "changed"
	copied["counts"].([]int)[0] = 99

	if got := input["labels"].(map[string]string)["state"]; got != "original" {
		t.Fatalf("input label = %v, want original", got)
	}
	if got := input["counts"].([]int)[0]; got != 1 {
		t.Fatalf("input count = %v, want 1", got)
	}
}

func TestAnyClonesPointerStructArrayAndInterfaceValues(t *testing.T) {
	interfaceSnapshot := any(map[string]string{"state": "original"})
	input := map[string]any{
		"snapshot": snapshot{
			Labels: map[string]string{"state": "original"},
			Items:  []string{"first"},
		},
		"snapshot_ptr": &snapshot{
			Labels: map[string]string{"state": "original"},
			Items:  []string{"first"},
		},
		"array": [1]map[string]string{
			{"state": "original"},
		},
		"interface_ptr": &interfaceSnapshot,
	}

	copied := Map(input)
	copiedSnapshot := copied["snapshot"].(snapshot)
	copiedSnapshot.Labels["state"] = "changed"
	copiedSnapshot.Items[0] = "changed"
	copied["snapshot_ptr"].(*snapshot).Labels["state"] = "changed"
	copied["snapshot_ptr"].(*snapshot).Items[0] = "changed"
	copied["array"].([1]map[string]string)[0]["state"] = "changed"
	(*copied["interface_ptr"].(*any)).(map[string]string)["state"] = "changed"

	if got := input["snapshot"].(snapshot).Labels["state"]; got != "original" {
		t.Fatalf("input snapshot label = %v, want original", got)
	}
	if got := input["snapshot"].(snapshot).Items[0]; got != "first" {
		t.Fatalf("input snapshot item = %v, want first", got)
	}
	if got := input["snapshot_ptr"].(*snapshot).Labels["state"]; got != "original" {
		t.Fatalf("input snapshot ptr label = %v, want original", got)
	}
	if got := input["snapshot_ptr"].(*snapshot).Items[0]; got != "first" {
		t.Fatalf("input snapshot ptr item = %v, want first", got)
	}
	if got := input["array"].([1]map[string]string)[0]["state"]; got != "original" {
		t.Fatalf("input array label = %v, want original", got)
	}
	if got := (*input["interface_ptr"].(*any)).(map[string]string)["state"]; got != "original" {
		t.Fatalf("input interface ptr label = %v, want original", got)
	}
}

func TestStringsPreservesNilAndEmpty(t *testing.T) {
	if got := Strings(nil); got != nil {
		t.Fatalf("nil slice copied to %#v, want nil", got)
	}
	if got := Strings([]string{}); got == nil {
		t.Fatal("empty slice copied to nil")
	}
}

func TestAnyPreservesTypedNilSlices(t *testing.T) {
	var nilAnySlice []any
	var nilByteSlice []byte
	var nilMapSlice []map[string]any
	input := map[string]any{
		"any_slice": nilAnySlice,
		"bytes":     nilByteSlice,
		"map_slice": nilMapSlice,
	}

	copied := Map(input)

	if got := copied["any_slice"].([]any); got != nil {
		t.Fatalf("nil []any copied to %#v, want nil", got)
	}
	if got := copied["bytes"].([]byte); got != nil {
		t.Fatalf("nil []byte copied to %#v, want nil", got)
	}
	if got := copied["map_slice"].([]map[string]any); got != nil {
		t.Fatalf("nil []map[string]any copied to %#v, want nil", got)
	}
}

func TestAnyClonesCyclicContainers(t *testing.T) {
	selfMap := map[string]any{"name": "original"}
	selfMap["self"] = selfMap

	selfSlice := make([]any, 2)
	selfSlice[0] = "original"
	selfSlice[1] = selfSlice

	selfNode := &linkedSnapshot{Name: "original"}
	selfNode.Next = selfNode

	input := map[string]any{
		"self_map":   selfMap,
		"self_slice": selfSlice,
		"self_node":  selfNode,
	}

	copied := Map(input)

	copiedMap := copied["self_map"].(map[string]any)
	if reflect.ValueOf(copiedMap).Pointer() != reflect.ValueOf(copiedMap["self"]).Pointer() {
		t.Fatalf("copied self map did not preserve cycle")
	}
	copiedMap["name"] = "changed"

	copiedSlice := copied["self_slice"].([]any)
	if reflect.ValueOf(copiedSlice).Pointer() != reflect.ValueOf(copiedSlice[1]).Pointer() {
		t.Fatalf("copied self slice did not preserve cycle")
	}
	copiedSlice[0] = "changed"

	copiedNode := copied["self_node"].(*linkedSnapshot)
	if copiedNode != copiedNode.Next {
		t.Fatalf("copied self node did not preserve cycle")
	}
	copiedNode.Name = "changed"

	if got := selfMap["name"]; got != "original" {
		t.Fatalf("input self map name = %v, want original", got)
	}
	if got := selfSlice[0]; got != "original" {
		t.Fatalf("input self slice value = %v, want original", got)
	}
	if got := selfNode.Name; got != "original" {
		t.Fatalf("input self node name = %v, want original", got)
	}
}

func TestAnyClonesDirectCyclicSlice(t *testing.T) {
	selfSlice := make([]any, 2)
	selfSlice[0] = "original"
	selfSlice[1] = selfSlice

	copied := Any(selfSlice).([]any)

	if reflect.ValueOf(copied).Pointer() != reflect.ValueOf(copied[1]).Pointer() {
		t.Fatalf("copied direct self slice did not preserve cycle")
	}
	copied[0] = "changed"
	if got := selfSlice[0]; got != "original" {
		t.Fatalf("input direct self slice value = %v, want original", got)
	}
}

func TestAnyKeepsOverlappingSlicesDistinctByLength(t *testing.T) {
	values := []int{1, 2, 3}
	input := map[string]any{
		"short": values[:1],
		"long":  values[:2],
	}

	copied := Map(input)

	if got := copied["short"]; !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("short slice = %#v, want [1]", got)
	}
	if got := copied["long"]; !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("long slice = %#v, want [1 2]", got)
	}
}
