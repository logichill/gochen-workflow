package jsonclone

import "reflect"

type visitKey struct {
	typ reflect.Type
	ptr uintptr
	len int
}

// Map deep-copies JSON-like maps used by workflow design metadata and instance data.
func Map(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	return cloneMapStringAny(values, map[uintptr]map[string]any{}, map[uintptr][]any{})
}

// Any deep-copies mutable JSON-like container shapes.
func Any(value any) any {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case map[string]any:
		return cloneMapStringAny(v, map[uintptr]map[string]any{}, map[uintptr][]any{})
	case []any:
		return cloneAnySlice(v, map[uintptr]map[string]any{}, map[uintptr][]any{})
	}
	return cloneReflect(value)
}

func cloneMapStringAny(values map[string]any, seenMaps map[uintptr]map[string]any, seenSlices map[uintptr][]any) map[string]any {
	if values == nil {
		return nil
	}
	ptr := reflect.ValueOf(values).Pointer()
	if cached, ok := seenMaps[ptr]; ok {
		return cached
	}
	out := make(map[string]any, len(values))
	seenMaps[ptr] = out
	for key, value := range values {
		out[key] = cloneJSONLike(value, seenMaps, seenSlices)
	}
	return out
}

func cloneAnySlice(values []any, seenMaps map[uintptr]map[string]any, seenSlices map[uintptr][]any) []any {
	if values == nil {
		return nil
	}
	ptr := reflect.ValueOf(values).Pointer()
	if cached, ok := seenSlices[ptr]; ok {
		return cached
	}
	out := make([]any, len(values))
	seenSlices[ptr] = out
	for i, value := range values {
		out[i] = cloneJSONLike(value, seenMaps, seenSlices)
	}
	return out
}

func cloneJSONLike(value any, seenMaps map[uintptr]map[string]any, seenSlices map[uintptr][]any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneMapStringAny(v, seenMaps, seenSlices)
	case []any:
		return cloneAnySlice(v, seenMaps, seenSlices)
	default:
		return cloneReflect(value)
	}
}

func cloneReflect(value any) any {
	if value == nil {
		return nil
	}
	cloned := cloneValue(reflect.ValueOf(value), map[visitKey]reflect.Value{})
	if !cloned.IsValid() {
		return nil
	}
	return cloned.Interface()
}

func cloneValue(value reflect.Value, seen map[visitKey]reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		return valueForType(cloneValue(value.Elem(), seen), value.Type(), value)
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		visit := visitFor(value)
		if cached, ok := seen[visit]; ok {
			return cached
		}
		out := reflect.New(value.Type().Elem())
		seen[visit] = out
		setValue(out.Elem(), cloneValue(value.Elem(), seen))
		return out
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		visit := visitFor(value)
		if cached, ok := seen[visit]; ok {
			return cached
		}
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		seen[visit] = out
		iter := value.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), valueForType(cloneValue(iter.Value(), seen), value.Type().Elem(), iter.Value()))
		}
		return out
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		visit := visitFor(value)
		if cached, ok := seen[visit]; ok {
			return cached
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		seen[visit] = out
		for i := 0; i < value.Len(); i++ {
			setValue(out.Index(i), cloneValue(value.Index(i), seen))
		}
		return out
	case reflect.Array:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			setValue(out.Index(i), cloneValue(value.Index(i), seen))
		}
		return out
	case reflect.Struct:
		out := reflect.New(value.Type()).Elem()
		out.Set(value)
		for i := 0; i < value.NumField(); i++ {
			field := value.Field(i)
			if out.Field(i).CanSet() && field.CanInterface() {
				setValue(out.Field(i), cloneValue(field, seen))
			}
		}
		return out
	default:
		return value
	}
}

func visitFor(value reflect.Value) visitKey {
	visit := visitKey{typ: value.Type(), ptr: value.Pointer()}
	if value.Kind() == reflect.Slice {
		visit.len = value.Len()
	}
	return visit
}

func setValue(target reflect.Value, value reflect.Value) {
	target.Set(valueForType(value, target.Type(), target))
}

func valueForType(value reflect.Value, target reflect.Type, fallback reflect.Value) reflect.Value {
	if !value.IsValid() {
		return reflect.Zero(target)
	}
	if value.Type().AssignableTo(target) {
		return value
	}
	if value.Type().ConvertibleTo(target) {
		return value.Convert(target)
	}
	if fallback.IsValid() && fallback.Type().AssignableTo(target) {
		return fallback
	}
	return reflect.Zero(target)
}

// Strings copies a string slice while preserving the nil versus empty distinction.
func Strings(values []string) []string {
	if values == nil {
		return nil
	}
	out := make([]string, len(values))
	copy(out, values)
	return out
}
