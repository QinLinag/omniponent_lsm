package kv

import (
	"encoding/json"
)

type SearchResult int

const (
	None SearchResult = iota

	Deleted 

	Success
)

// kv
type Value struct {
	Key     string
	Value   []byte
	Deleted bool
}


func (v *Value) Copy() *Value {
	return &Value{
		Key: v.Key,
		Value: v.Value,
		Deleted: v.Deleted,
	}
}

//反序列化kv对象value
func Get[T any](v *Value) (T, error) {
	var value T
	err := json.Unmarshal(v.Value, &value)
	return value, err
}

//将kv对象中的value序列化
func Convert[T any](value T) ([]byte, error) {
	return json.Marshal(value)
}

//将Value的二进制反序列化为Value对象
func Decode(data []byte) (Value, error) {
	var value Value
	err := json.Unmarshal(data, &value)
	return value, err
}

//将kv对象序列话为二进制
func Encode(value Value) ([]byte, error) {
	return json.Marshal(value)
}