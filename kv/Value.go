package kv

import (
	"encoding/json"
)

// kv 需要序列化，故必须大写
type Value struct {
	Key     string
	Value   []byte
	Deleted bool
}

type SearchResult int

const (
	None SearchResult = iota
	Deleted
	Success
)

func Isdeleted(result SearchResult) bool {
	return result == Deleted
}
func IsSuccess(result SearchResult) bool {
	return result == Success
}
func IsNone(result SearchResult) bool {
	return result == None
}

/*
初始化模块
*/
func NewValue(key string, value []byte) *Value {
	newValue := Value{
		Key:     key,
		Value:   value,
		Deleted: false,
	}
	return &newValue
}

/*
功能性模块
*/
func (v *Value) Isdeleted() bool {
	return v.Deleted
}
func (v *Value) GetKey() string {
	return v.Key
}
func (v *Value) GetValue() []byte {
	return v.Value
}
func (v *Value) SetValue(value []byte) {
	v.Value = value
}
func (v *Value) SetDeleted(flag bool) {
	v.Deleted = flag
}
func (v *Value) SetKey(key string) {
	v.Key = key
}
func (v *Value) Copy() *Value {
	return &Value{
		Key:     v.Key,
		Value:   v.Value,
		Deleted: v.Deleted,
	}
}

/*
序列化与反序列化模块
*/
// 反序列化kv对象value
func Get[T any](v *Value) (T, error) {
	var value T
	err := json.Unmarshal(v.Value, &value)
	return value, err
}

// 将kv对象中的value序列化
func Convert[T any](value T) ([]byte, error) {
	return json.Marshal(value)
}

// 将value的二进制反序列化为value对象
func Decode(data []byte) (Value, error) {
	var value Value
	err := json.Unmarshal(data, &value)
	return value, err
}

// 将kv对象序列话为二进制
func Encode(value Value) ([]byte, error) {
	return json.Marshal(value)
}
