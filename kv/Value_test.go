package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type Person struct {
	Name  string `json:"name"`
	Age   int    `json:"age"`
	Email string `json:"email"`
}

func Test_Convert(t *testing.T) {
	person := Person{
		Name:  "Bob",
		Age:   18,
		Email: "2874974475@qq.com",
	}
	v_data, err := Convert(person)
	require.Equal(t, err, nil)
	require.Equal(t, string(v_data), "{\"name\":\"Bob\",\"age\":18,\"email\":\"2874974475@qq.com\"}")
}

func Test_Encode(t *testing.T) {
	person := Person{
		Name:  "Bob",
		Age:   18,
		Email: "2874974475@qq.com",
	}
	v_data, err := Convert(person)
	require.Equal(t, err, nil)
	require.Equal(t, string(v_data), "{\"name\":\"Bob\",\"age\":18,\"email\":\"2874974475@qq.com\"}")

	value := Value{
		Key:     "persion",
		Value:   v_data,
		Deleted: false,
	}

	value_data, err := Encode(value)
	require.Equal(t, err, nil)
	require.Equal(t, string(value_data), "{\"Key\":\"persion\",\"Value\":\"eyJuYW1lIjoiQm9iIiwiYWdlIjoxOCwiZW1haWwiOiIyODc0OTc0NDc1QHFxLmNvbSJ9\",\"Deleted\":false}")

}
