// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package graphx_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func init() {
	runtime.RegisterFunction(dec)
	runtime.RegisterFunction(enc)
}

type registeredNamedTypeForTest struct {
	A, B int64
	C    string
}

func init() {
	schema.RegisterType(reflect.TypeOf((*registeredNamedTypeForTest)(nil)))
}

func TestMyThing(t *testing.T) {

	//0 128 0 1 132 156 174 78 224 224 212 3")
	arr, err := readBytesFromString("0 128 0 1 132 156 174 78 224 224 212 3")
	if err != nil {
		panic(err)
	}
	kvc := coder.NewKV([]*coder.Coder{coder.NewBytes(), coder.NewIntervalWindowCoder()})
	dec := exec.MakeElementDecoder(kvc)
	v, err := dec.Decode(bytes.NewReader(arr))
	fmt.Printf("%v, err: %s\n", v, err)

	//arr, err = readBytesFromString("0 0 0 0 0 0 0 0 0 0 0 1 15")
	//exec.DecodeWindowedValueHeader(exec.MakeWindowDecoder(coder.NewGlobalWindow()), bytes.NewReader(arr))
}

func TestThing2(t *testing.T) {
	arr, err := readBytesFromString("0 0 0 0 0 0 0 0 0 0 0 1 15")
	if err != nil {
		panic(err)
	}
	w, e, pn, err := exec.DecodeWindowedValueHeader(exec.MakeWindowDecoder(coder.NewGlobalWindow()), bytes.NewReader(arr))
	if err != nil {
		t.Fatalf("wrong: %s", err)
	}
	t.Logf("%s, %s, %v", w, e, pn)
}

func readBytesFromString(s string) ([]byte, error) {
	ss := strings.Split(s, " ")
	var arr []byte
	for _, v := range ss {
		pv, err := strconv.ParseUint(v, 10, 8)
		if err != nil {
			return nil, err
		}
		arr = append(arr, byte(pv))
	}
	return arr, nil
}

// TestMarshalUnmarshalCoders verifies that coders survive a proto roundtrip.
func TestMarshalUnmarshalCoders(t *testing.T) {
	foo := custom("foo", reflectx.Bool)
	bar := custom("bar", reflectx.String)
	baz := custom("baz", reflectx.Int)

	tests := []struct {
		name string
		c    *coder.Coder
	}{
		{
			"bytes",
			coder.NewBytes(),
		},
		{
			"bool",
			coder.NewBool(),
		},
		{
			"varint",
			coder.NewVarInt(),
		},
		{
			"double",
			coder.NewDouble(),
		},
		{
			"string",
			coder.NewString(),
		},
		{
			"foo",
			foo,
		},
		{
			"bar",
			bar,
		},
		{
			"baz",
			baz,
		},
		{
			"W<bytes>",
			coder.NewW(coder.NewBytes(), coder.NewGlobalWindow()),
		},
		{
			"N<bytes>",
			coder.NewN(coder.NewBytes()),
		},
		{
			"KV<foo,bar>",
			coder.NewKV([]*coder.Coder{foo, bar}),
		},
		{
			"CoGBK<foo,bar>",
			coder.NewCoGBK([]*coder.Coder{foo, bar}),
		},
		{
			"CoGBK<foo,bar,baz>",
			coder.NewCoGBK([]*coder.Coder{foo, bar, baz}),
		},
		{
			name: "R[graphx.registeredNamedTypeForTest]",
			c:    coder.NewR(typex.New(reflect.TypeOf((*registeredNamedTypeForTest)(nil)).Elem())),
		},
		{
			name: "R[*graphx.registeredNamedTypeForTest]",
			c:    coder.NewR(typex.New(reflect.TypeOf((*registeredNamedTypeForTest)(nil)))),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ids, marshalCoders, err := graphx.MarshalCoders([]*coder.Coder{test.c})
			if err != nil {
				t.Fatalf("Marshal(%v) failed: %v", test.c, err)
			}
			coders, err := graphx.UnmarshalCoders(ids, marshalCoders)
			if err != nil {
				t.Fatalf("Unmarshal(Marshal(%v)) failed: %v", test.c, err)
			}
			if len(coders) != 1 || !test.c.Equals(coders[0]) {
				t.Errorf("Unmarshal(Marshal(%v)) = %v, want identity", test.c, coders)
			}
		})
	}

	for _, test := range tests {
		t.Run("namespaced:"+test.name, func(t *testing.T) {
			cm := graphx.NewCoderMarshaller()
			cm.Namespace = "testnamespace"
			ids, err := cm.AddMulti([]*coder.Coder{test.c})
			if err != nil {
				t.Fatalf("AddMulti(%v) failed: %v", test.c, err)
			}
			marshalCoders := cm.Build()
			for _, id := range ids {
				if !strings.Contains(id, cm.Namespace) {
					t.Errorf("got %v, want it to contain %v", id, cm.Namespace)
				}
			}

			coders, err := graphx.UnmarshalCoders(ids, marshalCoders)
			if err != nil {
				t.Fatalf("Unmarshal(Marshal(%v)) failed: %v", test.c, err)
			}
			if len(coders) != 1 || !test.c.Equals(coders[0]) {
				t.Errorf("Unmarshal(Marshal(%v)) = %v, want identity", test.c, coders)
			}
		})
	}

	// These tests cover the pure dataflow to dataflow coder cases.
	for _, test := range tests {
		t.Run("dataflow:"+test.name, func(t *testing.T) {
			ref, err := graphx.EncodeCoderRef(test.c)
			if err != nil {
				t.Fatalf("EncodeCoderRef(%v) failed: %v", test.c, err)
			}
			got, err := graphx.DecodeCoderRef(ref)
			if err != nil {
				t.Fatalf("DecodeCoderRef(EncodeCoderRef(%v)) failed: %v", test.c, err)
			}
			if !test.c.Equals(got) {
				t.Errorf("DecodeCoderRef(EncodeCoderRef(%v)) = %v, want identity", test.c, got)
			}
		})
	}
}

func enc(in typex.T) ([]byte, error) {
	panic("enc is fake")
}

func dec(t reflect.Type, in []byte) (typex.T, error) {
	panic("dec is fake")
}

func custom(name string, t reflect.Type) *coder.Coder {
	c, _ := coder.NewCustomCoder(name, t, enc, dec)
	return &coder.Coder{Kind: coder.Custom, T: typex.New(t), Custom: c}
}
