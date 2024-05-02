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

// Code generated by starcgen. DO NOT EDIT.
// File: testpipeline.shims.go

package testpipeline

import (
	"context"
	"reflect"

	// Library imports
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func init() {
	runtime.RegisterFunction(KvEmitFn)
	runtime.RegisterFunction(KvFn)
	runtime.RegisterFunction(VFn)
	runtime.RegisterType(reflect.TypeOf((*SCombine)(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*SCombine)(nil)).Elem())
	reflectx.RegisterStructWrapper(reflect.TypeOf((*SCombine)(nil)).Elem(), wrapMakerSCombine)
	reflectx.RegisterFunc(reflect.TypeOf((*func(int, int) int)(nil)).Elem(), funcMakerIntIntГInt)
	reflectx.RegisterFunc(reflect.TypeOf((*func(int) (string, int))(nil)).Elem(), funcMakerIntГStringInt)
	reflectx.RegisterFunc(reflect.TypeOf((*func(string, int, func(string, int)))(nil)).Elem(), funcMakerStringIntEmitStringIntГ)
	reflectx.RegisterFunc(reflect.TypeOf((*func(string, int) (string, int))(nil)).Elem(), funcMakerStringIntГStringInt)
	exec.RegisterEmitter(reflect.TypeOf((*func(string, int))(nil)).Elem(), emitMakerStringInt)
}

func wrapMakerSCombine(fn any) map[string]reflectx.Func {
	dfn := fn.(*SCombine)
	return map[string]reflectx.Func{
		"MergeAccumulators": reflectx.MakeFunc(func(a0 int, a1 int) int { return dfn.MergeAccumulators(a0, a1) }),
	}
}

type callerIntIntГInt struct {
	fn func(int, int) int
}

func funcMakerIntIntГInt(fn any) reflectx.Func {
	f := fn.(func(int, int) int)
	return &callerIntIntГInt{fn: f}
}

func (c *callerIntIntГInt) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerIntIntГInt) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerIntIntГInt) Call(args []any) []any {
	out0 := c.fn(args[0].(int), args[1].(int))
	return []any{out0}
}

func (c *callerIntIntГInt) Call2x1(arg0, arg1 any) any {
	return c.fn(arg0.(int), arg1.(int))
}

type callerIntГStringInt struct {
	fn func(int) (string, int)
}

func funcMakerIntГStringInt(fn any) reflectx.Func {
	f := fn.(func(int) (string, int))
	return &callerIntГStringInt{fn: f}
}

func (c *callerIntГStringInt) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerIntГStringInt) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerIntГStringInt) Call(args []any) []any {
	out0, out1 := c.fn(args[0].(int))
	return []any{out0, out1}
}

func (c *callerIntГStringInt) Call1x2(arg0 any) (any, any) {
	return c.fn(arg0.(int))
}

type callerStringIntEmitStringIntГ struct {
	fn func(string, int, func(string, int))
}

func funcMakerStringIntEmitStringIntГ(fn any) reflectx.Func {
	f := fn.(func(string, int, func(string, int)))
	return &callerStringIntEmitStringIntГ{fn: f}
}

func (c *callerStringIntEmitStringIntГ) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerStringIntEmitStringIntГ) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerStringIntEmitStringIntГ) Call(args []any) []any {
	c.fn(args[0].(string), args[1].(int), args[2].(func(string, int)))
	return []any{}
}

func (c *callerStringIntEmitStringIntГ) Call3x0(arg0, arg1, arg2 any) {
	c.fn(arg0.(string), arg1.(int), arg2.(func(string, int)))
}

type callerStringIntГStringInt struct {
	fn func(string, int) (string, int)
}

func funcMakerStringIntГStringInt(fn any) reflectx.Func {
	f := fn.(func(string, int) (string, int))
	return &callerStringIntГStringInt{fn: f}
}

func (c *callerStringIntГStringInt) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *callerStringIntГStringInt) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *callerStringIntГStringInt) Call(args []any) []any {
	out0, out1 := c.fn(args[0].(string), args[1].(int))
	return []any{out0, out1}
}

func (c *callerStringIntГStringInt) Call2x2(arg0, arg1 any) (any, any) {
	return c.fn(arg0.(string), arg1.(int))
}

type emitNative struct {
	n   exec.ElementProcessor
	fn  any
	est *sdf.WatermarkEstimator

	ctx   context.Context
	ws    []typex.Window
	et    typex.EventTime
	value exec.FullValue
}

func (e *emitNative) Init(ctx context.Context, pn typex.PaneInfo, ws []typex.Window, t typex.EventTime) error {
	e.ctx = ctx
	e.ws = ws
	e.et = et
	return nil
}

func (e *emitNative) Value() any {
	return e.fn
}

func (e *emitNative) AttachEstimator(est *sdf.WatermarkEstimator) {
	e.est = est
}

func emitMakerStringInt(n exec.ElementProcessor) exec.ReusableEmitter {
	ret := &emitNative{n: n}
	ret.fn = ret.invokeStringInt
	return ret
}

func (e *emitNative) invokeStringInt(key string, val int) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: e.et, Elm: key, Elm2: val}
	if e.est != nil {
		(*e.est).(sdf.TimestampObservingEstimator).ObserveTimestamp(e.et.ToTime())
	}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

// DO NOT MODIFY: GENERATED CODE
