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

package primitives

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/teststream"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
)

func init() {
	register.DoFn2x0[[]byte, func(beam.EventTime, string, int)](&makeTimestampedData{})

	register.Function3x1(filterAbove)
	register.Function4x1(filterAboveOther)
	register.Emitter1[float64]()
	register.Emitter1[int]()
	register.Iter1[float64]()

}

type testKv struct {
	K  string `json:"K,omitempty"`
	Vs []int  `json:"Vs,omitempty"`
}

func (t *testKv) MarshalJSON() ([]byte, error) {
	type NoMethod testKv
	raw := NoMethod(*t)
	return json.Marshal(raw)
}

func (t *testKv) UnmarshalJSON(d []byte) error {
	type NoMethod testKv
	var t1 NoMethod
	err := json.Unmarshal(d, &t1)
	if err != nil {
		return err
	}
	t.K = t1.K
	t.Vs = t1.Vs
	return nil
}

// makeTimestampedData produces data timestamped with the ordinal.
type makeTimestampedData struct {
	Data []*testKv
}

func (f *makeTimestampedData) ProcessElement(_ []byte, emit func(beam.EventTime, string, int)) {
	for i, v := range f.Data {
		timestamp := mtime.FromMilliseconds(int64((i + 1) * 1000)).Subtract(10 * time.Millisecond)
		for _, vv := range v.Vs {
			emit(timestamp, v.K, vv)
		}
	}
}

func MapWindows(s beam.Scope) {
	col := beam.ParDo(s, &makeTimestampedData{Data: []*testKv{
		{K: "foo", Vs: []int{1, 2, 3}},
		{K: "barbar", Vs: []int{4, 5, 6}},
		{K: "bazbazbaz", Vs: []int{2, 5, 9}},
	}}, beam.Impulse(s))
	windowed := beam.WindowInto(s, window.NewFixedWindows(3*time.Second), col)
	ks := filter.Distinct(s, beam.DropValue(s, windowed))
	keyLengths := beam.ParDo(s, func(k string) (string, int) {
		return k, len(k)
	}, ks)
	filtered := beam.ParDo(s, filterAboveOther, windowed, beam.SideInput{Input: keyLengths})
	globalFiltered := beam.WindowInto(s, window.NewGlobalWindows(), filtered)
	passert.Sum(s, globalFiltered, "a", 3, 18)
}

func MapWindowsStream(s beam.Scope) {
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0)
	con.AddElements(2000, 3.0, 4.0)
	col := teststream.Create(s, con)

	windowed := beam.WindowInto(s, window.NewFixedWindows(1*time.Second), col)
	mean := stats.Mean(s, windowed)
	filtered := beam.ParDo(s, filterAbove, windowed, beam.SideInput{Input: mean})
	globalFiltered := beam.WindowInto(s, window.NewGlobalWindows(), filtered)
	passert.Sum(s, globalFiltered, "a", 4, 30)
}

func filterAboveOther(k string, v int, cuttoffFunc func(string) func(*int) bool, emit func(int)) error {
	var cutoff int
	ok := cuttoffFunc(k)(&cutoff)
	if !ok {
		return fmt.Errorf("no cutoff found")
	}
	if v >= cutoff {
		emit(v)
	}
	return nil
}

func filterAbove(element float64, cutoffLookup func(*float64) bool, emit func(int)) error {
	var cutoff float64
	ok := cutoffLookup(&cutoff)
	if !ok {
		return fmt.Errorf("no mean provided")
	}
	if element > cutoff {
		emit(int(element))
	}
	return nil
}
