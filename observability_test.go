// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mongowrapper

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

func TestUnitRoundtripTrackingOperation(t *testing.T) {
	if err := RegisterAllViews(); err != nil {
		t.Fatalf("Failed to register all the views: %v", err)
	}
	defer UnregisterAllViews()

	reportingPeriod := 200 * time.Millisecond
	view.SetReportingPeriod(reportingPeriod)
	defer view.SetReportingPeriod(time.Minute)

	viewDataChan := make(chan *view.Data, 1)
	spanDataChan := make(chan *trace.SpanData, 1)
	exp := &mockExporter{viewDataChan: viewDataChan, spanDataChan: spanDataChan}
	view.RegisterExporter(exp)
	defer view.UnregisterExporter(exp)

	trace.RegisterExporter(exp)
	defer trace.UnregisterExporter(exp)

	pausePeriod := 28 * time.Millisecond
	deadline := time.Now().Add(reportingPeriod)
	_, rts := roundtripTrackingSpan(context.Background(), "a.b.c/D.Foo", trace.WithSampler(trace.AlwaysSample()))
	<-time.After(pausePeriod / 2)
	errMsg := "This is an error"
	rts.setError(errors.New(errMsg))
	<-time.After(pausePeriod / 2)
	rts.end(context.Background())

	// Verifying the spans since those don't
	// operate on a frequency.
	sd0 := <-spanDataChan
	// Comparing the name
	if g, w := sd0.Name, "a.b.c/D.Foo"; g != w {
		t.Errorf("SpanData.Name mismatch:: Got %q Want %q", g, w)
	}
	wantStatus := trace.Status{Code: trace.StatusCodeUnknown, Message: errMsg}
	if g, w := sd0.Status, wantStatus; g != w {
		t.Errorf("SpanData.Status mismatch:: Got %#v Want %#v", g, w)
	}
	minPeriod := pausePeriod
	gotPeriod := sd0.EndTime.Sub(sd0.StartTime)
	if gotPeriod < minPeriod {
		t.Errorf("SpanData.TimeSpent:: Got %s Want min: %s", gotPeriod, minPeriod)
	}

	wait := deadline.Sub(time.Now()) + 3*time.Millisecond
	<-time.After(wait)

	var vds []*view.Data
	maxWaitViewsTimer := time.NewTimer(wait)
	for done := false; !done; {
		select {
		case <-maxWaitViewsTimer.C:
			done = true
			break
		case vd := <-viewDataChan:
			// Great!
			vds = append(vds, vd)
		}
	}

	if len(vds) < 2 {
		t.Errorf("Got %d ViewData; expected at least 2", len(vds))
	}

	vdLatency := vds[0]
	vdCalls := vds[1]
	if strings.HasSuffix(vdCalls.View.Name, "client/latency") {
		// The order of reporting was wrong, so swap them.
		vdLatency, vdCalls = vdCalls, vdLatency
	}

	// From this point on, we should have the proper views.

	// Start examining Latency view.
	wantvLatency := &view.View{
		Name:        "mongo/client/latency",
		Description: "The latency of the various calls",
		Measure:     mLatencyMs,
		Aggregation: latencyDistribution,
		TagKeys:     []tag.Key{keyError, keyMethod, keyStatus},
	}
	if g, w := vdLatency.View, wantvLatency; !reflect.DeepEqual(g, w) {
		t.Errorf("Latency.ViewData:\nGot: %#v\nWant:%#v\n", g, w)
	}
	if g, w := len(vdLatency.Rows), 1; g != w {
		t.Errorf("Latency.ViewData.Rows: Got %d Wanted %d", g, w)
	} else {
		r0 := vdLatency.Rows[0]
		// We need to have the row with the tag "error" since we ended with an error"
		wantTags := []tag.Tag{{Key: keyError, Value: errMsg}}
		if !reflect.DeepEqual(wantTags, r0.Tags) {
			t.Errorf("Latency.ViewData.Rows[0].Tags mismatch\nGot: %#v\nWant:%#v\n", r0.Tags, wantTags)
		}

		// Compare the latency
		d0 := r0.Data.(*view.DistributionData)
		if g, w := d0.Count, int64(1); g != w {
			t.Errorf("DistributionData.Count:: Got %d Want %d", g, w)
		}
		if d0.Min != d0.Max || d0.Max != d0.Mean || d0.Min != d0.Mean {
			t.Errorf("DistributionData:: Expected all equal of these values:\nMin: %.4f Mean: %.4f Max: %.4f", d0.Min, d0.Mean, d0.Max)
		}
		if g, w := d0.Max, float64(pausePeriod)/float64(time.Millisecond); g < w {
			t.Errorf("Distribution.Max:: Got %.4f < Want %.4f", g, w)
		}
	}
	// End examining Latency view.

	// Start examining Calls view.
	wantvCalls := &view.View{
		Name:        "mongo/client/calls",
		Description: "The various calls",
		Measure:     mLatencyMs,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{keyError, keyMethod, keyStatus},
	}
	if g, w := vdCalls.View, wantvCalls; !reflect.DeepEqual(g, w) {
		t.Errorf("Calls.ViewData:\nGot: %#v\nWant:%#v\n", g, w)
	}
	if g, w := len(vdCalls.Rows), 1; g != w {
		t.Errorf("Calls.ViewdAta.Rows: Got %d Wanted %d", g, w)
	} else {
		r0 := vdCalls.Rows[0]
		wantTags := []tag.Tag{{Key: keyError, Value: errMsg}}
		if !reflect.DeepEqual(wantTags, r0.Tags) {
			t.Errorf("Calls.ViewData.Rows[0].Tags mismatch\nGot: %#v\nWant:%#v\n", r0.Tags, wantTags)
		}

		// Now comparing the actual value
		d0 := r0.Data.(*view.CountData)
		if g, w := d0.Value, int64(1); g != w {
			t.Errorf("CountData.Count.Value:: Got %d Want %d", g, w)
		}
	}
	// End examining Calls view.
}

type mockExporter struct {
	viewDataChan chan *view.Data
	spanDataChan chan *trace.SpanData
}

var _ trace.Exporter = (*mockExporter)(nil)
var _ view.Exporter = (*mockExporter)(nil)

func (me *mockExporter) ExportView(vd *view.Data) {
	me.viewDataChan <- vd
}

func (me *mockExporter) ExportSpan(sd *trace.SpanData) {
	me.spanDataChan <- sd
}
