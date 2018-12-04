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

package mongowrapper_test

import (
	"context"
	"log"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"

	"github.com/opencensus-integrations/gomongowrapper"

	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

func Example() {
	// Enabling the OpenCensus exporter.
	// Just using Stackdriver since it has both Tracing and Metrics
	// and is easy to whip up. Add your desired one here.
	sde, err := stackdriver.NewExporter(stackdriver.Options{
		ProjectID:    "census-demos",
		MetricPrefix: "mongosample",
	})
	if err != nil {
		log.Fatalf("Failed to create Stackdriver exporter: %v", err)
	}
	view.RegisterExporter(sde)
	trace.RegisterExporter(sde)
	if err := mongowrapper.RegisterAllViews(); err != nil {
		log.Fatalf("Failed to register all views: %v\n", err)
	}

	defer func() {
		<-time.After(2 * time.Minute)
	}()

	// Start a span like your application would start one.
	ctx, span := trace.StartSpan(context.Background(), "Fetch", trace.WithSampler(trace.AlwaysSample()))
	defer span.End()

        // Now for the mongo connections, using the context
        // with the span in it for continuity.
	client, err := mongowrapper.NewClient("mongodb://localhost:27017")
	if err != nil {
		log.Fatalf("Failed to create the new client: %v", err)
	}
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Failed to open client connection: %v", err)
	}
	defer client.Disconnect(ctx)
	coll := client.Database("the_db").Collection("music")

	q := bson.M{"name": "Examples"}
	cur, err := coll.Find(ctx, q)
	if err != nil {
		log.Fatalf("Find error: %v", err)
	}

	for cur.Next(ctx) {
		elem := make(map[string]int)
		if err := cur.Decode(elem); err != nil {
			log.Printf("Decode error: %v", err)
			continue
		}
		log.Printf("Got result: %v\n", elem)
	}
	log.Print("Done iterating")

	_, err = coll.DeleteMany(ctx, q)
	if err != nil {
		log.Fatalf("Failed to delete: %v", err)
	}
}
