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

	"github.com/mongodb/mongo-go-driver/bson"

	"github.com/opencensus-integrations/gomongowrapper"

	"go.opencensus.io/trace"
)

func Example() {
	client, err := mongowrapper.NewClient("mongodb://foo:bar@localhost:27017")
	if err != nil {
		log.Fatalf("Failed to create the new client: %v", err)
	}
	coll := client.Database("the_db").Collection("music")

	ctx, span := trace.StartSpan(context.Background(), "Fetch")
	defer span.End()

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
}
