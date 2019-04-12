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
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type WrappedDatabase struct {
	mu sync.Mutex
	db *mongo.Database
}

func (wd *WrappedDatabase) Client() *WrappedClient {
	wd.mu.Lock()
	defer wd.mu.Unlock()

	cc := wd.db.Client()
	if cc == nil {
		return nil
	}
	return &WrappedClient{cc: cc}
}

func (wd *WrappedDatabase) Collection(name string, opts ...*options.CollectionOptions) *WrappedCollection {
	if wd.db == nil {
		return nil
	}
	coll := wd.db.Collection(name, opts...)
	if coll == nil {
		return nil
	}
	return &WrappedCollection{coll: coll}
}

func (wd *WrappedDatabase) Drop(ctx context.Context) error {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Database.Drop")
	defer span.end(ctx)

	err := wd.db.Drop(ctx)
	if err != nil {
		span.setError(err)
	}
	return err
}

func (wd *WrappedDatabase) ListCollections(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) (*mongo.Cursor, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Database.ListCollections")
	defer span.end(ctx)

	cur, err := wd.db.ListCollections(ctx, filter, opts...)
	if err != nil {
		span.setError(err)
	}
	return cur, err
}

func (wd *WrappedDatabase) Name() string                          { return wd.db.Name() }
func (wd *WrappedDatabase) ReadConcern() *readconcern.ReadConcern { return wd.db.ReadConcern() }
func (wd *WrappedDatabase) ReadPreference() *readpref.ReadPref    { return wd.db.ReadPreference() }

func (wd *WrappedDatabase) RunCommand(ctx context.Context, runCommand interface{}, opts ...*options.RunCmdOptions) *mongo.SingleResult {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Database.RunCommand")
	defer span.end(ctx)

	return wd.db.RunCommand(ctx, runCommand, opts...)
}

func (wd *WrappedDatabase) WriteConcern() *writeconcern.WriteConcern { return wd.db.WriteConcern() }

func (wc *WrappedDatabase) Database() *mongo.Database { return wc.db }
