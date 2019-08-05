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

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type WrappedCollection struct {
	coll *mongo.Collection
}

func (wc *WrappedCollection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.Aggregate")
	defer span.end(ctx)

	cur, err := wc.coll.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		span.setError(err)
	}
	return cur, err
}

func (wc *WrappedCollection) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.BulkWrite")
	defer span.end(ctx)

	bwres, err := wc.coll.BulkWrite(ctx, models, opts...)
	if err != nil {
		span.setError(err)
	}
	return bwres, err
}

func (wc *WrappedCollection) Clone(opts ...*options.CollectionOptions) (*mongo.Collection, error) {
	return wc.coll.Clone(opts...)
}

func (wc *WrappedCollection) Count(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.Count")
	defer span.end(ctx)
	count, err := wc.coll.CountDocuments(ctx, filter, opts...)
	if err != nil {
		span.setError(err)
	}
	return count, err
}

func (wc *WrappedCollection) CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.CountDocuments")
	defer span.end(ctx)

	count, err := wc.coll.CountDocuments(ctx, filter, opts...)
	if err != nil {
		span.setError(err)
	}
	return count, err
}

func (wc *WrappedCollection) Database() *mongo.Database { return wc.coll.Database() }

func (wc *WrappedCollection) DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.DeleteMany")
	defer span.end(ctx)

	dmres, err := wc.coll.DeleteMany(ctx, filter, opts...)
	if err != nil {
		span.setError(err)
	}
	return dmres, err
}

func (wc *WrappedCollection) DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.DeleteOne")
	defer span.end(ctx)

	dor, err := wc.coll.DeleteOne(ctx, filter, opts...)
	if err != nil {
		span.setError(err)
	}
	return dor, err
}

func (wc *WrappedCollection) Distinct(ctx context.Context, fieldName string, filter interface{}, opts ...*options.DistinctOptions) ([]interface{}, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.Distinct")
	defer span.end(ctx)

	distinct, err := wc.coll.Distinct(ctx, fieldName, filter, opts...)
	if err != nil {
		span.setError(err)
	}
	return distinct, err
}

func (wc *WrappedCollection) Drop(ctx context.Context) error {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.Drop")
	defer span.end(ctx)

	err := wc.coll.Drop(ctx)
	if err != nil {
		span.setError(err)
	}
	return err
}

func (wc *WrappedCollection) EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.EstimatedDocumentCount")
	defer span.end(ctx)

	count, err := wc.coll.EstimatedDocumentCount(ctx, opts...)
	if err != nil {
		span.setError(err)
	}
	return count, err
}

func (wc *WrappedCollection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.Find")
	defer span.end(ctx)

	cur, err := wc.coll.Find(ctx, filter, opts...)
	if err != nil {
		span.setError(err)
	}
	return cur, err
}

func (wc *WrappedCollection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.FindOne")
	defer span.end(ctx)

	return wc.coll.FindOne(ctx, filter, opts...)
}

func (wc *WrappedCollection) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) *mongo.SingleResult {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.FindOneAndDelete")
	defer span.end(ctx)

	return wc.coll.FindOneAndDelete(ctx, filter, opts...)
}

func (wc *WrappedCollection) FindOneAndReplace(ctx context.Context, filter, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) *mongo.SingleResult {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.FindOneAndReplace")
	defer span.end(ctx)

	return wc.coll.FindOneAndReplace(ctx, filter, replacement, opts...)
}

func (wc *WrappedCollection) FindOneAndUpdate(ctx context.Context, filter, update interface{}, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.FindOneAndUpdate")
	defer span.end(ctx)

	return wc.coll.FindOneAndUpdate(ctx, filter, update, opts...)
}

func (wc *WrappedCollection) Indexes() mongo.IndexView { return wc.coll.Indexes() }

func (wc *WrappedCollection) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.InsertMany")
	defer span.end(ctx)

	insmres, err := wc.coll.InsertMany(ctx, documents, opts...)
	if err != nil {
		span.setError(err)
	}
	return insmres, err
}

func (wc *WrappedCollection) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.InsertOne")
	defer span.end(ctx)

	insores, err := wc.coll.InsertOne(ctx, document, opts...)
	if err != nil {
		span.setError(err)
	}
	return insores, err
}

func (wc *WrappedCollection) Name() string { return wc.coll.Name() }

func (wc *WrappedCollection) ReplaceOne(ctx context.Context, filter, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.ReplaceOne")
	defer span.end(ctx)

	repres, err := wc.coll.ReplaceOne(ctx, filter, replacement, opts...)
	if err != nil {
		span.setError(err)
	}
	return repres, err
}

func (wc *WrappedCollection) UpdateMany(ctx context.Context, filter, replacement interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.UpdateMany")
	defer span.end(ctx)

	umres, err := wc.coll.UpdateMany(ctx, filter, replacement, opts...)
	if err != nil {
		span.setError(err)
	}
	return umres, err
}

func (wc *WrappedCollection) UpdateOne(ctx context.Context, filter, replacement interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.UpdateOne")
	defer span.end(ctx)

	uores, err := wc.coll.UpdateOne(ctx, filter, replacement, opts...)
	if err != nil {
		span.setError(err)
	}
	return uores, err
}

func (wc *WrappedCollection) Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Collection.Watch")
	defer span.end(ctx)

	cs, err := wc.coll.Watch(ctx, pipeline, opts...)
	if err != nil {
		span.setError(err)
	}
	return cs, err
}

func (wc *WrappedCollection) Collection() *mongo.Collection {
	return wc.coll
}
