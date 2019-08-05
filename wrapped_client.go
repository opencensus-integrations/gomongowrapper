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
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type WrappedClient struct {
	cc *mongo.Client
}

func NewClient(opts ...*options.ClientOptions) (*WrappedClient, error) {
	client, err := mongo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &WrappedClient{cc: client}, nil
}

func (wc *WrappedClient) Connect(ctx context.Context) error {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Client.Connect")
	defer span.end(ctx)

	err := wc.cc.Connect(ctx)
	if err != nil {
		span.setError(err)
	}
	return err
}

func (wc *WrappedClient) Database(name string, opts ...*options.DatabaseOptions) *WrappedDatabase {
	db := wc.cc.Database(name, opts...)
	if db == nil {
		return nil
	}
	return &WrappedDatabase{db: db}
}

func (wc *WrappedClient) Disconnect(ctx context.Context) error {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Client.Disconnect")
	defer span.end(ctx)

	err := wc.cc.Disconnect(ctx)
	if err != nil {
		span.setError(err)
	}
	return err
}

func (wc *WrappedClient) ListDatabaseNames(ctx context.Context, filter interface{}, opts ...*options.ListDatabasesOptions) ([]string, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Client.ListDatabaseNames")
	defer span.end(ctx)

	dbs, err := wc.cc.ListDatabaseNames(ctx, filter, opts...)
	if err != nil {
		span.setError(err)
	}
	return dbs, err
}

func (wc *WrappedClient) ListDatabases(ctx context.Context, filter interface{}, opts ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Client.ListDatabases")
	defer span.end(ctx)

	dbr, err := wc.cc.ListDatabases(ctx, filter, opts...)
	if err != nil {
		span.setError(err)
	}
	return dbr, err
}

func (wc *WrappedClient) Ping(ctx context.Context, rp *readpref.ReadPref) error {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.Client.Ping")
	defer span.end(ctx)

	err := wc.cc.Ping(ctx, rp)
	if err != nil {
		span.setError(err)
	}
	return err
}

func (wc *WrappedClient) StartSession(opts ...*options.SessionOptions) (mongo.Session, error) {
	ss, err := wc.cc.StartSession(opts...)
	if err != nil {
		return nil, err
	}
	return &WrappedSession{Session: ss}, nil
}

func (wc *WrappedClient) UseSession(ctx context.Context, fn func(mongo.SessionContext) error) error {
	return wc.cc.UseSession(ctx, fn)
}

func (wc *WrappedClient) UseSessionWithOptions(ctx context.Context, opts *options.SessionOptions, fn func(mongo.SessionContext) error) error {
	return wc.cc.UseSessionWithOptions(ctx, opts, fn)
}

func (wc *WrappedClient) Client() *mongo.Client { return wc.cc }
