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

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type WrappedClientEncryption struct {
	cc *mongo.ClientEncryption
}

func (wc *WrappedClient) NewClientEncryption(opts ...*options.ClientEncryptionOptions) (*WrappedClientEncryption, error) {
	client, err := mongo.NewClientEncryption(wc.Client(), opts...)
	if err != nil {
		return nil, err
	}
	return &WrappedClientEncryption{cc: client}, nil
}

func (wce *WrappedClientEncryption) CreateDataKey(ctx context.Context, kmsProvider string, opts ...*options.DataKeyOptions) (primitive.Binary, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.ClientEncryption.CreateDataKey")
	defer span.end(ctx)

	id, err := wce.cc.CreateDataKey(ctx, kmsProvider, opts...)
	if err != nil {
		span.setError(err)
	}
	return id, err
}

func (wce *WrappedClientEncryption) Encrypt(ctx context.Context, val bson.RawValue, opts ...*options.EncryptOptions) (primitive.Binary, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.ClientEncryption.Encrypt")
	defer span.end(ctx)

	value, err := wce.cc.Encrypt(ctx, val, opts...)
	if err != nil {
		span.setError(err)
	}
	return value, err
}

func (wce *WrappedClientEncryption) Decrypt(ctx context.Context, val primitive.Binary) (bson.RawValue, error) {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.ClientEncryption.Decrypt")
	defer span.end(ctx)

	value, err := wce.cc.Decrypt(ctx, val)
	if err != nil {
		span.setError(err)
	}
	return value, err
}

func (wce *WrappedClientEncryption) Close(ctx context.Context) error {
	ctx, span := roundtripTrackingSpan(ctx, "go.mongodb.org/mongo-driver.ClientEncryption.Close")
	defer span.end(ctx)

	err := wce.cc.Close(ctx)
	if err != nil {
		span.setError(err)
	}
	return err
}
