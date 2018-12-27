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

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
)

type WrappedSession struct {
	mongo.Session
}

var _ mongo.Session = (*WrappedSession)(nil)

func (ws *WrappedSession) EndSession(ctx context.Context) {
	ctx, span := roundtripTrackingSpan(ctx, "github.com/mongodb/mongo-go-driver.Session.EndSession")
	defer span.end(ctx)

	ws.Session.EndSession(ctx)
}

func (ws *WrappedSession) StartTransaction(topts ...*options.TransactionOptions) error {
	return ws.Session.StartTransaction(topts...)
}

func (ws *WrappedSession) AbortTransaction(ctx context.Context) error {
	ctx, span := roundtripTrackingSpan(ctx, "github.com/mongodb/mongo-go-driver.Session.AbortTransaction")
	defer span.end(ctx)

	err := ws.Session.AbortTransaction(ctx)
	if err != nil {
		span.setError(err)
	}
	return err
}

func (ws *WrappedSession) CommitTransaction(ctx context.Context) error {
	ctx, span := roundtripTrackingSpan(ctx, "github.com/mongodb/mongo-go-driver.Session.CommitTransaction")
	defer span.end(ctx)

	err := ws.Session.CommitTransaction(ctx)
	if err != nil {
		span.setError(err)
	}
	return err
}

func (ws *WrappedSession) ClusterTime() bson.Raw {
	return ws.Session.ClusterTime()
}

func (ws *WrappedSession) AdvanceClusterTime(br bson.Raw) error {
	return ws.Session.AdvanceClusterTime(br)
}

func (ws *WrappedSession) OperationTime() *primitive.Timestamp {
	return ws.Session.OperationTime()
}

func (ws *WrappedSession) AdvanceOperationTime(pt *primitive.Timestamp) error {
	return ws.Session.AdvanceOperationTime(pt)
}
