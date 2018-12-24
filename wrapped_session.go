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
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/transactionopt"
)

type wrappedSession struct {
	ss mongo.Session
}

func (ws *wrappedSession) EndSession(ctx context.Context) {
	ctx, span := roundtripTrackingSpan(ctx, "github.com/mongodb/mongo-go-driver.Session.EndSession")
	defer span.end(ctx)

	ws.ss.EndSession(ctx)
}

func (ws *wrappedSession) StartTransaction(topts ...transactionopt.Transaction) error {
	return ws.ss.StartTransaction(topts...)
}

func (ws *wrappedSession) AbortTransaction(ctx context.Context) error {
	ctx, span := roundtripTrackingSpan(ctx, "github.com/mongodb/mongo-go-driver.Session.AbortTransaction")
	defer span.end(ctx)

	err := ws.ss.AbortTransaction(ctx)
	if err != nil {
		span.setError(err)
	}
	return err
}

func (ws *wrappedSession) CommitTransaction(ctx context.Context) error {
	ctx, span := roundtripTrackingSpan(ctx, "github.com/mongodb/mongo-go-driver.Session.CommitTransaction")
	defer span.end(ctx)

	err := ws.ss.CommitTransaction(ctx)
	if err != nil {
		span.setError(err)
	}
	return err
}

func (ws *wrappedSession) ClusterTime() *bson.Document {
	return ws.ss.ClusterTime()
}
