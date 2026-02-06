/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package connection

import (
	"context"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestBatchingNamespaceLockBlocksVisibilityUntilCommit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"requirepass":           "adminpwd",
		"workers":               "8",
		"batching-enabled":      "yes",
		"batching-max-ops":      "10000",
		"batching-max-bytes":    "1048576",
		"batching-max-delay-us": "500000",
	})
	defer srv.Close()

	ctx := context.Background()
	admin := srv.NewClientWithOption(&redis.Options{Password: "adminpwd"})
	defer func() { require.NoError(t, admin.Close()) }()
	require.NoError(t, admin.Do(ctx, "NAMESPACE", "ADD", "ns_batch", "token_batch").Err())

	writer := srv.NewClientWithOption(&redis.Options{Password: "token_batch"})
	reader := srv.NewClientWithOption(&redis.Options{Password: "token_batch"})
	defer func() { require.NoError(t, writer.Close()) }()
	defer func() { require.NoError(t, reader.Close()) }()

	require.NoError(t, writer.Set(ctx, "batch_k", "old", 0).Err())
	require.NoError(t, writer.Do(ctx, "CLIENT", "SETNAME", "writer_batch").Err())
	require.NoError(t, reader.Do(ctx, "CLIENT", "SETNAME", "reader_batch").Err())

	writeDone := make(chan error, 1)
	start := time.Now()
	go func() {
		writeDone <- writer.Set(ctx, "batch_k", "new", 0).Err()
	}()

	// The write reply should stay pending while batch window is open.
	select {
	case err := <-writeDone:
		require.NoError(t, err)
		t.Fatal("batched write finished too early; expected deferred reply until commit")
	case <-time.After(60 * time.Millisecond):
	}

	// After commit, both write and read complete with committed value.
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("batched write did not complete after commit window")
	}
	require.GreaterOrEqual(t, time.Since(start), 250*time.Millisecond)
	require.Equal(t, "new", reader.Get(ctx, "batch_k").Val())
}
