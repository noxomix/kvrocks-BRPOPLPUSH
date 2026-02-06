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

func TestBatchingApplyBatchActsAsBarrier(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"workers":               "1",
		"batching-enabled":      "yes",
		"batching-max-ops":      "10000",
		"batching-max-bytes":    "1048576",
		"batching-max-delay-us": "700000",
	})
	defer srv.Close()

	c := srv.NewTCPClient()
	defer func() { require.NoError(t, c.Close()) }()

	require.NoError(t, c.WriteArgs("SET", "applybatch_barrier_k", "old"))
	c.MustRead(t, "+OK")

	start := time.Now()
	require.NoError(t, c.WriteArgs("SET", "applybatch_barrier_k", "new"))
	require.NoError(t, c.WriteArgs("APPLYBATCH", "malformed"))

	c.MustRead(t, "+OK")
	require.Less(t, time.Since(start), 500*time.Millisecond)
	c.MustMatch(t, "^-ERR .*")
}

func TestBatchingAuthActsAsBarrier(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"requirepass":           "adminpwd",
		"workers":               "1",
		"batching-enabled":      "yes",
		"batching-max-ops":      "10000",
		"batching-max-bytes":    "1048576",
		"batching-max-delay-us": "700000",
	})
	defer srv.Close()

	ctx := context.Background()
	admin := srv.NewClientWithOption(&redis.Options{Password: "adminpwd"})
	defer func() { require.NoError(t, admin.Close()) }()
	require.NoError(t, admin.Do(ctx, "NAMESPACE", "ADD", "ns_auth_a", "token_auth_a").Err())
	require.NoError(t, admin.Do(ctx, "NAMESPACE", "ADD", "ns_auth_b", "token_auth_b").Err())

	c := srv.NewTCPClient()
	defer func() { require.NoError(t, c.Close()) }()

	require.NoError(t, c.WriteArgs("AUTH", "token_auth_a"))
	c.MustRead(t, "+OK")
	require.NoError(t, c.WriteArgs("SET", "auth_barrier_k", "old"))
	c.MustRead(t, "+OK")

	start := time.Now()
	require.NoError(t, c.WriteArgs("SET", "auth_barrier_k", "new"))
	require.NoError(t, c.WriteArgs("AUTH", "token_auth_b"))

	c.MustRead(t, "+OK")
	require.Less(t, time.Since(start), 500*time.Millisecond)
	c.MustRead(t, "+OK")
}

func TestBatchingHelloAuthActsAsBarrier(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"requirepass":           "adminpwd",
		"workers":               "1",
		"batching-enabled":      "yes",
		"batching-max-ops":      "10000",
		"batching-max-bytes":    "1048576",
		"batching-max-delay-us": "700000",
	})
	defer srv.Close()

	ctx := context.Background()
	admin := srv.NewClientWithOption(&redis.Options{Password: "adminpwd"})
	defer func() { require.NoError(t, admin.Close()) }()
	require.NoError(t, admin.Do(ctx, "NAMESPACE", "ADD", "ns_hello_a", "token_hello_a").Err())
	require.NoError(t, admin.Do(ctx, "NAMESPACE", "ADD", "ns_hello_b", "token_hello_b").Err())

	c := srv.NewTCPClient()
	defer func() { require.NoError(t, c.Close()) }()

	require.NoError(t, c.WriteArgs("AUTH", "token_hello_a"))
	c.MustRead(t, "+OK")
	require.NoError(t, c.WriteArgs("SET", "hello_barrier_k", "old"))
	c.MustRead(t, "+OK")

	start := time.Now()
	require.NoError(t, c.WriteArgs("SET", "hello_barrier_k", "new"))
	require.NoError(t, c.WriteArgs("HELLO", "3", "AUTH", "default", "token_hello_b"))

	c.MustRead(t, "+OK")
	require.Less(t, time.Since(start), 500*time.Millisecond)
	c.MustMatch(t, "^[*%].*")
}

func TestBatchingResetActsAsBarrier(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"requirepass":           "adminpwd",
		"workers":               "1",
		"batching-enabled":      "yes",
		"batching-max-ops":      "10000",
		"batching-max-bytes":    "1048576",
		"batching-max-delay-us": "700000",
	})
	defer srv.Close()

	c := srv.NewTCPClient()
	defer func() { require.NoError(t, c.Close()) }()

	require.NoError(t, c.WriteArgs("AUTH", "adminpwd"))
	c.MustRead(t, "+OK")
	require.NoError(t, c.WriteArgs("SET", "reset_barrier_k", "old"))
	c.MustRead(t, "+OK")

	start := time.Now()
	require.NoError(t, c.WriteArgs("SET", "reset_barrier_k", "new"))
	require.NoError(t, c.WriteArgs("RESET"))

	c.MustRead(t, "+OK")
	require.Less(t, time.Since(start), 500*time.Millisecond)
	c.MustRead(t, "+RESET")
}

func TestBatchingWatchDirtyAfterSuccessfulCommit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"workers":               "1",
		"batching-enabled":      "yes",
		"batching-max-ops":      "10000",
		"batching-max-bytes":    "1048576",
		"batching-max-delay-us": "700000",
	})
	defer srv.Close()

	ctx := context.Background()
	watcher := srv.NewClient()
	defer func() { require.NoError(t, watcher.Close()) }()

	require.NoError(t, watcher.Set(ctx, "watch_batch_k", "old", 0).Err())
	require.NoError(t, watcher.Do(ctx, "WATCH", "watch_batch_k").Err())
	require.NoError(t, watcher.Do(ctx, "MULTI").Err())
	require.NoError(t, watcher.Do(ctx, "GET", "watch_batch_k").Err())

	writer := srv.NewTCPClient()
	defer func() { require.NoError(t, writer.Close()) }()

	require.NoError(t, writer.WriteArgs("SET", "watch_batch_k", "new"))
	require.NoError(t, writer.WriteArgs("APPLYBATCH", "malformed"))
	writer.MustRead(t, "+OK")
	writer.MustMatch(t, "^-ERR .*")

	// EXEC must abort because watched key was modified in a successfully committed batch.
	require.Nil(t, watcher.Do(ctx, "EXEC").Val())
}
