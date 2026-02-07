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

func TestBatchingWatchDirtyAfterTimerCommit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"workers":               "1",
		"batching-enabled":      "yes",
		"batching-max-ops":      "10000",
		"batching-max-bytes":    "1048576",
		"batching-max-delay-us": "100000", // 100ms timer-based flush
	})
	defer srv.Close()

	ctx := context.Background()
	watcher := srv.NewClient()
	defer func() { require.NoError(t, watcher.Close()) }()

	require.NoError(t, watcher.Set(ctx, "watch_timer_k", "old", 0).Err())
	require.NoError(t, watcher.Do(ctx, "WATCH", "watch_timer_k").Err())
	require.NoError(t, watcher.Do(ctx, "MULTI").Err())
	require.NoError(t, watcher.Do(ctx, "GET", "watch_timer_k").Err())

	writer := srv.NewTCPClient()
	defer func() { require.NoError(t, writer.Close()) }()

	// Start a batched write and do NOT force barrier flush.
	require.NoError(t, writer.WriteArgs("SET", "watch_timer_k", "new"))

	// Wait until timer flush commits and writer receives reply.
	done := make(chan struct{})
	go func() {
		writer.MustRead(t, "+OK")
		close(done)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 2*time.Second, 20*time.Millisecond)

	// EXEC must abort because the watched key changed in committed timer-flushed batch.
	require.Nil(t, watcher.Do(ctx, "EXEC").Val())
}

func TestBatchingWatchDirtyAfterOpsThresholdCommit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"workers":               "1",
		"batching-enabled":      "yes",
		"batching-max-ops":      "1", // threshold flush on first write
		"batching-max-bytes":    "1048576",
		"batching-max-delay-us": "700000",
	})
	defer srv.Close()

	ctx := context.Background()
	watcher := srv.NewClient()
	defer func() { require.NoError(t, watcher.Close()) }()

	require.NoError(t, watcher.Set(ctx, "watch_ops_k", "old", 0).Err())
	require.NoError(t, watcher.Do(ctx, "WATCH", "watch_ops_k").Err())
	require.NoError(t, watcher.Do(ctx, "MULTI").Err())
	require.NoError(t, watcher.Do(ctx, "GET", "watch_ops_k").Err())

	writer := srv.NewTCPClient()
	defer func() { require.NoError(t, writer.Close()) }()

	require.NoError(t, writer.WriteArgs("SET", "watch_ops_k", "new"))
	writer.MustRead(t, "+OK")

	// EXEC must abort because threshold-triggered commit modified watched key.
	require.Nil(t, watcher.Do(ctx, "EXEC").Val())
}

func TestBatchingNamespaceSwitchFlushesPreviousBatch(t *testing.T) {
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
	require.NoError(t, admin.Do(ctx, "NAMESPACE", "ADD", "ns_sw_a", "token_sw_a").Err())
	require.NoError(t, admin.Do(ctx, "NAMESPACE", "ADD", "ns_sw_b", "token_sw_b").Err())

	watcherA := srv.NewClientWithOption(&redis.Options{Password: "token_sw_a"})
	defer func() { require.NoError(t, watcherA.Close()) }()
	require.NoError(t, watcherA.Set(ctx, "switch_k", "old_a", 0).Err())
	require.NoError(t, watcherA.Do(ctx, "WATCH", "switch_k").Err())
	require.NoError(t, watcherA.Do(ctx, "MULTI").Err())
	require.NoError(t, watcherA.Do(ctx, "GET", "switch_k").Err())

	conn := srv.NewTCPClient()
	defer func() { require.NoError(t, conn.Close()) }()

	// Enter namespace A, create active batch with deferred reply.
	require.NoError(t, conn.WriteArgs("AUTH", "token_sw_a"))
	conn.MustRead(t, "+OK")
	require.NoError(t, conn.WriteArgs("SET", "switch_k", "new_a"))

	// Switch namespace: this must flush previous namespace batch first.
	require.NoError(t, conn.WriteArgs("AUTH", "token_sw_b"))
	conn.MustRead(t, "+OK") // SET in ns_sw_a committed before auth switch reply
	conn.MustRead(t, "+OK") // AUTH token_sw_b

	// Namespace A watcher must observe committed write (EXEC abort).
	require.Nil(t, watcherA.Do(ctx, "EXEC").Val())
}

func TestBatchingPublishDeferredUntilCommit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"workers":               "1",
		"batching-enabled":      "yes",
		"batching-max-ops":      "10000",
		"batching-max-bytes":    "1048576",
		"batching-max-delay-us": "700000",
	})
	defer srv.Close()

	ctx := context.Background()
	subscriber := srv.NewClient()
	defer func() { require.NoError(t, subscriber.Close()) }()

	sub := subscriber.Subscribe(ctx, "batch_publish_ch")
	defer func() { require.NoError(t, sub.Close()) }()
	first, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.IsType(t, &redis.Subscription{}, first)

	conn := srv.NewTCPClient()
	defer func() { require.NoError(t, conn.Close()) }()

	require.NoError(t, conn.WriteArgs("SET", "batch_publish_k", "v1"))
	require.NoError(t, conn.WriteArgs("PUBLISH", "batch_publish_ch", "m1"))

	_, err = sub.ReceiveTimeout(ctx, 100*time.Millisecond)
	require.Error(t, err)

	require.NoError(t, conn.WriteArgs("APPLYBATCH", "malformed"))
	conn.MustRead(t, "+OK")
	conn.MustRead(t, ":1")
	conn.MustMatch(t, "^-ERR .*")

	received, err := sub.ReceiveTimeout(ctx, time.Second)
	require.NoError(t, err)
	pubsubMsg, ok := received.(*redis.Message)
	require.True(t, ok)
	require.Equal(t, "batch_publish_ch", pubsubMsg.Channel)
	require.Equal(t, "m1", pubsubMsg.Payload)
}

func TestBatchingReplyOverflowDiscardsTxn(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"workers":                  "1",
		"batching-enabled":         "yes",
		"batching-max-ops":         "10000",
		"batching-max-bytes":       "1048576",
		"batching-max-delay-us":    "700000",
		"batching-max-reply-bytes": "4",
	})
	defer srv.Close()

	ctx := context.Background()
	reader := srv.NewClient()
	defer func() { require.NoError(t, reader.Close()) }()

	c := srv.NewTCPClient()
	defer func() { require.NoError(t, c.Close()) }()

	require.NoError(t, c.WriteArgs("SET", "overflow_batch_k", "v1"))
	require.NoError(t, c.WriteArgs("APPLYBATCH", "malformed"))

	c.MustMatch(t, "^-ERR batch reply too large$")
	c.MustMatch(t, "^-ERR .*")

	_, err := reader.Get(ctx, "overflow_batch_k").Result()
	require.ErrorIs(t, err, redis.Nil)
}

func TestBatchingReplyOverflowUnlimited(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"workers":                  "1",
		"batching-enabled":         "yes",
		"batching-max-ops":         "10000",
		"batching-max-bytes":       "1048576",
		"batching-max-delay-us":    "700000",
		"batching-max-reply-bytes": "0",
	})
	defer srv.Close()

	ctx := context.Background()
	reader := srv.NewClient()
	defer func() { require.NoError(t, reader.Close()) }()

	c := srv.NewTCPClient()
	defer func() { require.NoError(t, c.Close()) }()

	require.NoError(t, c.WriteArgs("SET", "unlimited_batch_k", "v1"))
	require.NoError(t, c.WriteArgs("APPLYBATCH", "malformed"))

	c.MustRead(t, "+OK")
	c.MustMatch(t, "^-ERR .*")
	require.Equal(t, "v1", reader.Get(ctx, "unlimited_batch_k").Val())
}

func TestBatchingReplyPerMessageLimitNoCumulativeOverflow(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"workers":                  "1",
		"batching-enabled":         "yes",
		"batching-max-ops":         "10000",
		"batching-max-bytes":       "1048576",
		"batching-max-delay-us":    "700000",
		"batching-max-reply-bytes": "6",
	})
	defer srv.Close()

	ctx := context.Background()
	reader := srv.NewClient()
	defer func() { require.NoError(t, reader.Close()) }()

	c := srv.NewTCPClient()
	defer func() { require.NoError(t, c.Close()) }()

	require.NoError(t, c.WriteArgs("SET", "per_reply_k1", "v1"))
	require.NoError(t, c.WriteArgs("SET", "per_reply_k2", "v2"))
	require.NoError(t, c.WriteArgs("APPLYBATCH", "malformed"))

	c.MustRead(t, "+OK")
	c.MustRead(t, "+OK")
	c.MustMatch(t, "^-ERR .*")
	require.Equal(t, "v1", reader.Get(ctx, "per_reply_k1").Val())
	require.Equal(t, "v2", reader.Get(ctx, "per_reply_k2").Val())
}
