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
 */

package multi

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestMultiNamespaceIsolation(t *testing.T) {
	password := "adminpwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin-Client for namespace setup
	adminRdb := srv.NewClientWithOption(&redis.Options{
		Password: password,
	})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns_a", "token_a").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns_b", "token_b").Err())

	// Namespace clients
	clientA := srv.NewClientWithOption(&redis.Options{Password: "token_a"})
	defer func() { require.NoError(t, clientA.Close()) }()
	clientB := srv.NewClientWithOption(&redis.Options{Password: "token_b"})
	defer func() { require.NoError(t, clientB.Close()) }()

	t.Run("EXEC in different namespaces are isolated", func(t *testing.T) {
		// Both clients can execute MULTI/EXEC simultaneously
		require.NoError(t, clientA.Do(ctx, "MULTI").Err())
		require.NoError(t, clientA.Do(ctx, "SET", "key1", "value_a").Err())

		require.NoError(t, clientB.Do(ctx, "MULTI").Err())
		require.NoError(t, clientB.Do(ctx, "SET", "key1", "value_b").Err())

		// Both EXEC should succeed
		require.NoError(t, clientA.Do(ctx, "EXEC").Err())
		require.NoError(t, clientB.Do(ctx, "EXEC").Err())

		// Each namespace has its own value
		require.Equal(t, "value_a", clientA.Get(ctx, "key1").Val())
		require.Equal(t, "value_b", clientB.Get(ctx, "key1").Val())
	})

	t.Run("Transaction read-your-own-writes within namespace", func(t *testing.T) {
		var get *redis.StringCmd
		_, err := clientA.TxPipelined(ctx, func(pipeline redis.Pipeliner) error {
			pipeline.Set(ctx, "ryow_key", "visible_in_txn", 0)
			get = pipeline.Get(ctx, "ryow_key")
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, "visible_in_txn", get.Val())
	})

	t.Run("Transaction in ns_a cannot see uncommitted writes from ns_b", func(t *testing.T) {
		// Setup: Both have the same key name
		require.NoError(t, clientA.Set(ctx, "shared_name_key", "initial_a", 0).Err())
		require.NoError(t, clientB.Set(ctx, "shared_name_key", "initial_b", 0).Err())

		// Client B starts MULTI but doesn't commit yet
		require.NoError(t, clientB.Do(ctx, "MULTI").Err())
		require.NoError(t, clientB.Do(ctx, "SET", "shared_name_key", "uncommitted_b").Err())

		// Client A sees its own value (not B's uncommitted)
		require.Equal(t, "initial_a", clientA.Get(ctx, "shared_name_key").Val())

		// B commits
		require.NoError(t, clientB.Do(ctx, "EXEC").Err())

		// A still sees its own value (namespace isolation)
		require.Equal(t, "initial_a", clientA.Get(ctx, "shared_name_key").Val())
		// B sees its committed value
		require.Equal(t, "uncommitted_b", clientB.Get(ctx, "shared_name_key").Val())
	})

	t.Run("Concurrent EXEC in different namespaces do not block each other", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make(chan time.Duration, 2)

		// Client A executes slow transaction (many commands)
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			require.NoError(t, clientA.Do(ctx, "MULTI").Err())
			for i := 0; i < 100; i++ {
				require.NoError(t, clientA.Do(ctx, "SET", "bulk_key_a", i).Err())
			}
			require.NoError(t, clientA.Do(ctx, "EXEC").Err())
			results <- time.Since(start)
		}()

		// Short pause so A's MULTI starts first
		time.Sleep(10 * time.Millisecond)

		// Client B executes fast transaction
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			require.NoError(t, clientB.Do(ctx, "MULTI").Err())
			require.NoError(t, clientB.Do(ctx, "SET", "quick_key_b", "done").Err())
			require.NoError(t, clientB.Do(ctx, "EXEC").Err())
			results <- time.Since(start)
		}()

		wg.Wait()
		close(results)

		// B should complete quickly (< 100ms), not blocked by A
		durations := make([]time.Duration, 0, 2)
		for d := range results {
			durations = append(durations, d)
		}
		// At least one duration should be very short (B)
		minDuration := durations[0]
		if durations[1] < minDuration {
			minDuration = durations[1]
		}
		require.Less(t, minDuration, 100*time.Millisecond,
			"B should complete quickly without being blocked by A")
	})

	t.Run("DISCARD in one namespace does not affect other namespace", func(t *testing.T) {
		require.NoError(t, clientA.Set(ctx, "discard_key", "original_a", 0).Err())
		require.NoError(t, clientB.Set(ctx, "discard_key", "original_b", 0).Err())

		// A starts MULTI and does DISCARD
		require.NoError(t, clientA.Do(ctx, "MULTI").Err())
		require.NoError(t, clientA.Do(ctx, "SET", "discard_key", "discarded_a").Err())
		require.NoError(t, clientA.Do(ctx, "DISCARD").Err())

		// B can do normal MULTI/EXEC
		require.NoError(t, clientB.Do(ctx, "MULTI").Err())
		require.NoError(t, clientB.Do(ctx, "SET", "discard_key", "committed_b").Err())
		require.NoError(t, clientB.Do(ctx, "EXEC").Err())

		// A still has original, B has committed
		require.Equal(t, "original_a", clientA.Get(ctx, "discard_key").Val())
		require.Equal(t, "committed_b", clientB.Get(ctx, "discard_key").Val())
	})

	t.Run("WATCH in one namespace is independent from other namespace", func(t *testing.T) {
		require.NoError(t, clientA.Set(ctx, "watch_key", "v1_a", 0).Err())
		require.NoError(t, clientB.Set(ctx, "watch_key", "v1_b", 0).Err())

		// A watches its key
		require.NoError(t, clientA.Do(ctx, "WATCH", "watch_key").Err())

		// B modifies its own key (should not affect A)
		require.NoError(t, clientB.Set(ctx, "watch_key", "v2_b", 0).Err())

		// A's EXEC should succeed (its key was not changed)
		require.NoError(t, clientA.Do(ctx, "MULTI").Err())
		require.NoError(t, clientA.Do(ctx, "SET", "watch_key", "v2_a").Err())
		result := clientA.Do(ctx, "EXEC").Val()
		require.NotNil(t, result, "EXEC should succeed, WATCH was not triggered by other namespace")

		require.Equal(t, "v2_a", clientA.Get(ctx, "watch_key").Val())
		require.Equal(t, "v2_b", clientB.Get(ctx, "watch_key").Val())
	})

	// Cleanup
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns_a").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns_b").Err())
}
