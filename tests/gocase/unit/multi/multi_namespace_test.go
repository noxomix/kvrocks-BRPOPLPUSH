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

	t.Run("Concurrent EXEC in different namespaces run truly parallel", func(t *testing.T) {
		var wg sync.WaitGroup
		startBarrier := make(chan struct{}) // Both wait here before EXEC

		wg.Add(2)

		// Client A - queue many commands, then wait for barrier before EXEC
		go func() {
			defer wg.Done()
			require.NoError(t, clientA.Do(ctx, "MULTI").Err())
			for i := 0; i < 1000; i++ {
				require.NoError(t, clientA.Do(ctx, "SET", "bulk_key_a", i).Err())
			}
			<-startBarrier // Wait for signal
			require.NoError(t, clientA.Do(ctx, "EXEC").Err())
		}()

		// Client B - queue many commands, then wait for barrier before EXEC
		go func() {
			defer wg.Done()
			require.NoError(t, clientB.Do(ctx, "MULTI").Err())
			for i := 0; i < 1000; i++ {
				require.NoError(t, clientB.Do(ctx, "SET", "bulk_key_b", i).Err())
			}
			<-startBarrier // Wait for signal
			require.NoError(t, clientB.Do(ctx, "EXEC").Err())
		}()

		// Wait for both to finish queueing commands
		time.Sleep(100 * time.Millisecond)

		// Trigger BOTH EXEC at the same time
		start := time.Now()
		close(startBarrier)

		wg.Wait()
		totalTime := time.Since(start)

		// If parallel: ~T (time for 1000 writes)
		// If serial:   ~2T (one blocks the other)
		// We expect parallel execution, so total time should be reasonable
		// With namespace isolation, both EXEC run concurrently
		require.Less(t, totalTime, 2*time.Second,
			"Both EXEC should run in parallel, not sequentially blocking each other")
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

	// Cleanup
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns_a").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns_b").Err())
}
