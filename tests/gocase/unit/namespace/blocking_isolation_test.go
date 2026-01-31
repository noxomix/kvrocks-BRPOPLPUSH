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

package namespace

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestBlockingNamespaceIsolation(t *testing.T) {
	password := "adminpwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin connection
	adminRdb := srv.NewClientWithOption(&redis.Options{
		Password: password,
	})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns2", "token2").Err())

	// Tenant connections
	ns1Rdb := srv.NewClientWithOption(&redis.Options{
		Password: "token1",
	})
	defer func() { require.NoError(t, ns1Rdb.Close()) }()

	ns2Rdb := srv.NewClientWithOption(&redis.Options{
		Password: "token2",
	})
	defer func() { require.NoError(t, ns2Rdb.Close()) }()

	t.Run("Cross-tenant BLPOP isolation - ns2 LPUSH does not wake ns1 BLPOP", func(t *testing.T) {
		key := "blpop_isolation_test"

		// Start BLPOP in ns1 in a goroutine
		var blpopResult []string
		var blpopErr error
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			// Use a separate connection for blocking operation
			ns1BlockRdb := srv.NewClientWithOption(&redis.Options{
				Password: "token1",
			})
			defer ns1BlockRdb.Close()

			// BLPOP with 1 second timeout
			result, err := ns1BlockRdb.BLPop(ctx, 1*time.Second, key).Result()
			blpopResult = result
			blpopErr = err
		}()

		// Give BLPOP time to start blocking
		time.Sleep(100 * time.Millisecond)

		// ns2 pushes to the same key name - should NOT wake ns1's BLPOP
		ns2Rdb.LPush(ctx, key, "secret_value")

		// Wait for BLPOP to timeout
		wg.Wait()

		// BLPOP should have timed out (redis.Nil error)
		require.Error(t, blpopErr)
		require.Equal(t, redis.Nil, blpopErr, "BLPOP should timeout, not receive cross-tenant data")
		require.Nil(t, blpopResult)

		// Cleanup
		ns2Rdb.Del(ctx, key)
	})

	t.Run("Same-namespace BLPOP works correctly", func(t *testing.T) {
		key := "blpop_same_ns_test"

		// Start BLPOP in ns1 in a goroutine
		var blpopResult []string
		var blpopErr error
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			ns1BlockRdb := srv.NewClientWithOption(&redis.Options{
				Password: "token1",
			})
			defer ns1BlockRdb.Close()

			result, err := ns1BlockRdb.BLPop(ctx, 2*time.Second, key).Result()
			blpopResult = result
			blpopErr = err
		}()

		// Give BLPOP time to start blocking
		time.Sleep(100 * time.Millisecond)

		// ns1 (same namespace) pushes - should wake BLPOP
		ns1Rdb.LPush(ctx, key, "hello_ns1")

		// Wait for BLPOP
		wg.Wait()

		// BLPOP should succeed
		require.NoError(t, blpopErr)
		require.Len(t, blpopResult, 2)
		require.Equal(t, key, blpopResult[0])
		require.Equal(t, "hello_ns1", blpopResult[1])
	})

	t.Run("Cross-tenant XREAD BLOCK isolation", func(t *testing.T) {
		stream := "stream_isolation_test"

		// Create stream in ns1
		ns1Rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"init": "value"},
		})

		// Start XREAD BLOCK in ns1
		var xreadResult []redis.XStream
		var xreadErr error
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			ns1BlockRdb := srv.NewClientWithOption(&redis.Options{
				Password: "token1",
			})
			defer ns1BlockRdb.Close()

			result, err := ns1BlockRdb.XRead(ctx, &redis.XReadArgs{
				Streams: []string{stream, "$"},
				Block:   1 * time.Second,
			}).Result()
			xreadResult = result
			xreadErr = err
		}()

		// Give XREAD time to start blocking
		time.Sleep(100 * time.Millisecond)

		// ns2 adds to same stream name - should NOT wake ns1's XREAD
		ns2Rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"secret": "data"},
		})

		// Wait for XREAD to timeout
		wg.Wait()

		// XREAD should have timed out
		require.Error(t, xreadErr)
		require.Equal(t, redis.Nil, xreadErr, "XREAD BLOCK should timeout, not receive cross-tenant data")
		require.Nil(t, xreadResult)

		// Cleanup
		ns1Rdb.Del(ctx, stream)
		ns2Rdb.Del(ctx, stream)
	})

	t.Run("Same-namespace XREAD BLOCK works correctly", func(t *testing.T) {
		stream := "stream_same_ns_test"

		// Create stream in ns1
		ns1Rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"init": "value"},
		})

		// Start XREAD BLOCK in ns1
		var xreadResult []redis.XStream
		var xreadErr error
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			ns1BlockRdb := srv.NewClientWithOption(&redis.Options{
				Password: "token1",
			})
			defer ns1BlockRdb.Close()

			result, err := ns1BlockRdb.XRead(ctx, &redis.XReadArgs{
				Streams: []string{stream, "$"},
				Block:   2 * time.Second,
			}).Result()
			xreadResult = result
			xreadErr = err
		}()

		// Give XREAD time to start blocking
		time.Sleep(100 * time.Millisecond)

		// ns1 (same namespace) adds - should wake XREAD
		ns1Rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"hello": "ns1"},
		})

		// Wait for XREAD
		wg.Wait()

		// XREAD should succeed
		require.NoError(t, xreadErr)
		require.Len(t, xreadResult, 1)
		require.Equal(t, stream, xreadResult[0].Stream)
		require.Len(t, xreadResult[0].Messages, 1)

		// Cleanup
		ns1Rdb.Del(ctx, stream)
	})

	t.Run("INFO blocked_clients shows per-tenant count", func(t *testing.T) {
		key := "blocked_clients_test"

		// Start BLPOP in ns1
		var wg sync.WaitGroup
		wg.Add(1)

		ns1BlockRdb := srv.NewClientWithOption(&redis.Options{
			Password: "token1",
		})

		go func() {
			defer wg.Done()
			ns1BlockRdb.BLPop(ctx, 2*time.Second, key)
		}()

		// Give BLPOP time to start blocking
		time.Sleep(100 * time.Millisecond)

		// Check ns1 sees 1 blocked client
		info1 := ns1Rdb.Info(ctx, "clients").Val()
		require.Contains(t, info1, "blocked_clients:1", "ns1 should see 1 blocked client")

		// Check ns2 sees 0 blocked clients
		info2 := ns2Rdb.Info(ctx, "clients").Val()
		require.Contains(t, info2, "blocked_clients:0", "ns2 should see 0 blocked clients")

		// Check admin sees 1 blocked client (global)
		infoAdmin := adminRdb.Info(ctx, "clients").Val()
		require.Contains(t, infoAdmin, "blocked_clients:1", "admin should see 1 blocked client globally")

		// Cleanup - unblock by pushing
		ns1Rdb.LPush(ctx, key, "unblock")
		wg.Wait()
		ns1BlockRdb.Close()
	})

	t.Run("Cross-tenant BZPOPMIN isolation", func(t *testing.T) {
		key := "bzpopmin_isolation_test"

		// Start BZPOPMIN in ns1
		var result *redis.ZWithKey
		var err error
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			ns1BlockRdb := srv.NewClientWithOption(&redis.Options{
				Password: "token1",
			})
			defer ns1BlockRdb.Close()

			result, err = ns1BlockRdb.BZPopMin(ctx, 1*time.Second, key).Result()
		}()

		// Give BZPOPMIN time to start blocking
		time.Sleep(100 * time.Millisecond)

		// ns2 adds to same key name - should NOT wake ns1's BZPOPMIN
		ns2Rdb.ZAdd(ctx, key, redis.Z{Score: 1.0, Member: "secret"})

		// Wait for BZPOPMIN to timeout
		wg.Wait()

		// Should have timed out
		require.Error(t, err)
		require.Equal(t, redis.Nil, err, "BZPOPMIN should timeout, not receive cross-tenant data")
		require.Nil(t, result)

		// Cleanup
		ns2Rdb.Del(ctx, key)
	})
}

func TestBlockingNamespaceCleanup(t *testing.T) {
	password := "adminpwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()

	adminRdb := srv.NewClientWithOption(&redis.Options{
		Password: password,
	})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespace
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "cleanup_ns", "cleanup_token").Err())

	t.Run("Namespace deletion cleans up blocking structures", func(t *testing.T) {
		// Create tenant connection and start blocking
		nsRdb := srv.NewClientWithOption(&redis.Options{
			Password: "cleanup_token",
		})

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			// This will block until timeout or namespace deletion
			nsRdb.BLPop(ctx, 5*time.Second, "cleanup_key")
		}()

		// Give BLPOP time to start
		time.Sleep(100 * time.Millisecond)

		// Verify blocked client exists
		info := adminRdb.Info(ctx, "clients").Val()
		blockedLine := ""
		for _, line := range strings.Split(info, "\n") {
			if strings.HasPrefix(line, "blocked_clients:") {
				blockedLine = line
				break
			}
		}
		blockedCount, _ := strconv.Atoi(strings.TrimPrefix(strings.TrimSpace(blockedLine), "blocked_clients:"))
		require.GreaterOrEqual(t, blockedCount, 1, "Should have at least 1 blocked client before deletion")

		// Delete namespace - should cleanup blocking structures
		require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "cleanup_ns").Err())

		// Close the connection (will be rejected anyway after namespace deletion)
		nsRdb.Close()
		wg.Wait()
	})
}
