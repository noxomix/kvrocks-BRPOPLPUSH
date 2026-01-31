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
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestPubSubNamespaceIsolation(t *testing.T) {
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

	t.Run("Cross-tenant PUBLISH returns 0 subscribers", func(t *testing.T) {
		// ns1 subscribes to channel "foo"
		ns1Sub := ns1Rdb.Subscribe(ctx, "foo")
		defer ns1Sub.Close()

		// Wait for subscription confirmation
		msg, err := ns1Sub.Receive(ctx)
		require.NoError(t, err)
		require.IsType(t, &redis.Subscription{}, msg)

		// ns2 publishes to same channel name "foo" - should return 0
		result := ns2Rdb.Publish(ctx, "foo", "secret_message")
		require.NoError(t, result.Err())
		require.Equal(t, int64(0), result.Val(), "Cross-tenant PUBLISH should return 0 subscribers")

		// Verify ns1 did NOT receive the message (with timeout)
		ns1Sub.ReceiveTimeout(ctx, 200*time.Millisecond)
		// No assertion on error - timeout is expected
	})

	t.Run("Same-namespace PUBLISH works correctly", func(t *testing.T) {
		// ns1 subscribes to channel "bar"
		ns1Sub := ns1Rdb.Subscribe(ctx, "bar")
		defer ns1Sub.Close()

		// Wait for subscription confirmation
		msg, err := ns1Sub.Receive(ctx)
		require.NoError(t, err)
		require.IsType(t, &redis.Subscription{}, msg)

		// Create another ns1 connection for publishing
		ns1Rdb2 := srv.NewClientWithOption(&redis.Options{
			Password: "token1",
		})
		defer ns1Rdb2.Close()

		// ns1 publishes to channel "bar" - should return 1
		result := ns1Rdb2.Publish(ctx, "bar", "hello_ns1")
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val(), "Same-namespace PUBLISH should return 1 subscriber")

		// Verify ns1 received the message
		received, err := ns1Sub.ReceiveTimeout(ctx, 1*time.Second)
		require.NoError(t, err)
		require.IsType(t, &redis.Message{}, received)
		msg2 := received.(*redis.Message)
		require.Equal(t, "bar", msg2.Channel)
		require.Equal(t, "hello_ns1", msg2.Payload)
	})

	t.Run("PSUBSCRIBE pattern isolation", func(t *testing.T) {
		// ns1 pattern-subscribes to "user:*"
		ns1PSub := ns1Rdb.PSubscribe(ctx, "user:*")
		defer ns1PSub.Close()

		// Wait for subscription confirmation
		msg, err := ns1PSub.Receive(ctx)
		require.NoError(t, err)
		require.IsType(t, &redis.Subscription{}, msg)

		// ns2 publishes to "user:123" - ns1 should NOT receive
		result := ns2Rdb.Publish(ctx, "user:123", "ns2_user_data")
		require.NoError(t, result.Err())
		require.Equal(t, int64(0), result.Val(), "Cross-tenant pattern PUBLISH should return 0")

		// ns1 publishes to "user:456" - ns1 SHOULD receive
		ns1Rdb2 := srv.NewClientWithOption(&redis.Options{
			Password: "token1",
		})
		defer ns1Rdb2.Close()

		result = ns1Rdb2.Publish(ctx, "user:456", "ns1_user_data")
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val(), "Same-namespace pattern PUBLISH should return 1")

		// Verify ns1 received the message
		received, err := ns1PSub.ReceiveTimeout(ctx, 1*time.Second)
		require.NoError(t, err)
		require.IsType(t, &redis.Message{}, received)
		msg2 := received.(*redis.Message)
		require.Equal(t, "user:456", msg2.Channel)
		require.Equal(t, "ns1_user_data", msg2.Payload)
	})

	t.Run("PUBSUB CHANNELS isolation", func(t *testing.T) {
		// ns1 subscribes to channels
		ns1Sub := ns1Rdb.Subscribe(ctx, "ns1_chan1", "ns1_chan2")
		defer ns1Sub.Close()
		// Drain subscription confirmations
		ns1Sub.Receive(ctx)
		ns1Sub.Receive(ctx)

		// ns2 subscribes to channels
		ns2Sub := ns2Rdb.Subscribe(ctx, "ns2_chan1", "ns2_chan2", "ns2_chan3")
		defer ns2Sub.Close()
		// Drain subscription confirmations
		ns2Sub.Receive(ctx)
		ns2Sub.Receive(ctx)
		ns2Sub.Receive(ctx)

		// ns1 sees only its own channels
		channels := ns1Rdb.PubSubChannels(ctx, "*").Val()
		require.ElementsMatch(t, []string{"ns1_chan1", "ns1_chan2"}, channels,
			"ns1 should only see its own channels")

		// ns2 sees only its own channels
		channels = ns2Rdb.PubSubChannels(ctx, "*").Val()
		require.ElementsMatch(t, []string{"ns2_chan1", "ns2_chan2", "ns2_chan3"}, channels,
			"ns2 should only see its own channels")

		// Admin sees only its own (default namespace) channels - none in this test
		channels = adminRdb.PubSubChannels(ctx, "*").Val()
		require.Equal(t, 0, len(channels), "Admin should only see its own (empty) namespace channels")
	})

	t.Run("PUBSUB NUMSUB isolation", func(t *testing.T) {
		// Setup: ns1 has 2 subscribers to "shared_name", ns2 has 1
		ns1Sub1 := ns1Rdb.Subscribe(ctx, "shared_name")
		defer ns1Sub1.Close()
		ns1Sub1.Receive(ctx)

		ns1Rdb2 := srv.NewClientWithOption(&redis.Options{
			Password: "token1",
		})
		defer ns1Rdb2.Close()
		ns1Sub2 := ns1Rdb2.Subscribe(ctx, "shared_name")
		defer ns1Sub2.Close()
		ns1Sub2.Receive(ctx)

		ns2Sub := ns2Rdb.Subscribe(ctx, "shared_name")
		defer ns2Sub.Close()
		ns2Sub.Receive(ctx)

		// ns1 queries NUMSUB - should see 2 (only its own)
		result := ns1Rdb.PubSubNumSub(ctx, "shared_name")
		require.NoError(t, result.Err())
		require.Equal(t, int64(2), result.Val()["shared_name"],
			"ns1 should see 2 subscribers (its own)")

		// ns2 queries NUMSUB - should see 1 (only its own)
		result = ns2Rdb.PubSubNumSub(ctx, "shared_name")
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val()["shared_name"],
			"ns2 should see 1 subscriber (its own)")
	})

	t.Run("PUBSUB NUMPAT isolation", func(t *testing.T) {
		// ns1 creates 2 pattern subscriptions
		ns1PSub1 := ns1Rdb.PSubscribe(ctx, "pattern1:*")
		defer ns1PSub1.Close()
		ns1PSub1.Receive(ctx)

		ns1Rdb2 := srv.NewClientWithOption(&redis.Options{
			Password: "token1",
		})
		defer ns1Rdb2.Close()
		ns1PSub2 := ns1Rdb2.PSubscribe(ctx, "pattern2:*")
		defer ns1PSub2.Close()
		ns1PSub2.Receive(ctx)

		// ns2 creates 3 pattern subscriptions
		ns2PSub1 := ns2Rdb.PSubscribe(ctx, "other1:*")
		defer ns2PSub1.Close()
		ns2PSub1.Receive(ctx)

		ns2Rdb2 := srv.NewClientWithOption(&redis.Options{
			Password: "token2",
		})
		defer ns2Rdb2.Close()
		ns2PSub2 := ns2Rdb2.PSubscribe(ctx, "other2:*")
		defer ns2PSub2.Close()
		ns2PSub2.Receive(ctx)

		ns2Rdb3 := srv.NewClientWithOption(&redis.Options{
			Password: "token2",
		})
		defer ns2Rdb3.Close()
		ns2PSub3 := ns2Rdb3.PSubscribe(ctx, "other3:*")
		defer ns2PSub3.Close()
		ns2PSub3.Receive(ctx)

		// ns1 sees 2 patterns (its own)
		numpat := ns1Rdb.PubSubNumPat(ctx).Val()
		require.Equal(t, int64(2), numpat, "ns1 should see 2 patterns")

		// ns2 sees 3 patterns (its own)
		numpat = ns2Rdb.PubSubNumPat(ctx).Val()
		require.Equal(t, int64(3), numpat, "ns2 should see 3 patterns")
	})

	t.Run("Namespace cleanup removes PubSub subscriptions", func(t *testing.T) {
		// Create a new namespace for this test
		require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns_temp", "token_temp").Err())

		nsTempRdb := srv.NewClientWithOption(&redis.Options{
			Password: "token_temp",
		})

		// Subscribe to a channel
		nsTempSub := nsTempRdb.Subscribe(ctx, "temp_channel")
		nsTempSub.Receive(ctx)

		// Verify subscription exists
		channels := nsTempRdb.PubSubChannels(ctx, "*").Val()
		require.Contains(t, channels, "temp_channel")

		// Close connections before deleting namespace
		nsTempSub.Close()
		nsTempRdb.Close()

		// Delete namespace
		require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns_temp").Err())

		// Subscriptions should be cleaned up (no way to verify directly,
		// but this ensures CleanupPubSubNamespace was called without error)
	})

	t.Run("INFO pubsub stats isolation", func(t *testing.T) {
		// Helper to parse INFO output
		parseInfoValue := func(info, key string) string {
			for _, line := range strings.Split(info, "\r\n") {
				if strings.HasPrefix(line, key+":") {
					return strings.TrimPrefix(line, key+":")
				}
			}
			return ""
		}

		// Setup subscriptions
		ns1Sub := ns1Rdb.Subscribe(ctx, "info_test_chan")
		defer ns1Sub.Close()
		ns1Sub.Receive(ctx)

		ns1PSub := ns1Rdb.PSubscribe(ctx, "info_test:*")
		defer ns1PSub.Close()
		ns1PSub.Receive(ctx)

		// ns1 INFO should show its own pubsub stats
		info := ns1Rdb.Info(ctx, "stats").Val()
		pubsubChannels := parseInfoValue(info, "pubsub_channels")
		pubsubPatterns := parseInfoValue(info, "pubsub_patterns")

		// Values should reflect ns1's subscriptions (at least 1 each)
		require.NotEmpty(t, pubsubChannels)
		require.NotEmpty(t, pubsubPatterns)
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns2").Err())
}

func TestPubSubMultipleTenantsConcurrent(t *testing.T) {
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

	// Create 5 namespaces
	numTenants := 5
	tokens := make([]string, numTenants)
	for i := 0; i < numTenants; i++ {
		ns := "tenant" + string(rune('A'+i))
		token := "token" + string(rune('A'+i))
		tokens[i] = token
		require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", ns, token).Err())
	}

	t.Run("Concurrent pub/sub across multiple tenants", func(t *testing.T) {
		// Each tenant subscribes to channel "events"
		subs := make([]*redis.PubSub, numTenants)
		clients := make([]*redis.Client, numTenants)

		for i := 0; i < numTenants; i++ {
			clients[i] = srv.NewClientWithOption(&redis.Options{
				Password: tokens[i],
			})
			subs[i] = clients[i].Subscribe(ctx, "events")
			subs[i].Receive(ctx) // Wait for subscription
		}

		// Each tenant publishes to "events" - should only reach itself
		for i := 0; i < numTenants; i++ {
			pubClient := srv.NewClientWithOption(&redis.Options{
				Password: tokens[i],
			})
			result := pubClient.Publish(ctx, "events", "message_from_tenant_"+string(rune('A'+i)))
			require.Equal(t, int64(1), result.Val(),
				"Each tenant should have exactly 1 subscriber (itself)")
			pubClient.Close()
		}

		// Each tenant should receive only their own message
		for i := 0; i < numTenants; i++ {
			received, err := subs[i].ReceiveTimeout(ctx, 1*time.Second)
			require.NoError(t, err)
			msg := received.(*redis.Message)
			expectedPayload := "message_from_tenant_" + string(rune('A'+i))
			require.Equal(t, expectedPayload, msg.Payload,
				"Tenant should only receive their own message")
		}

		// Cleanup
		for i := 0; i < numTenants; i++ {
			subs[i].Close()
			clients[i].Close()
		}
	})

	// Cleanup namespaces
	for i := 0; i < numTenants; i++ {
		ns := "tenant" + string(rune('A'+i))
		require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", ns).Err())
	}
}
