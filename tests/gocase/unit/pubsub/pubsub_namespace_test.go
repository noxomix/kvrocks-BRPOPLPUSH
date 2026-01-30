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

package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestPubSubNamespaceIsolation(t *testing.T) {
	password := "pwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()
	adminRdb := srv.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns2", "token2").Err())

	ns1Rdb := srv.NewClientWithOption(&redis.Options{Password: "token1"})
	ns2Rdb := srv.NewClientWithOption(&redis.Options{Password: "token2"})
	defer func() { require.NoError(t, ns1Rdb.Close()) }()
	defer func() { require.NoError(t, ns2Rdb.Close()) }()

	t.Run("PUBLISH only reaches same namespace", func(t *testing.T) {
		pubsub := ns1Rdb.Subscribe(ctx, "mychannel")
		defer pubsub.Close()
		receiveType(t, pubsub, &redis.Subscription{})

		// ns2 publishes - ns1 should NOT receive
		receivers := ns2Rdb.Publish(ctx, "mychannel", "from-ns2").Val()
		require.EqualValues(t, 0, receivers) // No receivers in ns2

		// ns1 publishes - ns1 should receive
		receivers = ns1Rdb.Publish(ctx, "mychannel", "from-ns1").Val()
		require.EqualValues(t, 1, receivers)

		msg := receiveType(t, pubsub, &redis.Message{})
		require.Equal(t, "mychannel", msg.Channel)
		require.Equal(t, "from-ns1", msg.Payload)
	})

	t.Run("PSUBSCRIBE only matches same namespace", func(t *testing.T) {
		pubsub := ns1Rdb.PSubscribe(ctx, "foo*")
		defer pubsub.Close()
		receiveType(t, pubsub, &redis.Subscription{})

		// ns2 publishes to matching pattern - ns1 should NOT receive
		receivers := ns2Rdb.Publish(ctx, "foobar", "from-ns2").Val()
		require.EqualValues(t, 0, receivers)

		// ns1 publishes - ns1 should receive
		receivers = ns1Rdb.Publish(ctx, "foobar", "from-ns1").Val()
		require.EqualValues(t, 1, receivers)

		msg := receiveType(t, pubsub, &redis.Message{})
		require.Equal(t, "foobar", msg.Channel)
		require.Equal(t, "from-ns1", msg.Payload)
	})

	t.Run("PUBSUB CHANNELS only shows own namespace", func(t *testing.T) {
		pubsub1 := ns1Rdb.Subscribe(ctx, "ns1-channel")
		pubsub2 := ns2Rdb.Subscribe(ctx, "ns2-channel")
		defer pubsub1.Close()
		defer pubsub2.Close()
		receiveType(t, pubsub1, &redis.Subscription{})
		receiveType(t, pubsub2, &redis.Subscription{})

		// Give subscriptions a moment to register
		time.Sleep(50 * time.Millisecond)

		// ns1 should only see ns1-channel
		channels := ns1Rdb.PubSubChannels(ctx, "*").Val()
		require.Contains(t, channels, "ns1-channel")
		require.NotContains(t, channels, "ns2-channel")

		// ns2 should only see ns2-channel
		channels = ns2Rdb.PubSubChannels(ctx, "*").Val()
		require.Contains(t, channels, "ns2-channel")
		require.NotContains(t, channels, "ns1-channel")
	})

	t.Run("PUBSUB NUMSUB only counts own namespace", func(t *testing.T) {
		pubsub1 := ns1Rdb.Subscribe(ctx, "shared-name")
		pubsub2 := ns2Rdb.Subscribe(ctx, "shared-name")
		defer pubsub1.Close()
		defer pubsub2.Close()
		receiveType(t, pubsub1, &redis.Subscription{})
		receiveType(t, pubsub2, &redis.Subscription{})

		// Give subscriptions a moment to register
		time.Sleep(50 * time.Millisecond)

		// Each namespace should see count of 1 for their own "shared-name"
		numsub := ns1Rdb.PubSubNumSub(ctx, "shared-name").Val()
		require.EqualValues(t, 1, numsub["shared-name"])

		numsub = ns2Rdb.PubSubNumSub(ctx, "shared-name").Val()
		require.EqualValues(t, 1, numsub["shared-name"])
	})

	t.Run("PUBSUB NUMPAT only counts own namespace patterns", func(t *testing.T) {
		pubsub1 := ns1Rdb.PSubscribe(ctx, "pattern1*", "pattern2*")
		pubsub2 := ns2Rdb.PSubscribe(ctx, "other*")
		defer pubsub1.Close()
		defer pubsub2.Close()
		receiveType(t, pubsub1, &redis.Subscription{})
		receiveType(t, pubsub1, &redis.Subscription{})
		receiveType(t, pubsub2, &redis.Subscription{})

		// Give subscriptions a moment to register
		time.Sleep(50 * time.Millisecond)

		// ns1 should see 2 patterns (pattern1*, pattern2*)
		numpat := ns1Rdb.PubSubNumPat(ctx).Val()
		require.EqualValues(t, 2, numpat)

		// ns2 should see 1 pattern (other*)
		numpat = ns2Rdb.PubSubNumPat(ctx).Val()
		require.EqualValues(t, 1, numpat)
	})

	t.Run("Same channel name in different namespaces are independent", func(t *testing.T) {
		// Both namespaces subscribe to the same channel name
		pubsub1 := ns1Rdb.Subscribe(ctx, "common")
		pubsub2 := ns2Rdb.Subscribe(ctx, "common")
		defer pubsub1.Close()
		defer pubsub2.Close()
		receiveType(t, pubsub1, &redis.Subscription{})
		receiveType(t, pubsub2, &redis.Subscription{})

		// ns1 publishes
		receivers := ns1Rdb.Publish(ctx, "common", "msg-from-ns1").Val()
		require.EqualValues(t, 1, receivers) // Only ns1 subscriber

		// ns2 publishes
		receivers = ns2Rdb.Publish(ctx, "common", "msg-from-ns2").Val()
		require.EqualValues(t, 1, receivers) // Only ns2 subscriber

		// ns1 receives only ns1's message
		msg1 := receiveType(t, pubsub1, &redis.Message{})
		require.Equal(t, "msg-from-ns1", msg1.Payload)

		// ns2 receives only ns2's message
		msg2 := receiveType(t, pubsub2, &redis.Message{})
		require.Equal(t, "msg-from-ns2", msg2.Payload)
	})
}
