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
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestClientListKillNamespaceIsolation(t *testing.T) {
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

	// Verify connections are working
	require.NoError(t, ns1Rdb.Ping(ctx).Err())
	require.NoError(t, ns2Rdb.Ping(ctx).Err())

	t.Run("CLIENT LIST only shows own namespace connections", func(t *testing.T) {
		// ns1 sees only its own connection
		result := ns1Rdb.ClientList(ctx)
		require.NoError(t, result.Err())
		clients := result.Val()

		// Should see exactly 1 connection (itself)
		lines := strings.Split(strings.TrimSpace(clients), "\n")
		require.Equal(t, 1, len(lines), "ns1 should see only 1 connection (itself)")

		// ns2 sees only its own connection
		result = ns2Rdb.ClientList(ctx)
		require.NoError(t, result.Err())
		clients = result.Val()

		lines = strings.Split(strings.TrimSpace(clients), "\n")
		require.Equal(t, 1, len(lines), "ns2 should see only 1 connection (itself)")
	})

	t.Run("Admin sees all namespace connections", func(t *testing.T) {
		result := adminRdb.ClientList(ctx)
		require.NoError(t, result.Err())
		clients := result.Val()

		// Admin should see at least 3 connections (admin + ns1 + ns2)
		lines := strings.Split(strings.TrimSpace(clients), "\n")
		require.GreaterOrEqual(t, len(lines), 3, "Admin should see at least 3 connections")
	})

	t.Run("CLIENT KILL cannot kill other namespace connections", func(t *testing.T) {
		// Get ns2's client ID
		ns2Id := ns2Rdb.ClientID(ctx).Val()
		require.NotZero(t, ns2Id)

		// ns1 tries to kill ns2 by ID - should fail (0 killed)
		result := ns1Rdb.Do(ctx, "CLIENT", "KILL", "ID", ns2Id)
		require.NoError(t, result.Err())
		killed := result.Val().(int64)
		require.Equal(t, int64(0), killed, "ns1 should not be able to kill ns2's connection")

		// Verify ns2 is still connected
		require.NoError(t, ns2Rdb.Ping(ctx).Err())
	})

	t.Run("CLIENT KILL can kill own namespace connection", func(t *testing.T) {
		// Create a second ns1 connection
		ns1Rdb2 := srv.NewClientWithOption(&redis.Options{
			Password: "token1",
		})
		require.NoError(t, ns1Rdb2.Ping(ctx).Err())

		// Get ns1Rdb2's client ID
		ns1Rdb2Id := ns1Rdb2.ClientID(ctx).Val()
		require.NotZero(t, ns1Rdb2Id)

		// ns1 kills ns1Rdb2 by ID - should succeed
		result := ns1Rdb.Do(ctx, "CLIENT", "KILL", "ID", ns1Rdb2Id)
		require.NoError(t, result.Err())
		killed := result.Val().(int64)
		require.Equal(t, int64(1), killed, "ns1 should be able to kill its own namespace connection")

		// CLIENT KILL is async - go-redis will auto-reconnect
		// Verify by checking that client ID changed (reconnected = new ID)
		for i := 0; i < 10; i++ {
			ns1Rdb2.Ping(ctx) // Trigger reconnect
		}
		newId := ns1Rdb2.ClientID(ctx).Val()
		require.NotEqual(t, ns1Rdb2Id, newId, "Client ID should change after kill (reconnected)")

		ns1Rdb2.Close()
	})

	t.Run("Admin can kill any namespace connection", func(t *testing.T) {
		// Create a new ns1 connection for this test
		ns1RdbToKill := srv.NewClientWithOption(&redis.Options{
			Password: "token1",
		})
		require.NoError(t, ns1RdbToKill.Ping(ctx).Err())

		// Get the client ID
		clientId := ns1RdbToKill.ClientID(ctx).Val()
		require.NotZero(t, clientId)

		// Admin kills ns1 connection by ID
		result := adminRdb.Do(ctx, "CLIENT", "KILL", "ID", clientId)
		require.NoError(t, result.Err())
		killed := result.Val().(int64)
		require.Equal(t, int64(1), killed, "Admin should be able to kill any connection")

		// CLIENT KILL is async - go-redis will auto-reconnect
		// Verify by checking that client ID changed (reconnected = new ID)
		for i := 0; i < 10; i++ {
			ns1RdbToKill.Ping(ctx) // Trigger reconnect
		}
		newId := ns1RdbToKill.ClientID(ctx).Val()
		require.NotEqual(t, clientId, newId, "Client ID should change after kill (reconnected)")

		ns1RdbToKill.Close()
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns2").Err())
}

func TestInfoNamespaceIsolation(t *testing.T) {
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

	// Create additional ns1 connections to verify counting
	ns1Rdb2 := srv.NewClientWithOption(&redis.Options{
		Password: "token1",
	})
	defer func() { require.NoError(t, ns1Rdb2.Close()) }()

	ns1Rdb3 := srv.NewClientWithOption(&redis.Options{
		Password: "token1",
	})
	defer func() { require.NoError(t, ns1Rdb3.Close()) }()

	// Verify all connections are working
	require.NoError(t, ns1Rdb.Ping(ctx).Err())
	require.NoError(t, ns1Rdb2.Ping(ctx).Err())
	require.NoError(t, ns1Rdb3.Ping(ctx).Err())
	require.NoError(t, ns2Rdb.Ping(ctx).Err())

	// Helper to parse INFO output
	parseInfoValue := func(info, key string) string {
		for _, line := range strings.Split(info, "\r\n") {
			if strings.HasPrefix(line, key+":") {
				return strings.TrimPrefix(line, key+":")
			}
		}
		return ""
	}

	t.Run("INFO clients shows only own namespace connections for tenant", func(t *testing.T) {
		// ns1 should see 3 connections (ns1Rdb, ns1Rdb2, ns1Rdb3)
		info := ns1Rdb.Info(ctx, "clients").Val()
		connectedClients := parseInfoValue(info, "connected_clients")
		require.Equal(t, "3", connectedClients, "ns1 should see 3 connections in its namespace")

		// ns2 should see only 1 connection (itself)
		info = ns2Rdb.Info(ctx, "clients").Val()
		connectedClients = parseInfoValue(info, "connected_clients")
		require.Equal(t, "1", connectedClients, "ns2 should see 1 connection in its namespace")
	})

	t.Run("Admin sees all connections in INFO clients", func(t *testing.T) {
		info := adminRdb.Info(ctx, "clients").Val()
		connectedClients := parseInfoValue(info, "connected_clients")

		// Admin should see at least 5 connections (admin + 3*ns1 + 1*ns2)
		count, err := strconv.Atoi(connectedClients)
		require.NoError(t, err)
		require.GreaterOrEqual(t, count, 5, "Admin should see at least 5 connections")
	})

	t.Run("INFO keyspace works for tenants", func(t *testing.T) {
		// Write some data to ns1
		require.NoError(t, ns1Rdb.Set(ctx, "testkey1", "value1", 0).Err())
		require.NoError(t, ns1Rdb.Set(ctx, "testkey2", "value2", 0).Err())

		// Write some data to ns2
		require.NoError(t, ns2Rdb.Set(ctx, "testkey1", "value1", 0).Err())

		// Each namespace should see its own keyspace info
		info := ns1Rdb.Info(ctx, "keyspace").Val()
		require.Contains(t, info, "db0:", "ns1 should see keyspace info")

		info = ns2Rdb.Info(ctx, "keyspace").Val()
		require.Contains(t, info, "db0:", "ns2 should see keyspace info")

		// Admin should also see keyspace info
		info = adminRdb.Info(ctx, "keyspace").Val()
		require.Contains(t, info, "db0:", "Admin should see keyspace info")
	})

	t.Run("No side effects when admin calls INFO", func(t *testing.T) {
		// Admin calling INFO should not affect tenant connections
		adminRdb.Info(ctx, "clients")
		adminRdb.Info(ctx, "keyspace")
		adminRdb.Info(ctx, "all")

		// All tenant connections should still work
		require.NoError(t, ns1Rdb.Ping(ctx).Err())
		require.NoError(t, ns1Rdb2.Ping(ctx).Err())
		require.NoError(t, ns1Rdb3.Ping(ctx).Err())
		require.NoError(t, ns2Rdb.Ping(ctx).Err())

		// Tenant INFO should still work correctly
		info := ns1Rdb.Info(ctx, "clients").Val()
		connectedClients := parseInfoValue(info, "connected_clients")
		require.Equal(t, "3", connectedClients, "ns1 should still see 3 connections")
	})

	t.Run("No side effects when tenant calls INFO", func(t *testing.T) {
		// Tenant calling INFO should not affect other tenants
		ns1Rdb.Info(ctx, "clients")
		ns1Rdb.Info(ctx, "keyspace")

		// Other tenant should still work and see correct counts
		require.NoError(t, ns2Rdb.Ping(ctx).Err())
		info := ns2Rdb.Info(ctx, "clients").Val()
		connectedClients := parseInfoValue(info, "connected_clients")
		require.Equal(t, "1", connectedClients, "ns2 should still see 1 connection")
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns2").Err())
}

func TestBlockedClientsNamespaceIsolation(t *testing.T) {
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

	// Helper to parse INFO output
	parseInfoValue := func(info, key string) string {
		for _, line := range strings.Split(info, "\r\n") {
			if strings.HasPrefix(line, key+":") {
				return strings.TrimPrefix(line, key+":")
			}
		}
		return ""
	}

	t.Run("BLPOP blocking shows in own namespace only", func(t *testing.T) {
		// Create a separate connection for BLPOP (will block)
		ns1BlockingRdb := srv.NewClientWithOption(&redis.Options{
			Password: "token1",
		})
		defer ns1BlockingRdb.Close()

		// Start BLPOP in background (will block for 3 seconds)
		done := make(chan struct{})
		go func() {
			ns1BlockingRdb.BLPop(ctx, 3*time.Second, "nonexistent_key_for_block_test")
			close(done)
		}()

		// Give BLPOP time to start blocking
		time.Sleep(200 * time.Millisecond)

		// ns1 should see 1 blocked client
		info := ns1Rdb.Info(ctx, "clients").Val()
		blockedClients := parseInfoValue(info, "blocked_clients")
		require.Equal(t, "1", blockedClients, "ns1 should see 1 blocked client")

		// ns2 should see 0 blocked clients
		info = ns2Rdb.Info(ctx, "clients").Val()
		blockedClients = parseInfoValue(info, "blocked_clients")
		require.Equal(t, "0", blockedClients, "ns2 should see 0 blocked clients")

		// Admin should see 1 blocked client (global)
		info = adminRdb.Info(ctx, "clients").Val()
		blockedClients = parseInfoValue(info, "blocked_clients")
		require.Equal(t, "1", blockedClients, "Admin should see 1 blocked client (global)")

		// Wait for BLPOP to timeout
		<-done
	})

	t.Run("Multiple blocked clients in different namespaces", func(t *testing.T) {
		// Create blocking connections for both namespaces
		ns1BlockingRdb := srv.NewClientWithOption(&redis.Options{
			Password: "token1",
		})
		defer ns1BlockingRdb.Close()

		ns2BlockingRdb := srv.NewClientWithOption(&redis.Options{
			Password: "token2",
		})
		defer ns2BlockingRdb.Close()

		// Start BLPOP in both namespaces
		done1 := make(chan struct{})
		done2 := make(chan struct{})

		go func() {
			ns1BlockingRdb.BLPop(ctx, 3*time.Second, "ns1_block_key")
			close(done1)
		}()

		go func() {
			ns2BlockingRdb.BLPop(ctx, 3*time.Second, "ns2_block_key")
			close(done2)
		}()

		// Give both BLPOP time to start blocking
		time.Sleep(200 * time.Millisecond)

		// ns1 should see 1 blocked client (only its own)
		info := ns1Rdb.Info(ctx, "clients").Val()
		blockedClients := parseInfoValue(info, "blocked_clients")
		require.Equal(t, "1", blockedClients, "ns1 should see 1 blocked client")

		// ns2 should see 1 blocked client (only its own)
		info = ns2Rdb.Info(ctx, "clients").Val()
		blockedClients = parseInfoValue(info, "blocked_clients")
		require.Equal(t, "1", blockedClients, "ns2 should see 1 blocked client")

		// Admin should see 2 blocked clients (global total)
		info = adminRdb.Info(ctx, "clients").Val()
		blockedClients = parseInfoValue(info, "blocked_clients")
		require.Equal(t, "2", blockedClients, "Admin should see 2 blocked clients (global)")

		// Wait for both to timeout
		<-done1
		<-done2
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns2").Err())
}
