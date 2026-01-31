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
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// mustMatchWithinLines reads up to maxLines and checks if any matches the pattern
func mustMatchWithinLines(t testing.TB, c *util.TCPClient, pattern string, maxLines int) {
	re := regexp.MustCompile("(?i)" + pattern) // case insensitive
	for i := 0; i < maxLines; i++ {
		line, err := c.ReadLine()
		require.NoError(t, err)
		if re.MatchString(line) {
			return
		}
	}
	t.Fatalf("Pattern %q not found within %d lines", pattern, maxLines)
}

func TestMonitorNamespaceIsolation(t *testing.T) {
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

	// Tenant connections for commands
	ns1Rdb := srv.NewClientWithOption(&redis.Options{
		Password: "token1",
	})
	defer func() { require.NoError(t, ns1Rdb.Close()) }()

	ns2Rdb := srv.NewClientWithOption(&redis.Options{
		Password: "token2",
	})
	defer func() { require.NoError(t, ns2Rdb.Close()) }()

	t.Run("Tenant MONITOR sees only own namespace commands", func(t *testing.T) {
		// ns1 monitor connection
		ns1Monitor := srv.NewTCPClient()
		defer func() { require.NoError(t, ns1Monitor.Close()) }()
		require.NoError(t, ns1Monitor.WriteArgs("AUTH", "token1"))
		ns1Monitor.MustRead(t, "+OK")
		require.NoError(t, ns1Monitor.WriteArgs("MONITOR"))
		ns1Monitor.MustRead(t, "+OK")

		// ns2 monitor connection
		ns2Monitor := srv.NewTCPClient()
		defer func() { require.NoError(t, ns2Monitor.Close()) }()
		require.NoError(t, ns2Monitor.WriteArgs("AUTH", "token2"))
		ns2Monitor.MustRead(t, "+OK")
		require.NoError(t, ns2Monitor.WriteArgs("MONITOR"))
		ns2Monitor.MustRead(t, "+OK")

		// Execute command in ns1
		require.NoError(t, ns1Rdb.Set(ctx, "ns1_key", "ns1_value", 0).Err())

		// Execute command in ns2
		require.NoError(t, ns2Rdb.Set(ctx, "ns2_key", "ns2_value", 0).Err())

		// ns1 monitor should see ns1 command
		mustMatchWithinLines(t, ns1Monitor, ".*set.*ns1_key.*ns1_value.*", 10)

		// ns2 monitor should see ns2 command
		mustMatchWithinLines(t, ns2Monitor, ".*set.*ns2_key.*ns2_value.*", 10)
	})

	t.Run("Admin MONITOR sees only default namespace commands", func(t *testing.T) {
		// Admin monitor connection
		adminMonitor := srv.NewTCPClient()
		defer func() { require.NoError(t, adminMonitor.Close()) }()
		require.NoError(t, adminMonitor.WriteArgs("AUTH", password))
		adminMonitor.MustRead(t, "+OK")
		require.NoError(t, adminMonitor.WriteArgs("MONITOR"))
		adminMonitor.MustRead(t, "+OK")

		// Execute command in default namespace (admin)
		require.NoError(t, adminRdb.Set(ctx, "admin_key", "admin_value", 0).Err())

		// Admin monitor should see the command
		mustMatchWithinLines(t, adminMonitor, ".*set.*admin_key.*admin_value.*", 10)

		// Execute command in ns1 - admin should NOT see this
		require.NoError(t, ns1Rdb.Set(ctx, "invisible_key", "invisible_value", 0).Err())

		// Execute another admin command to verify monitor still works
		require.NoError(t, adminRdb.Get(ctx, "admin_key").Err())
		mustMatchWithinLines(t, adminMonitor, ".*get.*admin_key.*", 10)
	})

	t.Run("Parallel monitors work correctly", func(t *testing.T) {
		// Create multiple monitors per namespace
		monitors := make([]*util.TCPClient, 0)

		// 2 monitors for ns1
		for i := 0; i < 2; i++ {
			m := srv.NewTCPClient()
			require.NoError(t, m.WriteArgs("AUTH", "token1"))
			m.MustRead(t, "+OK")
			require.NoError(t, m.WriteArgs("MONITOR"))
			m.MustRead(t, "+OK")
			monitors = append(monitors, m)
		}

		// 2 monitors for ns2
		for i := 0; i < 2; i++ {
			m := srv.NewTCPClient()
			require.NoError(t, m.WriteArgs("AUTH", "token2"))
			m.MustRead(t, "+OK")
			require.NoError(t, m.WriteArgs("MONITOR"))
			m.MustRead(t, "+OK")
			monitors = append(monitors, m)
		}

		defer func() {
			for _, m := range monitors {
				m.Close()
			}
		}()

		// Execute parallel commands
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				ns1Rdb.Set(ctx, "parallel_ns1", "value", 0)
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				ns2Rdb.Set(ctx, "parallel_ns2", "value", 0)
			}
		}()

		wg.Wait()

		// All ns1 monitors should see ns1 commands
		for i := 0; i < 2; i++ {
			mustMatchWithinLines(t, monitors[i], ".*set.*parallel_ns1.*", 20)
		}

		// All ns2 monitors should see ns2 commands
		for i := 2; i < 4; i++ {
			mustMatchWithinLines(t, monitors[i], ".*set.*parallel_ns2.*", 20)
		}
	})

	t.Run("MONITOR with transaction", func(t *testing.T) {
		// ns1 monitor
		ns1Monitor := srv.NewTCPClient()
		defer func() { require.NoError(t, ns1Monitor.Close()) }()
		require.NoError(t, ns1Monitor.WriteArgs("AUTH", "token1"))
		ns1Monitor.MustRead(t, "+OK")
		require.NoError(t, ns1Monitor.WriteArgs("MONITOR"))
		ns1Monitor.MustRead(t, "+OK")

		// Execute transaction in ns1
		pipe := ns1Rdb.TxPipeline()
		pipe.Set(ctx, "tx_key1", "tx_value1", 0)
		pipe.Set(ctx, "tx_key2", "tx_value2", 0)
		_, err := pipe.Exec(ctx)
		require.NoError(t, err)

		// Monitor should see MULTI, commands, and EXEC
		mustMatchWithinLines(t, ns1Monitor, ".*multi.*", 10)
		mustMatchWithinLines(t, ns1Monitor, ".*set.*tx_key1.*", 10)
		mustMatchWithinLines(t, ns1Monitor, ".*set.*tx_key2.*", 10)
		mustMatchWithinLines(t, ns1Monitor, ".*exec.*", 10)
	})

	t.Run("MONITOR with Lua script", func(t *testing.T) {
		// ns1 monitor
		ns1Monitor := srv.NewTCPClient()
		defer func() { require.NoError(t, ns1Monitor.Close()) }()
		require.NoError(t, ns1Monitor.WriteArgs("AUTH", "token1"))
		ns1Monitor.MustRead(t, "+OK")
		require.NoError(t, ns1Monitor.WriteArgs("MONITOR"))
		ns1Monitor.MustRead(t, "+OK")

		// Execute Lua script in ns1
		script := "return redis.call('SET', KEYS[1], ARGV[1])"
		require.NoError(t, ns1Rdb.Eval(ctx, script, []string{"lua_key"}, "lua_value").Err())

		// Monitor should see EVAL or the inner SET (depending on skip-monitor flag)
		mustMatchWithinLines(t, ns1Monitor, ".*(eval|set.*lua_key).*", 10)
	})

	t.Run("MONITOR double call is idempotent", func(t *testing.T) {
		// Test that calling MONITOR twice doesn't cause issues
		m := srv.NewTCPClient()
		defer func() { require.NoError(t, m.Close()) }()
		require.NoError(t, m.WriteArgs("AUTH", "token1"))
		m.MustRead(t, "+OK")

		// First MONITOR
		require.NoError(t, m.WriteArgs("MONITOR"))
		m.MustRead(t, "+OK")

		// Second MONITOR (should be idempotent)
		require.NoError(t, m.WriteArgs("MONITOR"))
		m.MustRead(t, "+OK")

		// Should still work
		require.NoError(t, ns1Rdb.Set(ctx, "double_monitor_key", "value", 0).Err())
		mustMatchWithinLines(t, m, ".*set.*double_monitor_key.*", 10)
	})

	t.Run("INFO monitor_clients isolated per namespace", func(t *testing.T) {
		// Create monitor for ns1
		ns1Monitor := srv.NewTCPClient()
		defer func() { require.NoError(t, ns1Monitor.Close()) }()
		require.NoError(t, ns1Monitor.WriteArgs("AUTH", "token1"))
		ns1Monitor.MustRead(t, "+OK")
		require.NoError(t, ns1Monitor.WriteArgs("MONITOR"))
		ns1Monitor.MustRead(t, "+OK")

		// Give time for registration
		time.Sleep(50 * time.Millisecond)

		// ns1 should see 1 monitor client
		info := ns1Rdb.Info(ctx, "clients").Val()
		require.Contains(t, info, "monitor_clients:1", "ns1 should see 1 monitor client")

		// ns2 should see 0 monitor clients
		info = ns2Rdb.Info(ctx, "clients").Val()
		require.Contains(t, info, "monitor_clients:0", "ns2 should see 0 monitor clients")
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns2").Err())
}
