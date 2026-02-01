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

package server

import (
	"context"
	"regexp"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

// parseInfoStats extracts stats values from INFO output
func parseInfoStats(info string, key string) (int64, bool) {
	re := regexp.MustCompile(key + `:(\d+)`)
	matches := re.FindStringSubmatch(info)
	if len(matches) < 2 {
		return 0, false
	}
	val, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func TestInfoStatsNamespaceIsolation(t *testing.T) {
	password := "adminpwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin client (default namespace)
	adminRdb := srv.NewClientWithOption(&redis.Options{
		Password: password,
	})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns2", "token2").Err())

	// Clients for different namespaces
	ns1Rdb := srv.NewClientWithOption(&redis.Options{Password: "token1"})
	ns2Rdb := srv.NewClientWithOption(&redis.Options{Password: "token2"})
	defer func() { require.NoError(t, ns1Rdb.Close()) }()
	defer func() { require.NoError(t, ns2Rdb.Close()) }()

	t.Run("Stats are isolated between namespaces", func(t *testing.T) {
		// Get initial stats for ns1
		info1Before, err := ns1Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmdsBefore1, ok := parseInfoStats(info1Before, "total_commands_processed")
		require.True(t, ok, "should parse total_commands_processed")

		// Get initial stats for ns2
		info2Before, err := ns2Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmdsBefore2, ok := parseInfoStats(info2Before, "total_commands_processed")
		require.True(t, ok, "should parse total_commands_processed")

		// Execute 100 commands on ns1 only
		for i := 0; i < 100; i++ {
			require.NoError(t, ns1Rdb.Set(ctx, "key1", "value1", 0).Err())
		}

		// Get stats after ns1 commands
		info1After, err := ns1Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmdsAfter1, ok := parseInfoStats(info1After, "total_commands_processed")
		require.True(t, ok)

		info2After, err := ns2Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmdsAfter2, ok := parseInfoStats(info2After, "total_commands_processed")
		require.True(t, ok)

		// ns1 should see increased commands (100 SET + 2 INFO)
		ns1Increase := cmdsAfter1 - cmdsBefore1
		require.GreaterOrEqual(t, ns1Increase, int64(100), "ns1 should see at least 100 new commands")

		// ns2 should see minimal increase (only its own INFO commands)
		ns2Increase := cmdsAfter2 - cmdsBefore2
		require.Less(t, ns2Increase, int64(10), "ns2 should NOT see ns1's commands (got %d increase)", ns2Increase)
	})

	t.Run("Bytes are isolated between namespaces", func(t *testing.T) {
		// Get initial byte counts
		info1Before, err := ns1Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		inBytesBefore1, ok := parseInfoStats(info1Before, "total_net_input_bytes")
		require.True(t, ok)
		outBytesBefore1, ok := parseInfoStats(info1Before, "total_net_output_bytes")
		require.True(t, ok)

		info2Before, err := ns2Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		inBytesBefore2, ok := parseInfoStats(info2Before, "total_net_input_bytes")
		require.True(t, ok)
		outBytesBefore2, ok := parseInfoStats(info2Before, "total_net_output_bytes")
		require.True(t, ok)

		// Execute commands with large payloads on ns1 only
		largeValue := string(make([]byte, 10000)) // 10KB value
		for i := 0; i < 50; i++ {
			require.NoError(t, ns1Rdb.Set(ctx, "largekey", largeValue, 0).Err())
		}

		// Get stats after ns1 commands
		info1After, err := ns1Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		inBytesAfter1, ok := parseInfoStats(info1After, "total_net_input_bytes")
		require.True(t, ok)
		outBytesAfter1, ok := parseInfoStats(info1After, "total_net_output_bytes")
		require.True(t, ok)

		info2After, err := ns2Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		inBytesAfter2, ok := parseInfoStats(info2After, "total_net_input_bytes")
		require.True(t, ok)
		outBytesAfter2, ok := parseInfoStats(info2After, "total_net_output_bytes")
		require.True(t, ok)

		// ns1 should see significant byte increase (50 * 10KB = 500KB+)
		ns1InIncrease := inBytesAfter1 - inBytesBefore1
		require.Greater(t, ns1InIncrease, int64(400000), "ns1 should see >400KB input bytes increase")

		// ns2 should see minimal increase (only INFO command overhead)
		ns2InIncrease := inBytesAfter2 - inBytesBefore2
		require.Less(t, ns2InIncrease, int64(10000), "ns2 should NOT see ns1's bytes (got %d increase)", ns2InIncrease)

		// Output bytes should also be isolated (ns1 gets OK responses)
		ns1OutIncrease := outBytesAfter1 - outBytesBefore1
		ns2OutIncrease := outBytesAfter2 - outBytesBefore2
		require.Greater(t, ns1OutIncrease, ns2OutIncrease, "ns1 output bytes should increase more than ns2")
	})

	t.Run("Admin sees only own namespace stats", func(t *testing.T) {
		// Admin is in default namespace, sees only default namespace stats (not global)
		// This is consistent with: "Admin = normaler Tenant (default namespace)" for data stats

		// Get admin stats baseline
		adminInfo, err := adminRdb.Info(ctx, "stats").Result()
		require.NoError(t, err)

		adminCmds, ok := parseInfoStats(adminInfo, "total_commands_processed")
		require.True(t, ok)

		// Execute commands on other namespaces (ns1, ns2)
		for i := 0; i < 50; i++ {
			require.NoError(t, ns1Rdb.Ping(ctx).Err())
			require.NoError(t, ns2Rdb.Ping(ctx).Err())
		}

		// Get admin stats after - should NOT include ns1/ns2 commands
		adminInfoAfter, err := adminRdb.Info(ctx, "stats").Result()
		require.NoError(t, err)

		adminCmdsAfter, ok := parseInfoStats(adminInfoAfter, "total_commands_processed")
		require.True(t, ok)

		// Admin should see only minimal increase (just the INFO commands, not ns1/ns2 PINGs)
		adminCmdIncrease := adminCmdsAfter - adminCmds
		require.Less(t, adminCmdIncrease, int64(10), "admin should NOT see commands from other namespaces")
	})

	t.Run("Each namespace only sees own traffic", func(t *testing.T) {
		// Reset by getting fresh baselines
		info1, err := ns1Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		baseline1, _ := parseInfoStats(info1, "total_commands_processed")

		info2, err := ns2Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		baseline2, _ := parseInfoStats(info2, "total_commands_processed")

		// ns1: execute 200 commands
		for i := 0; i < 200; i++ {
			require.NoError(t, ns1Rdb.Ping(ctx).Err())
		}

		// ns2: execute 50 commands
		for i := 0; i < 50; i++ {
			require.NoError(t, ns2Rdb.Ping(ctx).Err())
		}

		// Check ns1 stats
		info1After, err := ns1Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmds1, _ := parseInfoStats(info1After, "total_commands_processed")
		ns1Delta := cmds1 - baseline1

		// Check ns2 stats
		info2After, err := ns2Rdb.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmds2, _ := parseInfoStats(info2After, "total_commands_processed")
		ns2Delta := cmds2 - baseline2

		// ns1 should see ~200 commands (200 PING + 2 INFO)
		require.GreaterOrEqual(t, ns1Delta, int64(200), "ns1 should see ~200 commands")
		require.Less(t, ns1Delta, int64(220), "ns1 should not see ns2's commands")

		// ns2 should see ~50 commands (50 PING + 2 INFO)
		require.GreaterOrEqual(t, ns2Delta, int64(50), "ns2 should see ~50 commands")
		require.Less(t, ns2Delta, int64(70), "ns2 should not see ns1's commands")
	})

	// Cleanup
	require.NoError(t, ns1Rdb.Del(ctx, "key1", "largekey").Err())
}

func TestInfoStatsNamespaceSwitch(t *testing.T) {
	password := "adminpwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin client
	adminRdb := srv.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "switch_ns1", "switchtoken1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "switch_ns2", "switchtoken2").Err())

	t.Run("Stats follow namespace after AUTH switch", func(t *testing.T) {
		// Create a client that will switch namespaces
		switchClient := srv.NewClientWithOption(&redis.Options{Password: "switchtoken1"})
		defer func() { require.NoError(t, switchClient.Close()) }()

		// Verify we're in ns1 (NAMESPACE CURRENT is not admin-only)
		ns, err := switchClient.Do(ctx, "NAMESPACE", "CURRENT").Result()
		require.NoError(t, err)
		require.Equal(t, "switch_ns1", ns)

		// Get baseline for ns1
		info1Before, err := switchClient.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmds1Before, _ := parseInfoStats(info1Before, "total_commands_processed")

		// Execute 50 commands as ns1
		for i := 0; i < 50; i++ {
			require.NoError(t, switchClient.Ping(ctx).Err())
		}

		// Check ns1 stats increased
		info1After, err := switchClient.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmds1After, _ := parseInfoStats(info1After, "total_commands_processed")
		ns1Increase := cmds1After - cmds1Before
		require.GreaterOrEqual(t, ns1Increase, int64(50), "ns1 should see 50+ commands")

		// Now switch to ns2 via AUTH
		result := switchClient.Do(ctx, "AUTH", "switchtoken2")
		require.NoError(t, result.Err())

		// Verify we're now in ns2
		ns, err = switchClient.Do(ctx, "NAMESPACE", "CURRENT").Result()
		require.NoError(t, err)
		require.Equal(t, "switch_ns2", ns)

		// Get baseline for ns2
		info2Before, err := switchClient.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmds2Before, _ := parseInfoStats(info2Before, "total_commands_processed")

		// Execute 30 commands as ns2
		for i := 0; i < 30; i++ {
			require.NoError(t, switchClient.Ping(ctx).Err())
		}

		// Check ns2 stats increased
		info2After, err := switchClient.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmds2After, _ := parseInfoStats(info2After, "total_commands_processed")
		ns2Increase := cmds2After - cmds2Before
		require.GreaterOrEqual(t, ns2Increase, int64(30), "ns2 should see 30+ commands after switch")

		// Switch back to ns1 and verify its stats didn't change much
		result = switchClient.Do(ctx, "AUTH", "switchtoken1")
		require.NoError(t, result.Err())

		info1Final, err := switchClient.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmds1Final, _ := parseInfoStats(info1Final, "total_commands_processed")

		// ns1 should only see the AUTH + INFO commands, not the 30 PINGs done as ns2
		ns1FinalIncrease := cmds1Final - cmds1After
		require.Less(t, ns1FinalIncrease, int64(10), "ns1 should NOT see ns2's 30 commands (got %d increase)", ns1FinalIncrease)
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "switch_ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "switch_ns2").Err())
}

func TestInfoStatsPreAuthTraffic(t *testing.T) {
	password := "adminpwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin client for setup
	adminRdb := srv.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespace
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "preauth_ns", "preauthtoken").Err())

	t.Run("Pre-AUTH traffic not counted in namespace stats", func(t *testing.T) {
		// Get baseline for namespace (using a properly authenticated client)
		nsClient := srv.NewClientWithOption(&redis.Options{Password: "preauthtoken"})
		defer func() { require.NoError(t, nsClient.Close()) }()

		infoBefore, err := nsClient.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmdsBefore, _ := parseInfoStats(infoBefore, "total_commands_processed")
		bytesBefore, _ := parseInfoStats(infoBefore, "total_net_input_bytes")

		// Create a client WITHOUT password - it will fail commands but send traffic
		unauthClient := srv.NewClientWithOption(&redis.Options{})

		// Try to execute commands without auth - they should fail
		for i := 0; i < 20; i++ {
			// These will fail with "Authentication required" but still send bytes
			_ = unauthClient.Ping(ctx).Err()
		}
		unauthClient.Close()

		// Check namespace stats - should NOT have increased significantly
		infoAfter, err := nsClient.Info(ctx, "stats").Result()
		require.NoError(t, err)
		cmdsAfter, _ := parseInfoStats(infoAfter, "total_commands_processed")
		bytesAfter, _ := parseInfoStats(infoAfter, "total_net_input_bytes")

		// The unauthenticated traffic should NOT appear in namespace stats
		// Only the INFO commands from nsClient should appear
		cmdIncrease := cmdsAfter - cmdsBefore
		bytesIncrease := bytesAfter - bytesBefore

		// Should see only ~2 INFO commands worth, not 20 failed PINGs
		require.Less(t, cmdIncrease, int64(10), "pre-auth commands should NOT be counted in namespace (got %d)", cmdIncrease)
		require.Less(t, bytesIncrease, int64(1000), "pre-auth bytes should NOT be counted in namespace (got %d)", bytesIncrease)
	})

	// Cleanup
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "preauth_ns").Err())
}
