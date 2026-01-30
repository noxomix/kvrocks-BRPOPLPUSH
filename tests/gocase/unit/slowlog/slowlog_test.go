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

package slowlog

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSlowlogNamespaceIsolation(t *testing.T) {
	password := "adminpwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass":             password,
		"slowlog-log-slower-than": "0", // Log all commands
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin client
	adminRdb := srv.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns2", "token2").Err())

	// Namespace clients
	ns1Rdb := srv.NewClientWithOption(&redis.Options{Password: "token1"})
	ns2Rdb := srv.NewClientWithOption(&redis.Options{Password: "token2"})
	defer func() { require.NoError(t, ns1Rdb.Close()) }()
	defer func() { require.NoError(t, ns2Rdb.Close()) }()

	// Reset slowlog
	require.NoError(t, adminRdb.Do(ctx, "SLOWLOG", "RESET").Err())

	t.Run("Tenants only see their own slowlog entries", func(t *testing.T) {
		// ns1: Execute some commands
		require.NoError(t, ns1Rdb.Set(ctx, "ns1_key", "ns1_value", 0).Err())
		require.NoError(t, ns1Rdb.Get(ctx, "ns1_key").Err())

		// ns2: Execute some commands
		require.NoError(t, ns2Rdb.Set(ctx, "ns2_key", "ns2_value", 0).Err())
		require.NoError(t, ns2Rdb.Get(ctx, "ns2_key").Err())

		// ns1: SLOWLOG GET should only show ns1 commands
		ns1Log := ns1Rdb.SlowLogGet(ctx, -1).Val()
		for _, entry := range ns1Log {
			// Should not see ns2 keys
			for _, arg := range entry.Args {
				require.NotContains(t, arg, "ns2_key", "ns1 should not see ns2 commands")
			}
		}

		// ns2: SLOWLOG GET should only show ns2 commands
		ns2Log := ns2Rdb.SlowLogGet(ctx, -1).Val()
		for _, entry := range ns2Log {
			// Should not see ns1 keys
			for _, arg := range entry.Args {
				require.NotContains(t, arg, "ns1_key", "ns2 should not see ns1 commands")
			}
		}

		// Verify ns1 actually has some entries with ns1_key
		foundNs1Key := false
		for _, entry := range ns1Log {
			for _, arg := range entry.Args {
				if strings.Contains(arg, "ns1_key") {
					foundNs1Key = true
					break
				}
			}
		}
		require.True(t, foundNs1Key, "ns1 should see its own ns1_key commands")

		// Verify ns2 actually has some entries with ns2_key
		foundNs2Key := false
		for _, entry := range ns2Log {
			for _, arg := range entry.Args {
				if strings.Contains(arg, "ns2_key") {
					foundNs2Key = true
					break
				}
			}
		}
		require.True(t, foundNs2Key, "ns2 should see its own ns2_key commands")
	})

	t.Run("Admin sees all slowlog entries", func(t *testing.T) {
		adminLog := adminRdb.SlowLogGet(ctx, -1).Val()

		// Admin should see both ns1 and ns2 commands
		foundNs1 := false
		foundNs2 := false
		for _, entry := range adminLog {
			for _, arg := range entry.Args {
				if strings.Contains(arg, "ns1_key") {
					foundNs1 = true
				}
				if strings.Contains(arg, "ns2_key") {
					foundNs2 = true
				}
			}
		}
		require.True(t, foundNs1, "Admin should see ns1 commands")
		require.True(t, foundNs2, "Admin should see ns2 commands")
	})

	t.Run("SLOWLOG LEN is namespace-aware", func(t *testing.T) {
		ns1Len := ns1Rdb.Do(ctx, "SLOWLOG", "LEN").Val().(int64)
		ns2Len := ns2Rdb.Do(ctx, "SLOWLOG", "LEN").Val().(int64)
		adminLen := adminRdb.Do(ctx, "SLOWLOG", "LEN").Val().(int64)

		// Admin should see more entries than each individual tenant
		require.GreaterOrEqual(t, adminLen, ns1Len, "Admin should see >= ns1 entries")
		require.GreaterOrEqual(t, adminLen, ns2Len, "Admin should see >= ns2 entries")
	})

	t.Run("SLOWLOG RESET only clears own namespace for tenant", func(t *testing.T) {
		// ns1 resets - should only clear ns1 entries
		require.NoError(t, ns1Rdb.Do(ctx, "SLOWLOG", "RESET").Err())

		// ns1 should have 0 entries (plus maybe the RESET command itself)
		ns1LenAfter := ns1Rdb.Do(ctx, "SLOWLOG", "LEN").Val().(int64)
		require.LessOrEqual(t, ns1LenAfter, int64(1), "ns1 should have <= 1 entries after reset")

		// ns2 should still have entries - ns1's reset should not affect ns2
		// Note: Each SLOWLOG command also gets logged, so we just check ns2 has entries
		ns2LenAfter := ns2Rdb.Do(ctx, "SLOWLOG", "LEN").Val().(int64)
		require.Greater(t, ns2LenAfter, int64(0), "ns2 should still have entries after ns1 reset")
	})

	t.Run("Admin SLOWLOG RESET clears all entries", func(t *testing.T) {
		// Create some entries first
		require.NoError(t, ns1Rdb.Set(ctx, "test1", "val1", 0).Err())
		require.NoError(t, ns2Rdb.Set(ctx, "test2", "val2", 0).Err())

		// Admin resets
		require.NoError(t, adminRdb.Do(ctx, "SLOWLOG", "RESET").Err())

		// All namespaces should have 0 entries (plus maybe the RESET command for admin)
		ns1Len := ns1Rdb.Do(ctx, "SLOWLOG", "LEN").Val().(int64)
		ns2Len := ns2Rdb.Do(ctx, "SLOWLOG", "LEN").Val().(int64)

		require.Equal(t, int64(0), ns1Len, "ns1 should have 0 entries after admin reset")
		require.Equal(t, int64(0), ns2Len, "ns2 should have 0 entries after admin reset")
	})
}

func TestSlowlog(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"slowlog-log-slower-than": "1000000",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SLOWLOG - check that it starts with an empty log", func(t *testing.T) {
		require.EqualValues(t, 0, len(rdb.SlowLogGet(ctx, -1).Val()))
	})

	t.Run("SLOWLOG - only logs commands taking more time than specified", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-log-slower-than", "100000").Err())
		require.NoError(t, rdb.Ping(ctx).Err())
		require.EqualValues(t, 0, len(rdb.SlowLogGet(ctx, -1).Val()))
	})

	t.Run("SLOWLOG - max entries is correctly handled", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-log-slower-than", "0").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-max-len", "10").Err())
		for i := 0; i < 100; i++ {
			require.NoError(t, rdb.Ping(ctx).Err())
		}
		require.EqualValues(t, 10, len(rdb.SlowLogGet(ctx, -1).Val()))
	})

	t.Run("SLOWLOG - GET optional argument to limit output len works", func(t *testing.T) {
		require.EqualValues(t, 5, len(rdb.SlowLogGet(ctx, 5).Val()))
	})

	t.Run("SLOWLOG - RESET subcommand works", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-log-slower-than", "100000").Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())
		require.EqualValues(t, 0, len(rdb.SlowLogGet(ctx, -1).Val()))
	})

	t.Run("SLOWLOG - logged entry sanity check", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "client", "setname", "foobar").Err())
		require.NoError(t, rdb.Do(ctx, "debug", "sleep", 0.2).Err())
		val, err := rdb.SlowLogGet(ctx, -1).Result()
		require.NoError(t, err)
		require.EqualValues(t, 105, val[0].ID)
		require.EqualValues(t, true, val[0].Duration > 100000)
		require.EqualValues(t, []string{"debug", "sleep", "0.2"}, val[0].Args)
	})

	t.Run("SLOWLOG - Rewritten commands are logged as their original command", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-log-slower-than", "0").Err())
		// Test rewriting client arguments
		require.NoError(t, rdb.SAdd(ctx, "set", "a", "b", "c", "d", "e").Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())

		// SPOP is rewritten as DEL when all keys are removed
		require.NoError(t, rdb.SPopN(ctx, "set", 10).Err())
		val, err := rdb.SlowLogGet(ctx, -1).Result()
		require.NoError(t, err)
		require.EqualValues(t, []string{"spop", "set", "10"}, val[0].Args)

		// Test replacing client arguments
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())

		// GEOADD is replicated as ZADD
		require.NoError(t, rdb.GeoAdd(ctx, "cool-cities", &redis.GeoLocation{Longitude: -122.33207, Latitude: 47.60621, Name: "Seattle"}).Err())

		val, err = rdb.SlowLogGet(ctx, -1).Result()
		require.NoError(t, err)
		require.EqualValues(t, []string{"geoadd", "cool-cities", "-122.33207", "47.60621", "Seattle"}, val[0].Args)

		// Test replacing a single command argument
		require.NoError(t, rdb.Set(ctx, "A", 5, 0).Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())

		// GETSET is replicated as SET
		require.EqualValues(t, redis.Nil, rdb.GetSet(ctx, "a", "5").Err())
		val, err = rdb.SlowLogGet(ctx, -1).Result()
		require.NoError(t, err)
		require.EqualValues(t, []string{"getset", "a", "5"}, val[0].Args)

		// INCRBYFLOAT calls rewrite multiple times, so it's a special case
		require.NoError(t, rdb.Set(ctx, "A", 0, 0).Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())

		// INCRBYFLOAT is replicated as SET
		require.NoError(t, rdb.IncrByFloat(ctx, "A", 1.0).Err())
		val, err = rdb.SlowLogGet(ctx, -1).Result()
		require.NoError(t, err)
		require.EqualValues(t, []string{"incrbyfloat", "A", "1"}, val[0].Args)
	})

	t.Run("SLOWLOG - commands with too many arguments are trimmed", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-log-slower-than", "0").Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())
		require.NoError(t, rdb.SAdd(ctx, "set", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33).Err())
		val, err := rdb.SlowLogGet(ctx, -1).Result()
		require.NoError(t, err)
		require.EqualValues(t, []string{"sadd", "set", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "... (2 more arguments)"}, val[0].Args)
	})

	t.Run("SLOWLOG - too long arguments are trimmed", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-log-slower-than", "0").Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())
		require.NoError(t, rdb.SAdd(ctx, "set", "foo", strings.Repeat("A", 129)).Err())
		val, err := rdb.SlowLogGet(ctx, -1).Result()
		require.NoError(t, err)
		require.EqualValues(t, []string{"sadd", "set", "foo", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA... (1 more bytes)"}, val[0].Args)
	})

	t.Run("SLOWLOG - can clean older entries", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "client", "setname", "lastentry_client").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-max-len", "1").Err())
		require.NoError(t, rdb.Do(ctx, "debug", "sleep", 0.2).Err())

		val, err := rdb.SlowLogGet(ctx, -1).Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, len(val))
	})

	t.Run("SLOWLOG - can be disabled", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-max-len", "1").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-log-slower-than", "1").Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())
		require.NoError(t, rdb.Do(ctx, "debug", "sleep", 0.2).Err())
		require.EqualValues(t, 1, len(rdb.SlowLogGet(ctx, -1).Val()))

		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-log-slower-than", "-1").Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())
		require.NoError(t, rdb.Do(ctx, "debug", "sleep", 0.2).Err())
		require.EqualValues(t, 0, len(rdb.SlowLogGet(ctx, -1).Val()))
	})

	t.Run("SLOWLOG - slowlog get output client name and ipport", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "slowlog-log-slower-than", "0").Err())
		require.NoError(t, rdb.Do(ctx, "client", "setname", "foobar").Err())
		require.NoError(t, rdb.SAdd(ctx, "set", "foo", "bar").Err())

		val, err := rdb.SlowLogGet(ctx, -1).Result()
		require.NoError(t, err)
		require.EqualValues(t, "foobar", val[0].ClientName)
		require.NotEmpty(t, val[0].ClientAddr)
	})

}
