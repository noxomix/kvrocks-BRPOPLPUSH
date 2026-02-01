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

package ping

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestPerflog(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	t.Run("PerfLog", func(t *testing.T) {
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.ConfigSet(ctx, "profiling-sample-commands", "set").Err())
		require.NoError(t, c.ConfigSet(ctx, "profiling-sample-ratio", "100").Err())
		require.NoError(t, c.ConfigSet(ctx, "profiling-sample-record-threshold-ms", "0").Err())

		for i := 0; i < 10; i++ {
			require.NoError(t, c.Set(ctx, fmt.Sprintf("key-%d", i), "value", 0).Err())
		}
		require.EqualValues(t, 10, c.Do(ctx, "perflog", "len").Val())
	})
}

func TestPerflogNamespaceIsolation(t *testing.T) {
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
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns2", "token2").Err())

	// Namespace clients
	ns1Rdb := srv.NewClientWithOption(&redis.Options{Password: "token1"})
	ns2Rdb := srv.NewClientWithOption(&redis.Options{Password: "token2"})
	defer func() { require.NoError(t, ns1Rdb.Close()) }()
	defer func() { require.NoError(t, ns2Rdb.Close()) }()

	// Enable profiling for SET command
	require.NoError(t, adminRdb.ConfigSet(ctx, "profiling-sample-commands", "set").Err())
	require.NoError(t, adminRdb.ConfigSet(ctx, "profiling-sample-ratio", "100").Err())
	require.NoError(t, adminRdb.ConfigSet(ctx, "profiling-sample-record-threshold-ms", "0").Err())

	// Reset perflog
	require.NoError(t, adminRdb.Do(ctx, "PERFLOG", "RESET").Err())

	t.Run("Tenants only see their own perflog entries", func(t *testing.T) {
		// ns1: Execute SET commands
		for i := 0; i < 5; i++ {
			require.NoError(t, ns1Rdb.Set(ctx, fmt.Sprintf("ns1_key_%d", i), "value", 0).Err())
		}
		// ns2: Execute SET commands
		for i := 0; i < 3; i++ {
			require.NoError(t, ns2Rdb.Set(ctx, fmt.Sprintf("ns2_key_%d", i), "value", 0).Err())
		}

		// ns1 should only see 5 entries
		ns1Len := ns1Rdb.Do(ctx, "PERFLOG", "LEN").Val().(int64)
		require.Equal(t, int64(5), ns1Len)

		// ns2 should only see 3 entries
		ns2Len := ns2Rdb.Do(ctx, "PERFLOG", "LEN").Val().(int64)
		require.Equal(t, int64(3), ns2Len)
	})

	t.Run("Admin sees all perflog entries", func(t *testing.T) {
		adminLen := adminRdb.Do(ctx, "PERFLOG", "LEN").Val().(int64)
		require.GreaterOrEqual(t, adminLen, int64(8)) // at least ns1(5) + ns2(3)
	})

	t.Run("PERFLOG RESET only clears own namespace for tenant", func(t *testing.T) {
		// ns1 resets - should only clear ns1 entries
		require.NoError(t, ns1Rdb.Do(ctx, "PERFLOG", "RESET").Err())

		ns1LenAfter := ns1Rdb.Do(ctx, "PERFLOG", "LEN").Val().(int64)
		require.Equal(t, int64(0), ns1LenAfter)

		// ns2 should still have entries
		ns2LenAfter := ns2Rdb.Do(ctx, "PERFLOG", "LEN").Val().(int64)
		require.Equal(t, int64(3), ns2LenAfter)
	})

	t.Run("Admin PERFLOG RESET clears all entries", func(t *testing.T) {
		// Create some entries first
		require.NoError(t, ns1Rdb.Set(ctx, "test1", "val1", 0).Err())
		require.NoError(t, ns2Rdb.Set(ctx, "test2", "val2", 0).Err())

		// Admin resets
		require.NoError(t, adminRdb.Do(ctx, "PERFLOG", "RESET").Err())

		// All namespaces should have 0 entries
		ns1Len := ns1Rdb.Do(ctx, "PERFLOG", "LEN").Val().(int64)
		ns2Len := ns2Rdb.Do(ctx, "PERFLOG", "LEN").Val().(int64)

		require.Equal(t, int64(0), ns1Len)
		require.Equal(t, int64(0), ns2Len)
	})
}
