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
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestStatsAdminOnly(t *testing.T) {
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

	// Create namespace for tenant
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())

	// Tenant connection
	ns1Rdb := srv.NewClientWithOption(&redis.Options{
		Password: "token1",
	})
	defer func() { require.NoError(t, ns1Rdb.Close()) }()

	t.Run("Tenant cannot execute STATS command", func(t *testing.T) {
		result := ns1Rdb.Do(ctx, "STATS")
		err := result.Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "admin permission required")
	})

	t.Run("Admin can execute STATS command", func(t *testing.T) {
		result := adminRdb.Do(ctx, "STATS")
		require.NoError(t, result.Err())

		// STATS returns RocksDB statistics as JSON
		stats, err := result.Text()
		require.NoError(t, err)
		require.NotEmpty(t, stats)
		// RocksDB stats JSON should contain typical keys
		require.Contains(t, stats, "rocksdb")
	})

	// Cleanup
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns1").Err())
}
