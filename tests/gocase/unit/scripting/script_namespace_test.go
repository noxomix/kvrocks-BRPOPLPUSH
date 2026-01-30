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

package scripting

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestScriptFlushAdminOnly(t *testing.T) {
	password := "pwd"
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

	// Create namespace
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())

	// Client for namespace
	ns1Rdb := srv.NewClientWithOption(&redis.Options{Password: "token1"})
	defer func() { require.NoError(t, ns1Rdb.Close()) }()

	t.Run("Non-admin cannot SCRIPT FLUSH", func(t *testing.T) {
		// Load a script
		sha := ns1Rdb.ScriptLoad(ctx, "return 1").Val()
		require.Len(t, sha, 40)

		// Non-admin tries to flush - should fail
		err := ns1Rdb.ScriptFlush(ctx).Err()
		require.Error(t, err)
		util.ErrorRegexp(t, err, ".*admin.*")

		// Script should still exist
		exists := ns1Rdb.ScriptExists(ctx, sha).Val()
		require.Equal(t, []bool{true}, exists)
	})

	t.Run("Admin can SCRIPT FLUSH", func(t *testing.T) {
		sha := ns1Rdb.ScriptLoad(ctx, "return 2").Val()
		require.Len(t, sha, 40)

		// Admin flushes - should work
		require.NoError(t, adminRdb.ScriptFlush(ctx).Err())

		// Script should be gone
		exists := ns1Rdb.ScriptExists(ctx, sha).Val()
		require.Equal(t, []bool{false}, exists)
	})
}
