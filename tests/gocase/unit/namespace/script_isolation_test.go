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
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestScriptNamespaceIsolation(t *testing.T) {
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

	t.Run("SCRIPT FLUSH only affects own namespace", func(t *testing.T) {
		// Load scripts in both namespaces
		sha1 := ns1Rdb.ScriptLoad(ctx, "return 1").Val()
		sha2 := ns2Rdb.ScriptLoad(ctx, "return 2").Val()

		// Verify both scripts exist
		exists1 := ns1Rdb.ScriptExists(ctx, sha1).Val()
		require.Equal(t, []bool{true}, exists1, "ns1 should have sha1")

		exists2 := ns2Rdb.ScriptExists(ctx, sha2).Val()
		require.Equal(t, []bool{true}, exists2, "ns2 should have sha2")

		// Flush scripts in ns1 only
		require.NoError(t, ns1Rdb.ScriptFlush(ctx).Err())

		// ns1's script should be gone
		exists1 = ns1Rdb.ScriptExists(ctx, sha1).Val()
		require.Equal(t, []bool{false}, exists1, "ns1 should NOT have sha1 after flush")

		// ns2's script should still exist
		exists2 = ns2Rdb.ScriptExists(ctx, sha2).Val()
		require.Equal(t, []bool{true}, exists2, "ns2 should still have sha2")
	})

	t.Run("SCRIPT EXISTS isolated per namespace", func(t *testing.T) {
		// Load script in ns1
		sha := ns1Rdb.ScriptLoad(ctx, "return 'ns1_script'").Val()

		// ns1 should see it
		exists1 := ns1Rdb.ScriptExists(ctx, sha).Val()
		require.Equal(t, []bool{true}, exists1, "ns1 should see its own script")

		// ns2 should NOT see it
		exists2 := ns2Rdb.ScriptExists(ctx, sha).Val()
		require.Equal(t, []bool{false}, exists2, "ns2 should NOT see ns1's script")

		// Cleanup
		require.NoError(t, ns1Rdb.ScriptFlush(ctx).Err())
	})

	t.Run("EVALSHA works only in own namespace", func(t *testing.T) {
		// Load script in ns1
		sha := ns1Rdb.ScriptLoad(ctx, "return 'hello'").Val()

		// ns1 can execute it
		result := ns1Rdb.EvalSha(ctx, sha, []string{}).Val()
		require.Equal(t, "hello", result, "ns1 should execute its script")

		// ns2 cannot execute it (NOSCRIPT error)
		err := ns2Rdb.EvalSha(ctx, sha, []string{}).Err()
		require.Error(t, err, "ns2 should get NOSCRIPT error")
		require.Contains(t, err.Error(), "NOSCRIPT", "error should be NOSCRIPT")

		// Cleanup
		require.NoError(t, ns1Rdb.ScriptFlush(ctx).Err())
	})

	t.Run("Same script different namespaces", func(t *testing.T) {
		// Same script body in both namespaces
		script := "return 'same_script'"

		// Both namespaces load the same script (same SHA)
		sha1 := ns1Rdb.ScriptLoad(ctx, script).Val()
		sha2 := ns2Rdb.ScriptLoad(ctx, script).Val()
		require.Equal(t, sha1, sha2, "same script body should produce same SHA")

		// Both can execute
		result1 := ns1Rdb.EvalSha(ctx, sha1, []string{}).Val()
		require.Equal(t, "same_script", result1)

		result2 := ns2Rdb.EvalSha(ctx, sha2, []string{}).Val()
		require.Equal(t, "same_script", result2)

		// Flush ns1's scripts
		require.NoError(t, ns1Rdb.ScriptFlush(ctx).Err())

		// ns1 should NOT be able to execute anymore
		err := ns1Rdb.EvalSha(ctx, sha1, []string{}).Err()
		require.Error(t, err, "ns1 should get NOSCRIPT after flush")
		require.Contains(t, err.Error(), "NOSCRIPT")

		// ns2 should STILL be able to execute
		result2 = ns2Rdb.EvalSha(ctx, sha2, []string{}).Val()
		require.Equal(t, "same_script", result2, "ns2 should still have its script")

		// Cleanup
		require.NoError(t, ns2Rdb.ScriptFlush(ctx).Err())
	})

	t.Run("Admin sees only default namespace scripts", func(t *testing.T) {
		// Admin loads a script
		adminSha := adminRdb.ScriptLoad(ctx, "return 42").Val()

		// Admin should see it
		existsAdmin := adminRdb.ScriptExists(ctx, adminSha).Val()
		require.Equal(t, []bool{true}, existsAdmin, "admin should see its own script")

		// ns1 should NOT see admin's script
		exists1 := ns1Rdb.ScriptExists(ctx, adminSha).Val()
		require.Equal(t, []bool{false}, exists1, "ns1 should NOT see admin's script")

		// ns1 loads a script
		ns1Sha := ns1Rdb.ScriptLoad(ctx, "return 'ns1'").Val()

		// Admin should NOT see ns1's script
		existsAdmin = adminRdb.ScriptExists(ctx, ns1Sha).Val()
		require.Equal(t, []bool{false}, existsAdmin, "admin should NOT see ns1's script")

		// Cleanup
		require.NoError(t, adminRdb.ScriptFlush(ctx).Err())
		require.NoError(t, ns1Rdb.ScriptFlush(ctx).Err())
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "ns2").Err())
}
