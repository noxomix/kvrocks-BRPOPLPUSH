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
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

// Lua library that returns a namespace identifier
func luaNsLib(nsIdentifier string) string {
	return `#!lua name=nslib

local function identify(keys, args)
    return "` + nsIdentifier + `"
end

local function setval(keys, args)
    return redis.call("SET", keys[1], "` + nsIdentifier + `:" .. args[1])
end

local function getval(keys, args)
    return redis.call("GET", keys[1])
end

redis.register_function("identify", identify)
redis.register_function("setval", setval)
redis.register_function("getval", getval)
`
}

func TestFunctionNamespaceIsolation(t *testing.T) {
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

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns2", "token2").Err())

	// Clients for different namespaces
	ns1Rdb := srv.NewClientWithOption(&redis.Options{Password: "token1"})
	ns2Rdb := srv.NewClientWithOption(&redis.Options{Password: "token2"})
	defer func() { require.NoError(t, ns1Rdb.Close()) }()
	defer func() { require.NoError(t, ns2Rdb.Close()) }()

	t.Run("Functions are isolated between namespaces", func(t *testing.T) {
		// ns1: Load library with function that returns "ns1"
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns1")).Err())

		// ns1: FCALL should return "ns1"
		result := ns1Rdb.Do(ctx, "FCALL", "identify", 0)
		require.NoError(t, result.Err())
		require.Equal(t, "ns1", result.Val())

		// ns2: FCALL should fail - function not found in ns2
		result = ns2Rdb.Do(ctx, "FCALL", "identify", 0)
		util.ErrorRegexp(t, result.Err(), ".*No such function.*")

		// ns2: Load library with same name but returns "ns2"
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns2")).Err())

		// ns2: FCALL should return "ns2"
		result = ns2Rdb.Do(ctx, "FCALL", "identify", 0)
		require.NoError(t, result.Err())
		require.Equal(t, "ns2", result.Val())

		// ns1: FCALL should still return "ns1" (not affected by ns2)
		result = ns1Rdb.Do(ctx, "FCALL", "identify", 0)
		require.NoError(t, result.Err())
		require.Equal(t, "ns1", result.Val())

		// Cleanup
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "DELETE", "nslib").Err())
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "DELETE", "nslib").Err())
	})

	t.Run("FUNCTION LIST only shows own namespace", func(t *testing.T) {
		// ns1: Load two libraries
		lib1 := `#!lua name=lib1
redis.register_function("func1", function() return "lib1" end)
`
		lib2 := `#!lua name=lib2
redis.register_function("func2", function() return "lib2" end)
`
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", lib1).Err())
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", lib2).Err())

		// ns2: Load one library
		lib3 := `#!lua name=lib3
redis.register_function("func3", function() return "lib3" end)
`
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "LOAD", lib3).Err())

		// ns1: FUNCTION LIST should show lib1 and lib2, NOT lib3
		result := ns1Rdb.Do(ctx, "FUNCTION", "LIST")
		require.NoError(t, result.Err())
		listResult := result.Val().([]interface{})
		require.Equal(t, 2, len(listResult))

		// ns2: FUNCTION LIST should show only lib3
		result = ns2Rdb.Do(ctx, "FUNCTION", "LIST")
		require.NoError(t, result.Err())
		listResult = result.Val().([]interface{})
		require.Equal(t, 1, len(listResult))

		// ns1: FUNCTION LISTFUNC should show func1 and func2
		result = ns1Rdb.Do(ctx, "FUNCTION", "LISTFUNC")
		require.NoError(t, result.Err())
		listResult = result.Val().([]interface{})
		require.Equal(t, 2, len(listResult))

		// ns2: FUNCTION LISTFUNC should show only func3
		result = ns2Rdb.Do(ctx, "FUNCTION", "LISTFUNC")
		require.NoError(t, result.Err())
		listResult = result.Val().([]interface{})
		require.Equal(t, 1, len(listResult))

		// Cleanup
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "DELETE", "lib1").Err())
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "DELETE", "lib2").Err())
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "DELETE", "lib3").Err())
	})

	t.Run("FUNCTION DELETE only affects own namespace", func(t *testing.T) {
		// Both namespaces load a library with the same name
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns1")).Err())
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns2")).Err())

		// Verify both work
		require.Equal(t, "ns1", ns1Rdb.Do(ctx, "FCALL", "identify", 0).Val())
		require.Equal(t, "ns2", ns2Rdb.Do(ctx, "FCALL", "identify", 0).Val())

		// ns1: DELETE the library
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "DELETE", "nslib").Err())

		// ns1: FCALL should now fail
		util.ErrorRegexp(t, ns1Rdb.Do(ctx, "FCALL", "identify", 0).Err(), ".*No such function.*")

		// ns2: FCALL should still work!
		require.Equal(t, "ns2", ns2Rdb.Do(ctx, "FCALL", "identify", 0).Val())

		// Cleanup
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "DELETE", "nslib").Err())
	})

	t.Run("FUNCTION FLUSH deletes all namespaces", func(t *testing.T) {
		// Both namespaces load libraries
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns1")).Err())
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns2")).Err())

		// Verify both work
		require.Equal(t, "ns1", ns1Rdb.Do(ctx, "FCALL", "identify", 0).Val())
		require.Equal(t, "ns2", ns2Rdb.Do(ctx, "FCALL", "identify", 0).Val())

		// Admin: FUNCTION FLUSH (should delete ALL)
		require.NoError(t, adminRdb.Do(ctx, "FUNCTION", "FLUSH").Err())

		// Both namespaces should have empty function lists
		result := ns1Rdb.Do(ctx, "FUNCTION", "LIST")
		require.NoError(t, result.Err())
		require.Equal(t, 0, len(result.Val().([]interface{})))

		result = ns2Rdb.Do(ctx, "FUNCTION", "LIST")
		require.NoError(t, result.Err())
		require.Equal(t, 0, len(result.Val().([]interface{})))
	})

	t.Run("Functions persist after restart per namespace", func(t *testing.T) {
		// Both namespaces load libraries
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns1")).Err())
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns2")).Err())

		// Verify both work before restart
		require.Equal(t, "ns1", ns1Rdb.Do(ctx, "FCALL", "identify", 0).Val())
		require.Equal(t, "ns2", ns2Rdb.Do(ctx, "FCALL", "identify", 0).Val())

		// Restart the server
		srv.Restart()

		// Recreate clients after restart
		ns1Rdb2 := srv.NewClientWithOption(&redis.Options{Password: "token1"})
		ns2Rdb2 := srv.NewClientWithOption(&redis.Options{Password: "token2"})
		defer func() { require.NoError(t, ns1Rdb2.Close()) }()
		defer func() { require.NoError(t, ns2Rdb2.Close()) }()

		// Both should still work after restart with correct namespace values
		require.Equal(t, "ns1", ns1Rdb2.Do(ctx, "FCALL", "identify", 0).Val())
		require.Equal(t, "ns2", ns2Rdb2.Do(ctx, "FCALL", "identify", 0).Val())

		// Cleanup
		require.NoError(t, ns1Rdb2.Do(ctx, "FUNCTION", "DELETE", "nslib").Err())
		require.NoError(t, ns2Rdb2.Do(ctx, "FUNCTION", "DELETE", "nslib").Err())
	})

	t.Run("Function with keys operates in correct namespace", func(t *testing.T) {
		// Both namespaces load the library
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns1")).Err())
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns2")).Err())

		// ns1: Set a value using the function
		require.NoError(t, ns1Rdb.Do(ctx, "FCALL", "setval", 1, "mykey", "hello").Err())

		// ns1: GET should return the value
		val, err := ns1Rdb.Get(ctx, "mykey").Result()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(val, "ns1:"))

		// ns2: GET should return nothing (keys are namespace-isolated)
		_, err = ns2Rdb.Get(ctx, "mykey").Result()
		require.ErrorIs(t, err, redis.Nil)

		// ns2: Set a value using the function
		require.NoError(t, ns2Rdb.Do(ctx, "FCALL", "setval", 1, "mykey", "world").Err())

		// ns2: GET should return ns2's value
		val, err = ns2Rdb.Get(ctx, "mykey").Result()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(val, "ns2:"))

		// ns1: GET should still return ns1's value (unchanged)
		val, err = ns1Rdb.Get(ctx, "mykey").Result()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(val, "ns1:"))

		// Cleanup
		require.NoError(t, ns1Rdb.Del(ctx, "mykey").Err())
		require.NoError(t, ns2Rdb.Del(ctx, "mykey").Err())
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "DELETE", "nslib").Err())
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "DELETE", "nslib").Err())
	})
}
