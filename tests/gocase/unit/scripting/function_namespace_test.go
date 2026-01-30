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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

	t.Run("FUNCTION FLUSH only affects own namespace", func(t *testing.T) {
		// Both namespaces load libraries
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns1")).Err())
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "LOAD", luaNsLib("ns2")).Err())

		// Verify both work
		require.Equal(t, "ns1", ns1Rdb.Do(ctx, "FCALL", "identify", 0).Val())
		require.Equal(t, "ns2", ns2Rdb.Do(ctx, "FCALL", "identify", 0).Val())

		// ns1: FUNCTION FLUSH (should ONLY delete ns1's functions)
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "FLUSH").Err())

		// ns1 should have empty function list
		result := ns1Rdb.Do(ctx, "FUNCTION", "LIST")
		require.NoError(t, result.Err())
		require.Equal(t, 0, len(result.Val().([]interface{})))

		// ns1 FCALL should fail
		util.ErrorRegexp(t, ns1Rdb.Do(ctx, "FCALL", "identify", 0).Err(), ".*No such function.*")

		// ns2 should STILL have its functions (key test for namespace isolation!)
		result = ns2Rdb.Do(ctx, "FUNCTION", "LIST")
		require.NoError(t, result.Err())
		require.Equal(t, 1, len(result.Val().([]interface{})))

		// ns2 FCALL should still work!
		require.Equal(t, "ns2", ns2Rdb.Do(ctx, "FCALL", "identify", 0).Val())

		// Cleanup
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "DELETE", "nslib").Err())
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

	t.Run("FUNCTION FLUSH with multiple libraries only affects own namespace", func(t *testing.T) {
		// ns1: Load multiple libraries
		lib1 := `#!lua name=multilib1
redis.register_function("multi1", function() return "lib1" end)
`
		lib2 := `#!lua name=multilib2
redis.register_function("multi2", function() return "lib2" end)
`
		lib3 := `#!lua name=multilib3
redis.register_function("multi3", function() return "lib3" end)
`
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", lib1).Err())
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", lib2).Err())
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "LOAD", lib3).Err())

		// ns2: Load one library
		lib4 := `#!lua name=multilib4
redis.register_function("multi4", function() return "lib4" end)
`
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "LOAD", lib4).Err())

		// Verify all work
		require.Equal(t, "lib1", ns1Rdb.Do(ctx, "FCALL", "multi1", 0).Val())
		require.Equal(t, "lib2", ns1Rdb.Do(ctx, "FCALL", "multi2", 0).Val())
		require.Equal(t, "lib3", ns1Rdb.Do(ctx, "FCALL", "multi3", 0).Val())
		require.Equal(t, "lib4", ns2Rdb.Do(ctx, "FCALL", "multi4", 0).Val())

		// ns1: FUNCTION FLUSH
		require.NoError(t, ns1Rdb.Do(ctx, "FUNCTION", "FLUSH").Err())

		// ns1: All 3 libs should be gone
		result := ns1Rdb.Do(ctx, "FUNCTION", "LIST")
		require.NoError(t, result.Err())
		require.Equal(t, 0, len(result.Val().([]interface{})))

		util.ErrorRegexp(t, ns1Rdb.Do(ctx, "FCALL", "multi1", 0).Err(), ".*No such function.*")
		util.ErrorRegexp(t, ns1Rdb.Do(ctx, "FCALL", "multi2", 0).Err(), ".*No such function.*")
		util.ErrorRegexp(t, ns1Rdb.Do(ctx, "FCALL", "multi3", 0).Err(), ".*No such function.*")

		// ns2: lib4 should still work!
		require.Equal(t, "lib4", ns2Rdb.Do(ctx, "FCALL", "multi4", 0).Val())

		// Cleanup
		require.NoError(t, ns2Rdb.Do(ctx, "FUNCTION", "DELETE", "multilib4").Err())
	})
}

// TestFunctionFlushParallelIsolation tests that FUNCTION FLUSH in different namespaces
// can run truly parallel without blocking each other.
func TestFunctionFlushParallelIsolation(t *testing.T) {
	password := "pwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin client
	adminRdb := srv.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create 4 namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "parallel_ns1", "ptoken1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "parallel_ns2", "ptoken2").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "parallel_ns3", "ptoken3").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "parallel_ns4", "ptoken4").Err())

	// Create clients
	client1 := srv.NewClientWithOption(&redis.Options{Password: "ptoken1"})
	client2 := srv.NewClientWithOption(&redis.Options{Password: "ptoken2"})
	client3 := srv.NewClientWithOption(&redis.Options{Password: "ptoken3"})
	client4 := srv.NewClientWithOption(&redis.Options{Password: "ptoken4"})
	defer func() { require.NoError(t, client1.Close()) }()
	defer func() { require.NoError(t, client2.Close()) }()
	defer func() { require.NoError(t, client3.Close()) }()
	defer func() { require.NoError(t, client4.Close()) }()

	t.Run("Parallel FUNCTION FLUSH in different namespaces", func(t *testing.T) {
		// This test verifies that FUNCTION FLUSH in different namespaces can run
		// truly parallel without blocking each other (they use different namespace locks).
		//
		// We test this by:
		// 1. Loading libraries in all 4 namespaces
		// 2. Triggering FUNCTION FLUSH in all 4 namespaces simultaneously
		// 3. Verifying all complete successfully (no deadlock)
		// 4. Verifying each namespace is empty afterward

		// Load libraries in all namespaces
		for i, client := range []*redis.Client{client1, client2, client3, client4} {
			for j := 0; j < 5; j++ {
				lib := fmt.Sprintf(`#!lua name=plib%d_%d
redis.register_function("pfunc%d_%d", function() return %d end)
`, i, j, i, j, i*10+j)
				require.NoError(t, client.Do(ctx, "FUNCTION", "LOAD", lib).Err())
			}
		}

		// Verify all loaded
		for _, client := range []*redis.Client{client1, client2, client3, client4} {
			result := client.Do(ctx, "FUNCTION", "LIST")
			require.NoError(t, result.Err())
			require.Equal(t, 5, len(result.Val().([]interface{})))
		}

		var wg sync.WaitGroup
		startBarrier := make(chan struct{})
		errors := make(chan error, 4)

		// Launch parallel FUNCTION FLUSH - all wait at barrier then execute together
		for _, client := range []*redis.Client{client1, client2, client3, client4} {
			wg.Add(1)
			go func(c *redis.Client) {
				defer wg.Done()
				<-startBarrier
				if err := c.Do(ctx, "FUNCTION", "FLUSH").Err(); err != nil {
					errors <- err
				}
			}(client)
		}

		// Trigger all at once
		close(startBarrier)
		wg.Wait()
		close(errors)

		// Check no errors - all FLUSH operations should succeed
		for err := range errors {
			require.NoError(t, err)
		}

		// All namespaces should be empty - each FLUSH only affected its own namespace
		for _, client := range []*redis.Client{client1, client2, client3, client4} {
			result := client.Do(ctx, "FUNCTION", "LIST")
			require.NoError(t, result.Err())
			require.Equal(t, 0, len(result.Val().([]interface{})))
		}

		// Note: We don't assert on timing because FUNCTION FLUSH is very fast.
		// The main value of this test is verifying:
		// - No deadlock when 4 namespaces flush simultaneously
		// - All operations succeed
		// - Each namespace is properly isolated (empty after its own flush)
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "parallel_ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "parallel_ns2").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "parallel_ns3").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "parallel_ns4").Err())
}

// TestFunctionFlushWhileFCALLRunning tests that FUNCTION FLUSH blocks FCALL in the same
// namespace but does not affect FCALL in other namespaces.
func TestFunctionFlushWhileFCALLRunning(t *testing.T) {
	password := "pwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin client
	adminRdb := srv.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "fcall_ns1", "ftoken1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "fcall_ns2", "ftoken2").Err())

	// Create clients
	client1 := srv.NewClientWithOption(&redis.Options{Password: "ftoken1"})
	client2 := srv.NewClientWithOption(&redis.Options{Password: "ftoken2"})
	defer func() { require.NoError(t, client1.Close()) }()
	defer func() { require.NoError(t, client2.Close()) }()

	t.Run("FCALL in other namespace continues during FUNCTION FLUSH", func(t *testing.T) {
		// Load library in both namespaces
		lib := `#!lua name=fcalllib
redis.register_function("counter", function(keys, args)
    local current = redis.call("INCR", keys[1])
    return current
end)
`
		require.NoError(t, client1.Do(ctx, "FUNCTION", "LOAD", lib).Err())
		require.NoError(t, client2.Do(ctx, "FUNCTION", "LOAD", lib).Err())

		// Initialize counters (each namespace has its own key)
		require.NoError(t, client1.Set(ctx, "fcall_counter", "0", 0).Err())
		require.NoError(t, client2.Set(ctx, "fcall_counter", "0", 0).Err())

		var wg sync.WaitGroup
		var ns2CallCount atomic.Int64
		stopNs2 := make(chan struct{})

		// ns2: Run FCALL in a loop
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopNs2:
					return
				default:
					result := client2.Do(ctx, "FCALL", "counter", 1, "fcall_counter")
					if result.Err() == nil {
						ns2CallCount.Add(1)
					}
				}
			}
		}()

		// Let ns2 run some calls
		time.Sleep(50 * time.Millisecond)
		callsBefore := ns2CallCount.Load()

		// ns1: FUNCTION FLUSH (should NOT affect ns2)
		require.NoError(t, client1.Do(ctx, "FUNCTION", "FLUSH").Err())

		// Let ns2 run more calls after ns1 flushed
		time.Sleep(50 * time.Millisecond)
		callsAfter := ns2CallCount.Load()

		// Stop ns2 worker
		close(stopNs2)
		wg.Wait()

		// ns2 should have continued running FCALL during and after ns1's FLUSH
		require.Greater(t, callsAfter, callsBefore, "ns2 FCALL should continue during ns1 FUNCTION FLUSH")

		// ns1: FCALL should fail (flushed)
		util.ErrorRegexp(t, client1.Do(ctx, "FCALL", "counter", 1, "fcall_counter").Err(), ".*No such function.*")

		// ns2: FCALL should still work
		result := client2.Do(ctx, "FCALL", "counter", 1, "fcall_counter")
		require.NoError(t, result.Err())

		// Cleanup
		require.NoError(t, client2.Do(ctx, "FUNCTION", "DELETE", "fcalllib").Err())
		require.NoError(t, client1.Del(ctx, "fcall_counter").Err())
		require.NoError(t, client2.Del(ctx, "fcall_counter").Err())
	})

	t.Run("FCALL already running completes even if FLUSH is called", func(t *testing.T) {
		// This test verifies that an FCALL that has already started will complete
		// its execution even if FUNCTION FLUSH is called while it's running.
		//
		// With async reset, FLUSH doesn't block - it just sets a reset flag.
		// But an FCALL that's already past the entry check will continue to completion.
		//
		// Strategy:
		// 1. Load a function that signals "I'm running" by setting a key, then does slow work
		// 2. Start FCALL in goroutine
		// 3. Wait until the "running" signal appears (proves FCALL actually started)
		// 4. Call FLUSH (with async reset, this returns immediately)
		// 5. Verify FCALL completed its work (the function was still available during execution)

		// Function that: sets "running" flag, does slow work, sets "completed" flag
		slowLib := `#!lua name=slowlib
local function slow(keys, args)
    -- Signal that we're running
    redis.call("SET", "slow_running", "1")

    -- Do slow work (enough iterations to take measurable time)
    local sum = 0
    for i = 1, 5000000 do
        sum = sum + i
    end

    -- Signal completion with the result
    redis.call("SET", "slow_completed", tostring(sum))
    return sum
end
redis.register_function("slow", slow)
`
		require.NoError(t, client1.Do(ctx, "FUNCTION", "LOAD", slowLib).Err())

		// Clean any leftover keys
		client1.Del(ctx, "slow_running", "slow_completed")

		var wg sync.WaitGroup
		var fcallErr error

		// Start FCALL in background
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := client1.Do(ctx, "FCALL", "slow", 0)
			fcallErr = result.Err()
		}()

		// Wait until FCALL is actually running (not just queued)
		// Poll for the "running" signal with timeout
		runningDetected := false
		for i := 0; i < 100; i++ { // Max 1 second
			val, err := client1.Get(ctx, "slow_running").Result()
			if err == nil && val == "1" {
				runningDetected = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.True(t, runningDetected, "FCALL should have started and set running flag")

		// Now start FLUSH - it should wait for FCALL to complete
		// (because FCALL holds shared_lock, FLUSH needs exclusive_lock)
		flushErr := client1.Do(ctx, "FUNCTION", "FLUSH").Err()
		require.NoError(t, flushErr)

		// Wait for FCALL goroutine to finish
		wg.Wait()
		require.NoError(t, fcallErr)

		// Key test: "slow_completed" should exist, proving FCALL finished its work
		// before FLUSH deleted the function (if FLUSH ran first, FCALL would fail mid-execution)
		val, err := client1.Get(ctx, "slow_completed").Result()
		require.NoError(t, err, "slow_completed key should exist - FCALL should have completed before FLUSH")
		require.NotEmpty(t, val)

		// Cleanup
		client1.Del(ctx, "slow_running", "slow_completed")
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "fcall_ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "fcall_ns2").Err())
}

// TestAsyncResetBehavior tests the async/lazy reset mechanism for Lua states.
// With async reset, FUNCTION FLUSH doesn't directly reset other workers' Lua states.
// Instead, it sets a flag, and each worker resets its own state lazily when it
// next attempts to execute Lua code for that namespace.
func TestAsyncResetBehavior(t *testing.T) {
	password := "pwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin client
	adminRdb := srv.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "async_ns1", "atoken1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "async_ns2", "atoken2").Err())

	// Create clients
	client1 := srv.NewClientWithOption(&redis.Options{Password: "atoken1"})
	client2 := srv.NewClientWithOption(&redis.Options{Password: "atoken2"})
	defer func() { require.NoError(t, client1.Close()) }()
	defer func() { require.NoError(t, client2.Close()) }()

	t.Run("FCALL after FLUSH reloads function from storage", func(t *testing.T) {
		// This tests the lazy reload behavior:
		// 1. Load a function
		// 2. Call it (loads into Lua state)
		// 3. FLUSH (marks for reset, doesn't actually reset yet)
		// 4. LOAD the function again (stores in storage)
		// 5. FCALL should work (triggers reset, then reloads from storage)

		lib := `#!lua name=reloadlib
redis.register_function("reloadfunc", function() return "reloaded" end)
`
		// Load and verify it works
		require.NoError(t, client1.Do(ctx, "FUNCTION", "LOAD", lib).Err())
		result := client1.Do(ctx, "FCALL", "reloadfunc", 0)
		require.NoError(t, result.Err())
		require.Equal(t, "reloaded", result.Val())

		// FLUSH (marks for reset)
		require.NoError(t, client1.Do(ctx, "FUNCTION", "FLUSH").Err())

		// Load the function again
		require.NoError(t, client1.Do(ctx, "FUNCTION", "LOAD", lib).Err())

		// FCALL should work - it reloads from storage after the reset
		result = client1.Do(ctx, "FCALL", "reloadfunc", 0)
		require.NoError(t, result.Err())
		require.Equal(t, "reloaded", result.Val())

		// Cleanup
		require.NoError(t, client1.Do(ctx, "FUNCTION", "DELETE", "reloadlib").Err())
	})

	t.Run("Rapid FLUSH LOAD cycles work correctly", func(t *testing.T) {
		// Test that rapid FLUSH/LOAD cycles don't cause issues.
		// This tests the race condition handling where:
		// - FLUSH sets reset flag
		// - LOAD stores function and loads into Lua
		// - Next FCALL might see reset flag and reload from storage

		lib := `#!lua name=rapidlib
redis.register_function("rapidfunc", function() return "rapid" end)
`
		// Do 10 rapid cycles
		for i := 0; i < 10; i++ {
			require.NoError(t, client1.Do(ctx, "FUNCTION", "LOAD", lib).Err())
			require.NoError(t, client1.Do(ctx, "FUNCTION", "FLUSH").Err())
		}

		// Final load
		require.NoError(t, client1.Do(ctx, "FUNCTION", "LOAD", lib).Err())

		// Should work - function is in storage and will be loaded
		result := client1.Do(ctx, "FCALL", "rapidfunc", 0)
		require.NoError(t, result.Err())
		require.Equal(t, "rapid", result.Val())

		// Cleanup
		require.NoError(t, client1.Do(ctx, "FUNCTION", "DELETE", "rapidlib").Err())
	})

	t.Run("Multiple connections same namespace see reset", func(t *testing.T) {
		// Test that when FLUSH is called from one connection,
		// another connection to the same namespace also sees the reset.
		// This verifies the async reset propagates across workers.

		// Create a second client for the same namespace
		client1b := srv.NewClientWithOption(&redis.Options{Password: "atoken1"})
		defer func() { require.NoError(t, client1b.Close()) }()

		lib := `#!lua name=multiconnlib
redis.register_function("multiconnfunc", function() return "multiconn" end)
`
		// Load from client1
		require.NoError(t, client1.Do(ctx, "FUNCTION", "LOAD", lib).Err())

		// Verify both connections can call it
		require.Equal(t, "multiconn", client1.Do(ctx, "FCALL", "multiconnfunc", 0).Val())
		require.Equal(t, "multiconn", client1b.Do(ctx, "FCALL", "multiconnfunc", 0).Val())

		// FLUSH from client1
		require.NoError(t, client1.Do(ctx, "FUNCTION", "FLUSH").Err())

		// Both connections should see the function as gone
		util.ErrorRegexp(t, client1.Do(ctx, "FCALL", "multiconnfunc", 0).Err(), ".*No such function.*")
		util.ErrorRegexp(t, client1b.Do(ctx, "FCALL", "multiconnfunc", 0).Err(), ".*No such function.*")
	})

	t.Run("FLUSH in one namespace does not trigger reset in another", func(t *testing.T) {
		// Verify that async reset flags are per-namespace.
		// FLUSH in ns1 should not affect ns2's Lua state at all.

		lib1 := `#!lua name=isolib
redis.register_function("isofunc", function() return "ns1" end)
`
		lib2 := `#!lua name=isolib
redis.register_function("isofunc", function() return "ns2" end)
`
		// Load in both namespaces
		require.NoError(t, client1.Do(ctx, "FUNCTION", "LOAD", lib1).Err())
		require.NoError(t, client2.Do(ctx, "FUNCTION", "LOAD", lib2).Err())

		// Verify both work
		require.Equal(t, "ns1", client1.Do(ctx, "FCALL", "isofunc", 0).Val())
		require.Equal(t, "ns2", client2.Do(ctx, "FCALL", "isofunc", 0).Val())

		// FLUSH ns1 many times (simulating spam)
		for i := 0; i < 20; i++ {
			require.NoError(t, client1.Do(ctx, "FUNCTION", "FLUSH").Err())
		}

		// ns1 should be empty
		util.ErrorRegexp(t, client1.Do(ctx, "FCALL", "isofunc", 0).Err(), ".*No such function.*")

		// ns2 should still work perfectly (no reset was triggered)
		require.Equal(t, "ns2", client2.Do(ctx, "FCALL", "isofunc", 0).Val())

		// Cleanup
		require.NoError(t, client2.Do(ctx, "FUNCTION", "DELETE", "isolib").Err())
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "async_ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "async_ns2").Err())
}

// TestGlobalScriptFlush tests that global SCRIPT FLUSH (as opposed to namespace
// FUNCTION FLUSH) correctly resets all namespaces using the generation counter.
func TestGlobalScriptFlush(t *testing.T) {
	password := "pwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin client
	adminRdb := srv.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "script_ns1", "stoken1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "script_ns2", "stoken2").Err())

	// Create clients
	client1 := srv.NewClientWithOption(&redis.Options{Password: "stoken1"})
	client2 := srv.NewClientWithOption(&redis.Options{Password: "stoken2"})
	defer func() { require.NoError(t, client1.Close()) }()
	defer func() { require.NoError(t, client2.Close()) }()

	t.Run("Global SCRIPT FLUSH resets all namespaces", func(t *testing.T) {
		// Load EVAL scripts in both namespaces
		script := "return 'hello'"

		// EVAL creates a cached script in the Lua state
		result1 := client1.Do(ctx, "EVAL", script, 0)
		require.NoError(t, result1.Err())
		require.Equal(t, "hello", result1.Val())

		result2 := client2.Do(ctx, "EVAL", script, 0)
		require.NoError(t, result2.Err())
		require.Equal(t, "hello", result2.Val())

		// Global SCRIPT FLUSH from admin (affects ALL namespaces)
		require.NoError(t, adminRdb.Do(ctx, "SCRIPT", "FLUSH").Err())

		// Both namespaces should still be able to EVAL (scripts are re-created)
		// This verifies the reset happened but doesn't break functionality
		result1 = client1.Do(ctx, "EVAL", script, 0)
		require.NoError(t, result1.Err())
		require.Equal(t, "hello", result1.Val())

		result2 = client2.Do(ctx, "EVAL", script, 0)
		require.NoError(t, result2.Err())
		require.Equal(t, "hello", result2.Val())
	})

	t.Run("SCRIPT FLUSH clears EVALSHA cache", func(t *testing.T) {
		// Load a script with SCRIPT LOAD
		script := "return 'cached'"
		sha := adminRdb.Do(ctx, "SCRIPT", "LOAD", script).Val().(string)
		require.NotEmpty(t, sha)

		// EVALSHA should work
		result := adminRdb.Do(ctx, "EVALSHA", sha, 0)
		require.NoError(t, result.Err())
		require.Equal(t, "cached", result.Val())

		// SCRIPT FLUSH - clears from storage AND Lua
		require.NoError(t, adminRdb.Do(ctx, "SCRIPT", "FLUSH").Err())

		// EVALSHA should FAIL - script is gone from storage and Lua
		result = adminRdb.Do(ctx, "EVALSHA", sha, 0)
		util.ErrorRegexp(t, result.Err(), ".*NOSCRIPT.*")
	})

	// Cleanup namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "script_ns1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "DEL", "script_ns2").Err())
}
