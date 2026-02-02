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
	"sync"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestScriptTimeout(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"lua-time-limit": "500", // 500ms timeout
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("EVAL times out after lua-time-limit", func(t *testing.T) {
		start := time.Now()
		err := rdb.Eval(ctx, "while true do end", []string{}).Err()
		elapsed := time.Since(start)

		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "timed out"), "Expected timeout error, got: %v", err)
		require.True(t, elapsed >= 400*time.Millisecond, "Should take at least 400ms")
		require.True(t, elapsed < 2*time.Second, "Should not take more than 2s")
	})

	t.Run("EVALSHA times out after lua-time-limit", func(t *testing.T) {
		// Load script first
		sha := rdb.ScriptLoad(ctx, "while true do end").Val()

		start := time.Now()
		err := rdb.EvalSha(ctx, sha, []string{}).Err()
		elapsed := time.Since(start)

		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "timed out"), "Expected timeout error, got: %v", err)
		require.True(t, elapsed >= 400*time.Millisecond, "Should take at least 400ms")
	})

	t.Run("FCALL times out after lua-time-limit", func(t *testing.T) {
		// Load function with infinite loop
		_, err := rdb.FunctionLoad(ctx, "#!lua name=timeoutlib\nredis.register_function('infiniteloop', function() while true do end end)").Result()
		require.NoError(t, err)

		start := time.Now()
		err = rdb.FCall(ctx, "infiniteloop", []string{}).Err()
		elapsed := time.Since(start)

		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "timed out"), "Expected timeout error, got: %v", err)
		require.True(t, elapsed >= 400*time.Millisecond, "Should take at least 400ms")

		// Cleanup
		rdb.FunctionDelete(ctx, "timeoutlib")
	})

	t.Run("Normal script completes without timeout", func(t *testing.T) {
		result, err := rdb.Eval(ctx, "return 'hello'", []string{}).Result()
		require.NoError(t, err)
		require.Equal(t, "hello", result)
	})
}

func TestScriptKill(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"lua-time-limit": "0", // No auto-timeout
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	rdb2 := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	defer func() { require.NoError(t, rdb2.Close()) }()

	t.Run("SCRIPT KILL aborts running script", func(t *testing.T) {
		errCh := make(chan error, 1)

		// Start infinite loop in background
		go func() {
			errCh <- rdb.Eval(ctx, "while true do end", []string{}).Err()
		}()

		// Wait for script to start
		time.Sleep(100 * time.Millisecond)

		// Kill from another connection
		err := rdb2.Do(ctx, "SCRIPT", "KILL").Err()
		require.NoError(t, err)

		// Original script should error
		select {
		case err := <-errCh:
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), "killed") || strings.Contains(err.Error(), "KILL"),
				"Expected kill error, got: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("Script did not terminate after SCRIPT KILL")
		}
	})

	t.Run("SCRIPT KILL with no running script returns error", func(t *testing.T) {
		err := rdb.Do(ctx, "SCRIPT", "KILL").Err()
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "No scripts"),
			"Expected 'No scripts' error, got: %v", err)
	})

	t.Run("SCRIPT KILL works for FCALL", func(t *testing.T) {
		// Load function
		_, err := rdb.FunctionLoad(ctx, "#!lua name=killtest\nredis.register_function('loop', function() while true do end end)").Result()
		require.NoError(t, err)

		errCh := make(chan error, 1)
		go func() {
			errCh <- rdb.FCall(ctx, "loop", []string{}).Err()
		}()

		time.Sleep(100 * time.Millisecond)

		// Kill
		err = rdb2.Do(ctx, "SCRIPT", "KILL").Err()
		require.NoError(t, err)

		select {
		case err := <-errCh:
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), "killed") || strings.Contains(err.Error(), "KILL"),
				"Expected kill error, got: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("FCALL did not terminate after SCRIPT KILL")
		}

		// Cleanup
		rdb.FunctionDelete(ctx, "killtest")
	})
}

func TestScriptTimeoutMultiTenant(t *testing.T) {
	password := "adminpwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass":    password,
		"lua-time-limit": "0", // No auto-timeout for SCRIPT KILL tests
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin connection
	adminRdb := srv.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns2", "token2").Err())

	// Tenant connections
	ns1Rdb := srv.NewClientWithOption(&redis.Options{Password: "token1"})
	ns1Rdb2 := srv.NewClientWithOption(&redis.Options{Password: "token1"})
	ns2Rdb := srv.NewClientWithOption(&redis.Options{Password: "token2"})
	defer func() { require.NoError(t, ns1Rdb.Close()) }()
	defer func() { require.NoError(t, ns1Rdb2.Close()) }()
	defer func() { require.NoError(t, ns2Rdb.Close()) }()

	t.Run("SCRIPT KILL only affects own namespace", func(t *testing.T) {
		errCh := make(chan error, 1)

		// Start infinite loop in ns1
		go func() {
			errCh <- ns1Rdb.Eval(ctx, "while true do end", []string{}).Err()
		}()
		time.Sleep(100 * time.Millisecond)

		// ns2 tries to kill - should fail (no script in ns2)
		err := ns2Rdb.Do(ctx, "SCRIPT", "KILL").Err()
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "No scripts"),
			"ns2 should not see ns1's script, got: %v", err)

		// ns1 kills its own script
		err = ns1Rdb2.Do(ctx, "SCRIPT", "KILL").Err()
		require.NoError(t, err)

		// ns1's script should be killed
		select {
		case err := <-errCh:
			require.Error(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("Script was not killed")
		}
	})

	t.Run("Long script in one namespace does NOT block other namespace", func(t *testing.T) {
		var wg sync.WaitGroup
		ns1Done := make(chan struct{})

		// Start long script in ns1
		wg.Add(1)
		go func() {
			defer wg.Done()
			// This will run for a while (we'll kill it later)
			ns1Rdb.Eval(ctx, "for i=1,100000000 do end", []string{})
			close(ns1Done)
		}()

		time.Sleep(50 * time.Millisecond) // Let ns1 script start

		// ns2 should be able to run commands immediately
		start := time.Now()
		result, err := ns2Rdb.Ping(ctx).Result()
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Equal(t, "PONG", result)
		require.True(t, elapsed < 100*time.Millisecond,
			"ns2 should respond immediately, took: %v", elapsed)

		// Kill ns1's script to clean up
		ns1Rdb2.Do(ctx, "SCRIPT", "KILL")

		// Wait for cleanup
		select {
		case <-ns1Done:
		case <-time.After(2 * time.Second):
			t.Log("ns1 script cleanup timeout (may have finished naturally)")
		}
	})

	t.Run("Parallel scripts in different namespaces", func(t *testing.T) {
		// Both namespaces run scripts in parallel
		var wg sync.WaitGroup
		results := make([]string, 2)
		errors := make([]error, 2)

		wg.Add(2)
		go func() {
			defer wg.Done()
			r, err := ns1Rdb.Eval(ctx, "return 'ns1_result'", []string{}).Result()
			if err == nil {
				results[0] = r.(string)
			}
			errors[0] = err
		}()
		go func() {
			defer wg.Done()
			r, err := ns2Rdb.Eval(ctx, "return 'ns2_result'", []string{}).Result()
			if err == nil {
				results[1] = r.(string)
			}
			errors[1] = err
		}()

		wg.Wait()

		require.NoError(t, errors[0])
		require.NoError(t, errors[1])
		require.Equal(t, "ns1_result", results[0])
		require.Equal(t, "ns2_result", results[1])
	})
}
