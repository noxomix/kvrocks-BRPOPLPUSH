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

package scheduling

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// extractWorkerIDs parses CLIENT LIST output and extracts worker IDs
func extractWorkerIDs(clientList string) []int {
	var workerIDs []int
	re := regexp.MustCompile(`worker=(\d+)`)
	lines := strings.Split(strings.TrimSpace(clientList), "\n")
	for _, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) == 2 {
			id, err := strconv.Atoi(matches[1])
			if err == nil {
				workerIDs = append(workerIDs, id)
			}
		}
	}
	return workerIDs
}

func extractWorkerIDsByNamePrefix(clientList, prefix string) []int {
	var workerIDs []int
	re := regexp.MustCompile(`worker=(\d+)`)
	lines := strings.Split(strings.TrimSpace(clientList), "\n")
	for _, line := range lines {
		if !strings.Contains(line, "name="+prefix) {
			continue
		}
		matches := re.FindStringSubmatch(line)
		if len(matches) == 2 {
			id, err := strconv.Atoi(matches[1])
			if err == nil {
				workerIDs = append(workerIDs, id)
			}
		}
	}
	return workerIDs
}

// countUniqueWorkers counts unique worker IDs
func countUniqueWorkers(ids []int) int {
	seen := make(map[int]bool)
	for _, id := range ids {
		seen[id] = true
	}
	return len(seen)
}

func TestFairSchedulerConfig(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SNI config parameters are gettable and settable", func(t *testing.T) {
		// Test sni-max-workers-percent
		require.NoError(t, rdb.ConfigSet(ctx, "sni-max-workers-percent", "50").Err())
		val := rdb.ConfigGet(ctx, "sni-max-workers-percent").Val()
		require.Equal(t, "50", val["sni-max-workers-percent"])

		// Test sni-overdraft-percent
		require.NoError(t, rdb.ConfigSet(ctx, "sni-overdraft-percent", "40").Err())
		val = rdb.ConfigGet(ctx, "sni-overdraft-percent").Val()
		require.Equal(t, "40", val["sni-overdraft-percent"])
	})

}

func TestFairSchedulerConfigDefaults(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	val := rdb.ConfigGet(ctx, "sni-max-workers-percent").Val()
	require.Equal(t, "0", val["sni-max-workers-percent"])

	val = rdb.ConfigGet(ctx, "sni-overdraft-percent").Val()
	require.Equal(t, "30", val["sni-overdraft-percent"])
}

func TestFairSchedulerWorkerDistribution(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"workers":               "8",
		"sni-overdraft-percent": "30",
	})
	defer srv.Close()

	ctx := context.Background()

	t.Run("CLIENT LIST includes worker field", func(t *testing.T) {
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()

		result := rdb.ClientList(ctx)
		require.NoError(t, result.Err())

		// Check that worker= field is present
		require.Contains(t, result.Val(), "worker=", "CLIENT LIST should include worker= field")
	})

	t.Run("TCPClient connection works", func(t *testing.T) {
		tc := srv.NewTCPClient()
		defer func() { require.NoError(t, tc.Close()) }()

		require.NoError(t, tc.WriteArgs("PING"))
		tc.MustRead(t, "+PONG")
	})

	t.Run("Multiple connections from same IP spread across workers", func(t *testing.T) {
		// Create 16 connections from localhost (single scheduling key: default.domain)
		conns := make([]*redis.Client, 16)
		for i := 0; i < 16; i++ {
			conns[i] = srv.NewClient()
			require.NoError(t, conns[i].Ping(ctx).Err())
		}
		defer func() {
			for _, c := range conns {
				c.Close()
			}
		}()

		// Get CLIENT LIST from first connection
		list := conns[0].ClientList(ctx).Val()
		workerIDs := extractWorkerIDs(list)

		unique := countUniqueWorkers(workerIDs)

		// With no concentration gating, single-key traffic should fan out early.
		require.GreaterOrEqual(t, unique, 6,
			"16 connections should spread to >=6 workers, got %d unique workers from IDs: %v",
			unique, workerIDs)
	})
}

func TestFairSchedulerLuaAvoidance(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"workers":        "4",
		"lua-time-limit": "0", // No auto-timeout
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("New connections avoid Lua-blocked workers", func(t *testing.T) {
		errCh := make(chan error, 1)

		// Start infinite loop in background
		go func() {
			errCh <- rdb.Eval(ctx, "while true do end", []string{}).Err()
		}()

		// Wait for script to start
		time.Sleep(100 * time.Millisecond)

		// Create new connection AFTER script started
		// Fair scheduler should route this to a non-blocked worker
		tc := srv.NewTCPClient()
		defer func() { require.NoError(t, tc.Close()) }()

		// PING should respond immediately (not blocked by Lua)
		start := time.Now()
		require.NoError(t, tc.WriteArgs("PING"))
		tc.MustRead(t, "+PONG")
		elapsed := time.Since(start)

		require.True(t, elapsed < 500*time.Millisecond,
			"New connection should respond immediately, took: %v", elapsed)

		// Kill the blocking script
		tcKill := srv.NewTCPClient()
		defer func() { require.NoError(t, tcKill.Close()) }()
		require.NoError(t, tcKill.WriteArgs("SCRIPT", "KILL"))
		tcKill.MustRead(t, "+OK")

		// Wait for script to be killed
		select {
		case <-errCh:
			// Script killed, good
		case <-time.After(2 * time.Second):
			t.Log("Script cleanup timeout")
		}
	})
}

func TestFairSchedulerMultiTenant(t *testing.T) {
	password := "adminpwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
		"workers":     "8",
	})
	defer srv.Close()

	ctx := context.Background()

	// Admin connection
	adminRdb := srv.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, adminRdb.Close()) }()

	// Create namespaces
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())
	require.NoError(t, adminRdb.Do(ctx, "NAMESPACE", "ADD", "ns2", "token2").Err())

	t.Run("Different namespaces can use different workers", func(t *testing.T) {
		// Create connections for ns1
		ns1Conns := make([]*redis.Client, 5)
		for i := 0; i < 5; i++ {
			ns1Conns[i] = srv.NewClientWithOption(&redis.Options{Password: "token1"})
			require.NoError(t, ns1Conns[i].Ping(ctx).Err())
		}
		defer func() {
			for _, c := range ns1Conns {
				c.Close()
			}
		}()

		// Create connections for ns2
		ns2Conns := make([]*redis.Client, 5)
		for i := 0; i < 5; i++ {
			ns2Conns[i] = srv.NewClientWithOption(&redis.Options{Password: "token2"})
			require.NoError(t, ns2Conns[i].Ping(ctx).Err())
		}
		defer func() {
			for _, c := range ns2Conns {
				c.Close()
			}
		}()

		// Verify both namespaces have connections
		// Note: Since all connections come from localhost without TLS,
		// they may share the same scheduling key. This test mainly verifies
		// that multi-tenant setup works with fair scheduling enabled.
		ns1List := ns1Conns[0].ClientList(ctx).Val()
		require.Contains(t, ns1List, "worker=")

		ns2List := ns2Conns[0].ClientList(ctx).Val()
		require.Contains(t, ns2List, "worker=")
	})
}

func TestFairSchedulerSNIDistribution(t *testing.T) {
	// SNI hostnames that should be in /etc/hosts pointing to 127.0.0.1
	sniHosts := []string{
		"localhost",
		"ecebo.dev",
		"noxomix.dev",
		"claude.local",
	}

	// Start TLS server with auto-generated certs supporting these SNIs
	srv, caCertPath := util.StartTLSServerWithSNIs(t, map[string]string{
		"workers":               "8",
		"sni-overdraft-percent": "30",
	}, sniHosts)
	defer srv.Close()

	ctx := context.Background()

	t.Run("Different SNIs get different worker groups", func(t *testing.T) {
		// Create connections with different SNIs
		sni1 := "ecebo.dev"
		sni2 := "noxomix.dev"

		// TLS config for SNI 1
		tlsConfig1, err := util.TLSConfigWithSNI(sni1, caCertPath)
		require.NoError(t, err)

		// TLS config for SNI 2
		tlsConfig2, err := util.TLSConfigWithSNI(sni2, caCertPath)
		require.NoError(t, err)

		// Create 5 connections with SNI 1
		sni1Conns := make([]*redis.Client, 5)
		for i := 0; i < 5; i++ {
			sni1Conns[i] = srv.NewClientWithOption(&redis.Options{
				Addr:       srv.TLSAddr(),
				TLSConfig:  tlsConfig1,
				ClientName: fmt.Sprintf("sni1-%d", i),
			})
			require.NoError(t, sni1Conns[i].Ping(ctx).Err())
		}
		defer func() {
			for _, c := range sni1Conns {
				c.Close()
			}
		}()

		// Create 5 connections with SNI 2
		sni2Conns := make([]*redis.Client, 5)
		for i := 0; i < 5; i++ {
			sni2Conns[i] = srv.NewClientWithOption(&redis.Options{
				Addr:       srv.TLSAddr(),
				TLSConfig:  tlsConfig2,
				ClientName: fmt.Sprintf("sni2-%d", i),
			})
			require.NoError(t, sni2Conns[i].Ping(ctx).Err())
		}
		defer func() {
			for _, c := range sni2Conns {
				c.Close()
			}
		}()

		// Get worker IDs for SNI 1 connections
		sni1List := sni1Conns[0].ClientList(ctx).Val()
		sni1Workers := extractWorkerIDsByNamePrefix(sni1List, "sni1-")
		t.Logf("SNI %s connections on workers: %v", sni1, sni1Workers)

		// Get worker IDs for SNI 2 connections
		sni2List := sni2Conns[0].ClientList(ctx).Val()
		sni2Workers := extractWorkerIDsByNamePrefix(sni2List, "sni2-")
		t.Logf("SNI %s connections on workers: %v", sni2, sni2Workers)

		// Both should have worker= field
		require.NotEmpty(t, sni1Workers, "SNI 1 should have worker IDs")
		require.NotEmpty(t, sni2Workers, "SNI 2 should have worker IDs")

		// Each SNI group should stay within fair-share+overdraft range.
		unique1 := countUniqueWorkers(sni1Workers)
		unique2 := countUniqueWorkers(sni2Workers)

		require.LessOrEqual(t, unique1, 5,
			"SNI %s should use <=5 workers (4 fair + 30%% overdraft), got %d", sni1, unique1)
		require.LessOrEqual(t, unique2, 5,
			"SNI %s should use <=5 workers (4 fair + 30%% overdraft), got %d", sni2, unique2)
	})

	t.Run("TLS connection with SNI works", func(t *testing.T) {
		tlsConfig, err := util.TLSConfigWithSNI("localhost", caCertPath)
		require.NoError(t, err)

		rdb := srv.NewClientWithOption(&redis.Options{
			Addr:      srv.TLSAddr(),
			TLSConfig: tlsConfig,
		})
		defer func() { require.NoError(t, rdb.Close()) }()

		// Verify TLS connection works
		require.NoError(t, rdb.Ping(ctx).Err())

		// Verify worker field in CLIENT LIST
		list := rdb.ClientList(ctx).Val()
		require.Contains(t, list, "worker=")
	})
}

func TestFairSchedulerSNIWideDistribution(t *testing.T) {
	sniHosts := []string{"localhost", "test.local"}

	srv, caCertPath := util.StartTLSServerWithSNIs(t, map[string]string{
		"workers":               "8",
		"sni-overdraft-percent": "0", // pure fair-share
	}, sniHosts)
	defer srv.Close()

	ctx := context.Background()

	t.Run("Single SNI spreads without concentration gating", func(t *testing.T) {
		tlsConfig, err := util.TLSConfigWithSNI("localhost", caCertPath)
		require.NoError(t, err)

		// Create 6 connections. Without concentration gating, these should spread.
		conns := make([]*redis.Client, 6)
		for i := 0; i < 6; i++ {
			conns[i] = srv.NewClientWithOption(&redis.Options{
				Addr:      srv.TLSAddr(),
				TLSConfig: tlsConfig,
			})
			require.NoError(t, conns[i].Ping(ctx).Err())
		}
		defer func() {
			for _, c := range conns {
				c.Close()
			}
		}()

		// Check spread
		list := conns[0].ClientList(ctx).Val()
		workerIDs := extractWorkerIDs(list)
		unique := countUniqueWorkers(workerIDs)

		t.Logf("6 connections on %d unique workers: %v", unique, workerIDs)

		require.GreaterOrEqual(t, unique, 5,
			"6 connections should spread to >=5 workers, got %d", unique)
	})
}

// Ensure unused import doesn't cause error
var _ = fmt.Sprintf
var _ tls.Config
