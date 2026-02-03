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
 *
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

class Server;
class Worker;
struct Config;

// Configuration for SNI-based fair scheduling
struct FairSchedulerConfig {
  uint32_t connections_per_worker = 10;  // Concentration threshold
  uint32_t min_workers = 2;              // Minimum workers per SNI
  uint32_t max_workers_percent = 0;      // 0 = auto (workers/active_snis)
  uint32_t overdraft_percent = 30;       // Burst allowance

  void LoadFromConfig(const Config* config);
};

// Per-SNI scheduling state
struct SNIState {
  std::string sni;
  std::atomic<uint32_t> active_connections{0};
  std::atomic<uint32_t> next_worker_idx{0};  // Lock-free round-robin
  std::vector<uint32_t> preferred_workers;   // Worker affinity
  std::atomic<uint64_t> last_activity_ms{0};
  mutable std::mutex mu;  // Only for preferred_workers updates

  explicit SNIState(std::string sni_name) : sni(std::move(sni_name)) {}
};

// Statistics for monitoring
struct SNIStats {
  std::string sni;
  uint32_t active_connections = 0;
  uint32_t preferred_worker_count = 0;
  uint32_t target_worker_count = 0;
};

// SNI-based fair scheduler for worker selection
// Thread-safe: called from Acceptor threads (SelectWorker) and Worker threads (OnConnectionClosed)
class FairScheduler {
 public:
  explicit FairScheduler(Server* srv);

  // Select worker for new connection with given SNI
  // Called from Acceptor thread - must be fast and thread-safe
  std::shared_ptr<Worker> SelectWorker(const std::string& sni);

  // Called when connection is closed
  // Called from Worker thread
  void OnConnectionClosed(const std::string& sni);

  // Periodic cleanup of inactive SNIs (call from cron)
  void CleanupInactiveSNIs();

  // Get statistics for monitoring
  std::vector<SNIStats> GetSNIStats() const;

  // Get count of active SNIs (with connections > 0) - O(1) via cached counter
  uint32_t GetActiveSNICount() const { return std::max(1u, active_sni_count_.load(std::memory_order_relaxed)); }

  // Reload config (e.g., after CONFIG SET)
  void ReloadConfig();

 private:
  Server* srv_;
  FairSchedulerConfig config_;

  mutable std::shared_mutex sni_states_mu_;
  std::unordered_map<std::string, std::shared_ptr<SNIState>> sni_states_;

  // Cached active SNI count (updated on state changes) - O(1) instead of O(n)
  std::atomic<uint32_t> active_sni_count_{0};

  // Global round-robin counter for fallback selection
  std::atomic<uint32_t> next_fallback_worker_{0};

  // Get or create state for SNI (thread-safe)
  std::shared_ptr<SNIState> GetOrCreateState(const std::string& sni);

  // Calculate target worker count for SNI based on config
  uint32_t GetTargetWorkerCount(const SNIState* state, uint32_t total_workers, uint32_t active_snis) const;

  // Select worker from preferred list
  std::shared_ptr<Worker> SelectFromPreferred(
      SNIState* state, uint32_t target_count,
      const std::shared_ptr<std::vector<std::shared_ptr<Worker>>>& snapshot);

  // Fallback: select any accepting worker (does NOT modify preferred_workers)
  std::shared_ptr<Worker> SelectAnyAccepting(
      const std::shared_ptr<std::vector<std::shared_ptr<Worker>>>& snapshot);

  // Assign initial preferred workers to new SNI
  void AssignPreferredWorkers(SNIState* state, uint32_t total_workers, uint32_t active_snis);

  // Update active_sni_count_ when a connection becomes first/last for an SNI
  void OnSNIBecameActive();
  void OnSNIBecameInactive();
};
