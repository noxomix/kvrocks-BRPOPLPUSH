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

#include "fair_scheduler.h"

#include <algorithm>
#include <functional>

#include "config/config.h"
#include "event_util.h"  // For info(), warn(), etc.
#include "server.h"
#include "time_util.h"
#include "worker.h"

void FairSchedulerConfig::LoadFromConfig(const Config* config) {
  max_workers_percent = static_cast<uint32_t>(config->sni_max_workers_percent);
  overdraft_percent = static_cast<uint32_t>(config->sni_overdraft_percent);
}

FairScheduler::FairScheduler(Server* srv) : srv_(srv) {
  config_.LoadFromConfig(srv_->GetConfig());
}

void FairScheduler::ReloadConfig() {
  config_.LoadFromConfig(srv_->GetConfig());
}

void FairScheduler::OnSNIBecameActive() {
  active_sni_count_.fetch_add(1, std::memory_order_relaxed);
}

void FairScheduler::OnSNIBecameInactive() {
  uint32_t prev = active_sni_count_.load(std::memory_order_relaxed);
  while (prev > 0) {
    if (active_sni_count_.compare_exchange_weak(prev, prev - 1, std::memory_order_relaxed)) {
      break;
    }
  }
}

uint32_t FairScheduler::GetMaxWorkerCount(uint32_t total_workers, uint32_t active_snis) const {
  if (total_workers == 0) return 0;
  uint32_t fair_count = GetFairWorkerCount(total_workers, active_snis);
  uint32_t max_with_overdraft = fair_count + (fair_count * config_.overdraft_percent / 100);
  max_with_overdraft = std::max(1u, max_with_overdraft);
  return std::min(max_with_overdraft, total_workers);
}

uint32_t FairScheduler::GetFairWorkerCount(uint32_t total_workers, uint32_t active_snis) const {
  if (total_workers == 0) return 0;
  uint32_t fair_share = total_workers / std::max(1u, active_snis);
  fair_share = std::max(1u, fair_share);

  if (config_.max_workers_percent == 0) return fair_share;

  uint32_t cap = total_workers * config_.max_workers_percent / 100;
  cap = std::max(1u, cap);
  return std::min(fair_share, cap);
}

std::shared_ptr<SNIState> FairScheduler::GetOrCreateState(const std::string& sni) {
  auto& shard = sni_shards_[std::hash<std::string>{}(sni) % sni_shards_.size()];
  {
    std::shared_lock lock(shard.mu);
    auto it = shard.map.find(sni);
    if (it != shard.map.end()) {
      return it->second;
    }
  }

  std::unique_lock lock(shard.mu);
  auto [it, inserted] = shard.map.try_emplace(sni, std::make_shared<SNIState>(sni));
  (void)inserted;
  return it->second;
}

void FairScheduler::RefreshPreferredWorkers(SNIState* state, uint32_t total_workers, uint32_t active_snis) {
  if (total_workers == 0) return;

  uint32_t desired_size = GetMaxWorkerCount(total_workers, active_snis);
  auto current = std::atomic_load_explicit(&state->preferred_workers, std::memory_order_acquire);
  uint32_t last_total_workers = state->preferred_total_workers.load(std::memory_order_relaxed);
  if (current && current->size() == desired_size && last_total_workers == total_workers) {
    return;
  }

  uint32_t start = static_cast<uint32_t>(std::hash<std::string>{}(state->sni) % total_workers);
  auto rebuilt = std::make_shared<std::vector<uint32_t>>();
  rebuilt->reserve(desired_size);
  for (uint32_t i = 0; i < desired_size; i++) {
    rebuilt->push_back((start + i) % total_workers);
  }
  state->preferred_total_workers.store(total_workers, std::memory_order_relaxed);
  std::atomic_store_explicit(&state->preferred_workers,
                             std::static_pointer_cast<const std::vector<uint32_t>>(std::move(rebuilt)),
                             std::memory_order_release);
}

std::shared_ptr<Worker> FairScheduler::SelectFromPreferred(
    SNIState* state, uint32_t fair_count,
    const std::shared_ptr<std::vector<std::shared_ptr<Worker>>>& snapshot) {
  if (!snapshot || snapshot->empty()) return nullptr;
  auto preferred = std::atomic_load_explicit(&state->preferred_workers, std::memory_order_acquire);
  if (!preferred || preferred->empty()) return nullptr;
  uint32_t preferred_size = static_cast<uint32_t>(preferred->size());

  // First pass: fair-share workers
  uint32_t first_pass_count = std::min(fair_count, preferred_size);
  first_pass_count = std::max(1u, first_pass_count);

  // Round-robin over fair-share workers
  uint32_t idx = state->next_worker_idx.fetch_add(1, std::memory_order_relaxed);

  for (uint32_t i = 0; i < first_pass_count; i++) {
    uint32_t worker_idx = (idx + i) % first_pass_count;
    uint32_t worker_id = (*preferred)[worker_idx];
    if (worker_id < snapshot->size()) {
      auto& worker = (*snapshot)[worker_id];
      if (worker->IsAccepting() && !worker->IsLuaScriptRunning()) {
        return worker;
      }
    }
  }

  // Second pass: overdraft range (if available), still round-robin with offset
  for (uint32_t i = 0; i < preferred_size - first_pass_count; i++) {
    uint32_t worker_idx = first_pass_count + ((idx + i) % (preferred_size - first_pass_count));
    uint32_t worker_id = (*preferred)[worker_idx];
    if (worker_id < snapshot->size()) {
      auto& worker = (*snapshot)[worker_id];
      if (worker->IsAccepting() && !worker->IsLuaScriptRunning()) {
        return worker;
      }
    }
  }

  return nullptr;
}

std::shared_ptr<Worker> FairScheduler::SelectAnyAccepting(
    const std::shared_ptr<std::vector<std::shared_ptr<Worker>>>& snapshot) {
  if (!snapshot || snapshot->empty()) return nullptr;

  uint32_t size = static_cast<uint32_t>(snapshot->size());
  // Round-robin fallback (not always starting from 0)
  uint32_t start = next_fallback_worker_.fetch_add(1, std::memory_order_relaxed) % size;

  // First pass: non-Lua-blocked workers
  for (uint32_t i = 0; i < size; i++) {
    auto& worker = (*snapshot)[(start + i) % size];
    if (worker->IsAccepting() && !worker->IsLuaScriptRunning()) {
      return worker;
    }
  }

  // Last fallback: any accepting worker (even if running Lua)
  for (uint32_t i = 0; i < size; i++) {
    auto& worker = (*snapshot)[(start + i) % size];
    if (worker->IsAccepting()) {
      return worker;
    }
  }

  return nullptr;
}

std::shared_ptr<Worker> FairScheduler::SelectWorker(const std::string& sni) {
  // Load snapshot once and pass it through (avoid multiple RCU loads)
  auto snapshot = srv_->GetWorkerSnapshot();
  if (!snapshot || snapshot->empty()) {
    warn("[FairScheduler] SelectWorker: No snapshot or empty!");
    return nullptr;
  }

  if (sni.empty()) {
    return SelectAnyAccepting(snapshot);
  }

  uint32_t total_workers = static_cast<uint32_t>(snapshot->size());
  uint32_t active_snis = GetActiveSNICount();

  auto state = GetOrCreateState(sni);
  if (!state) return nullptr;

  // Track if this connection makes the SNI active
  uint32_t prev_conns = state->active_connections.fetch_add(1, std::memory_order_relaxed);
  if (prev_conns == 0) {
    OnSNIBecameActive();
    active_snis = active_snis + 1;
  }
  state->last_activity_ms.store(util::GetTimeStampMS(), std::memory_order_relaxed);

  // Rebuild preferred worker list lazily when active SNI count or worker count changed
  RefreshPreferredWorkers(state.get(), total_workers, active_snis);

  // Fair-share worker count (no concentration gating)
  uint32_t fair_count = GetFairWorkerCount(total_workers, active_snis);

  // Try to select from preferred workers
  auto worker = SelectFromPreferred(state.get(), fair_count, snapshot);
  if (worker) {
    return worker;
  }

  // Fallback: any accepting worker (does NOT modify preferred_workers)
  worker = SelectAnyAccepting(snapshot);
  if (worker) {
    return worker;
  }

  // Roll back counters if we couldn't select any worker
  warn("[FairScheduler] No worker available!");
  OnConnectionClosed(sni);
  return nullptr;
}

void FairScheduler::OnConnectionClosed(const std::string& sni) {
  if (sni.empty()) return;

  std::shared_ptr<SNIState> state;
  {
    auto& shard = sni_shards_[std::hash<std::string>{}(sni) % sni_shards_.size()];
    std::shared_lock lock(shard.mu);
    auto it = shard.map.find(sni);
    if (it != shard.map.end()) state = it->second;
  }
  if (!state) return;

  // Safe decrement with underflow protection
  uint32_t prev = state->active_connections.load(std::memory_order_relaxed);
  while (prev > 0) {
    if (state->active_connections.compare_exchange_weak(prev, prev - 1, std::memory_order_relaxed)) {
      // Check if this was the last connection
      if (prev == 1) {
        OnSNIBecameInactive();
      }
      break;
    }
  }
}

void FairScheduler::CleanupInactiveSNIs() {
  uint64_t now = util::GetTimeStampMS();
  constexpr uint64_t cleanup_timeout_ms = 300000;  // 5 minutes

  for (auto& shard : sni_shards_) {
    std::unique_lock lock(shard.mu);
    for (auto it = shard.map.begin(); it != shard.map.end();) {
      auto& state = it->second;
      bool is_dead = state->active_connections.load(std::memory_order_relaxed) == 0 &&
                     (now - state->last_activity_ms.load(std::memory_order_relaxed)) > cleanup_timeout_ms;
      if (is_dead) {
        it = shard.map.erase(it);
      } else {
        ++it;
      }
    }
  }
}

std::vector<SNIStats> FairScheduler::GetSNIStats() const {
  std::vector<SNIStats> result;

  auto snapshot = srv_->GetWorkerSnapshot();
  uint32_t total_workers = snapshot ? static_cast<uint32_t>(snapshot->size()) : 0;
  uint32_t active_snis = GetActiveSNICount();

  for (const auto& shard : sni_shards_) {
    std::shared_lock lock(shard.mu);
    result.reserve(result.size() + shard.map.size());
    for (const auto& [sni, state] : shard.map) {
      SNIStats stats;
      stats.sni = sni;
      stats.active_connections = state->active_connections.load(std::memory_order_relaxed);
      stats.fair_share = GetFairWorkerCount(total_workers, active_snis);
      stats.overdraft_limit = GetMaxWorkerCount(total_workers, active_snis);
      auto preferred = std::atomic_load_explicit(&state->preferred_workers, std::memory_order_acquire);
      stats.target_pool_size = preferred ? static_cast<uint32_t>(preferred->size()) : 0;
      result.push_back(std::move(stats));
    }
  }

  return result;
}
