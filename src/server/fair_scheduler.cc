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
  connections_per_worker = static_cast<uint32_t>(config->sni_connections_per_worker);
  min_workers = static_cast<uint32_t>(config->sni_min_workers);
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
  uint32_t fair_share = total_workers / std::max(1u, active_snis);
  if (fair_share == 0) fair_share = 1;

  // Max workers (config or fair share)
  uint32_t max_workers = (config_.max_workers_percent > 0)
                             ? (total_workers * config_.max_workers_percent / 100)
                             : fair_share;

  // Overdraft allowance
  uint32_t max_with_overdraft = fair_share + (fair_share * config_.overdraft_percent / 100);
  max_workers = std::min(max_workers, max_with_overdraft);
  max_workers = std::min(max_workers, total_workers);

  uint32_t effective_min = std::min(config_.min_workers, total_workers);
  if (max_workers < effective_min) max_workers = effective_min;
  return max_workers;
}

uint32_t FairScheduler::GetTargetWorkerCount(const SNIState* state, uint32_t total_workers,
                                             uint32_t active_snis) const {
  if (total_workers == 0) return config_.min_workers;

  uint32_t conns = state->active_connections.load(std::memory_order_relaxed);
  uint32_t max_workers = GetMaxWorkerCount(total_workers, active_snis);
  uint32_t effective_min = std::min(config_.min_workers, total_workers);

  // Concentration: how many workers needed for current load?
  uint32_t needed_for_load = (conns + config_.connections_per_worker - 1) / config_.connections_per_worker;
  if (needed_for_load == 0) needed_for_load = 1;

  // Clamp between min and max
  return std::clamp(needed_for_load, effective_min, max_workers);
}

std::shared_ptr<SNIState> FairScheduler::GetOrCreateState(const std::string& sni) {
  // Fast path: shared lock for lookup
  {
    std::shared_lock lock(sni_states_mu_);
    auto it = sni_states_.find(sni);
    if (it != sni_states_.end()) {
      return it->second;
    }
  }

  // Slow path: unique lock for insert
  std::unique_lock lock(sni_states_mu_);
  // Double-check after acquiring unique lock
  auto [it, inserted] = sni_states_.try_emplace(sni, std::make_shared<SNIState>(sni));
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
    SNIState* state, uint32_t target_count,
    const std::shared_ptr<std::vector<std::shared_ptr<Worker>>>& snapshot) {
  if (!snapshot || snapshot->empty()) return nullptr;
  auto preferred = std::atomic_load_explicit(&state->preferred_workers, std::memory_order_acquire);
  if (!preferred || preferred->empty()) return nullptr;
  uint32_t preferred_size = static_cast<uint32_t>(preferred->size());

  // Limit to target_count
  uint32_t search_count = std::min(target_count, preferred_size);
  if (search_count == 0) return nullptr;

  // Round-robin within target_count workers
  uint32_t idx = state->next_worker_idx.fetch_add(1, std::memory_order_relaxed);

  // Try preferred workers (within target_count)
  for (uint32_t i = 0; i < search_count; i++) {
    uint32_t worker_idx = (idx + i) % search_count;
    uint32_t worker_id = (*preferred)[worker_idx];
    if (worker_id < snapshot->size()) {
      auto& worker = (*snapshot)[worker_id];
      if (worker->IsAccepting() && !worker->IsLuaScriptRunning()) {
        return worker;
      }
    }
  }

  // All target workers Lua-blocked, try remaining preferred workers (overdraft)
  for (uint32_t i = search_count; i < preferred_size; i++) {
    uint32_t worker_id = (*preferred)[i];
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

  // Calculate target worker count based on config
  uint32_t target_count = GetTargetWorkerCount(state.get(), total_workers, active_snis);

  // Try to select from preferred workers
  auto worker = SelectFromPreferred(state.get(), target_count, snapshot);
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
    std::shared_lock lock(sni_states_mu_);
    auto it = sni_states_.find(sni);
    if (it != sni_states_.end()) {
      state = it->second;
    }
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

  std::unique_lock lock(sni_states_mu_);
  for (auto it = sni_states_.begin(); it != sni_states_.end();) {
    auto& state = it->second;
    bool is_dead = state->active_connections.load(std::memory_order_relaxed) == 0 &&
                   (now - state->last_activity_ms.load(std::memory_order_relaxed)) > cleanup_timeout_ms;
    if (is_dead) {
      it = sni_states_.erase(it);
    } else {
      ++it;
    }
  }
}

std::vector<SNIStats> FairScheduler::GetSNIStats() const {
  std::vector<SNIStats> result;

  auto snapshot = srv_->GetWorkerSnapshot();
  uint32_t total_workers = snapshot ? static_cast<uint32_t>(snapshot->size()) : 0;
  uint32_t active_snis = GetActiveSNICount();

  std::shared_lock lock(sni_states_mu_);
  result.reserve(sni_states_.size());

  for (const auto& [sni, state] : sni_states_) {
    SNIStats stats;
    stats.sni = sni;
    stats.active_connections = state->active_connections.load(std::memory_order_relaxed);
    auto preferred = std::atomic_load_explicit(&state->preferred_workers, std::memory_order_acquire);
    stats.preferred_worker_count = preferred ? static_cast<uint32_t>(preferred->size()) : 0;
    // Now GetTargetWorkerCount doesn't acquire sni_states_mu_ (no deadlock)
    stats.target_worker_count = GetTargetWorkerCount(state.get(), total_workers, active_snis);
    result.push_back(std::move(stats));
  }

  return result;
}
