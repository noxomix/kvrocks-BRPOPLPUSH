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

#include "log_collector.h"

#include <algorithm>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "server/redis_reply.h"
#include "time_util.h"

std::string SlowEntry::ToRedisString() const {
  std::string output;
  output.append(redis::MultiLen(6));
  output.append(redis::Integer(id));
  output.append(redis::Integer(time));
  output.append(redis::Integer(duration));
  output.append(redis::ArrayOfBulkStrings(args));
  output.append(redis::BulkString(ip + ":" + std::to_string(port)));
  output.append(redis::BulkString(client_name));
  return output;
}

void SlowEntry::DumpToLogFile(spdlog::level::level_enum level) const {
  if (level == spdlog::level::off) {
    return;
  }

  std::string cmd;
  if (args.size() > 0) {
    for (const auto &arg : args) {
      cmd.append(arg).append(" ");
    }
    cmd.pop_back();
  }
  log(level, "[slowlog] id: {}, timestamp: {}, duration: {}, cmd: {}, ip: {}, port: {}, client_name: {}", id, time,
      duration, cmd, ip, port, client_name);
}

std::string PerfEntry::ToRedisString() const {
  std::string output;
  output.append(redis::MultiLen(6));
  output.append(redis::Integer(id));
  output.append(redis::Integer(time));
  output.append(redis::BulkString(cmd_name));
  output.append(redis::Integer(duration));
  output.append(redis::BulkString(perf_context));
  output.append(redis::BulkString(iostats_context));
  return output;
}

// GetOrCreateNsLog: Fast path with shared_lock, slow path with unique_lock
// Pattern from Konstitution Zeile 102-105
template <class T>
NamespaceLogData<T> *LogCollector<T>::GetOrCreateNsLog(const std::string &ns) {
  // Fast path: shared_lock for lookup (most common case)
  {
    std::shared_lock<std::shared_mutex> read_lock(ns_map_mu_);
    auto it = ns_logs_.find(ns);
    if (it != ns_logs_.end()) {
      return it->second.get();
    }
  }

  // Slow path: unique_lock for create (rare)
  std::unique_lock<std::shared_mutex> write_lock(ns_map_mu_);
  // Double-check after lock upgrade (another thread might have created it)
  auto it = ns_logs_.find(ns);
  if (it != ns_logs_.end()) {
    return it->second.get();
  }

  auto [inserted_it, _] = ns_logs_.emplace(ns, std::make_unique<NamespaceLogData<T>>());
  return inserted_it->second.get();
}

template <class T>
LogCollector<T>::~LogCollector() {
  Reset();
}

// Size: Sum of all namespace sizes (for admin)
template <class T>
ssize_t LogCollector<T>::Size() {
  std::shared_lock<std::shared_mutex> map_lock(ns_map_mu_);
  ssize_t total = 0;
  for (const auto &[ns, ns_log] : ns_logs_) {
    std::lock_guard<std::mutex> ns_lock(ns_log->mu_);
    total += static_cast<ssize_t>(ns_log->entries_.size());
  }
  return total;
}

// Reset: Clear all namespaces (for admin)
template <class T>
void LogCollector<T>::Reset() {
  std::unique_lock<std::shared_mutex> map_lock(ns_map_mu_);
  for (auto &[ns, ns_log] : ns_logs_) {
    std::lock_guard<std::mutex> ns_lock(ns_log->mu_);
    ns_log->entries_.clear();
  }
}

// SetMaxEntries: Update global config, then trim all namespaces
template <class T>
void LogCollector<T>::SetMaxEntries(int64_t max_entries) {
  max_entries_per_ns_.store(max_entries);

  // Trim all existing namespaces
  std::shared_lock<std::shared_mutex> map_lock(ns_map_mu_);
  for (auto &[ns, ns_log] : ns_logs_) {
    std::lock_guard<std::mutex> ns_lock(ns_log->mu_);
    while (max_entries > 0 && static_cast<int64_t>(ns_log->entries_.size()) > max_entries) {
      ns_log->entries_.pop_back();
    }
  }
}

// SetDumpToLogfileLevel: Atomic update (no lock needed)
template <class T>
void LogCollector<T>::SetDumpToLogfileLevel(spdlog::level::level_enum level) {
  dump_to_logfile_level_.store(level);
}

// PushEntry: Route to namespace-specific deque
// Pattern: Copy-then-Log (Konstitution Zeile 77-82)
template <class T>
void LogCollector<T>::PushEntry(std::unique_ptr<T> &&entry) {
  const std::string &ns = entry->ns;
  NamespaceLogData<T> *ns_log = GetOrCreateNsLog(ns);

  spdlog::level::level_enum dump_level = dump_to_logfile_level_.load();
  int64_t max_entries = max_entries_per_ns_.load();
  std::optional<T> entry_copy;

  {
    std::lock_guard<std::mutex> ns_lock(ns_log->mu_);  // Only lock own namespace!
    entry->id = ++(ns_log->id_);
    entry->time = util::GetTimeStamp();

    // Copy entry for logging BEFORE move (only if logging enabled)
    if (dump_level != spdlog::level::off) {
      entry_copy.emplace(*entry);
    }

    // Evict oldest if at capacity
    if (max_entries > 0 && !ns_log->entries_.empty() &&
        ns_log->entries_.size() >= static_cast<size_t>(max_entries)) {
      ns_log->entries_.pop_back();
    }
    ns_log->entries_.push_front(std::move(entry));
  }  // NS lock released here

  // Log OUTSIDE lock - no blocking for other threads during disk I/O
  if (entry_copy) {
    entry_copy->DumpToLogFile(dump_level);
  }
}

// GetLatestEntries: Merge all namespaces sorted by time (for admin)
// Pattern: Copy-then-Reply (Konstitution Zeile 77-82) - copy data under lock, then process
template <class T>
std::string LogCollector<T>::GetLatestEntries(int64_t cnt) {
  std::vector<T> all_entries;  // COPIES, not pointers - safe after lock release

  {
    std::shared_lock<std::shared_mutex> map_lock(ns_map_mu_);
    for (const auto &[ns, ns_log] : ns_logs_) {
      std::lock_guard<std::mutex> ns_lock(ns_log->mu_);
      for (const auto &entry : ns_log->entries_) {
        all_entries.push_back(*entry);  // Copy under lock
      }
    }
  }

  // Sort by time descending (newest first) - safe, working on copies
  std::sort(all_entries.begin(), all_entries.end(),
            [](const T &a, const T &b) { return a.time > b.time; });

  size_t n = (cnt > 0) ? std::min(all_entries.size(), static_cast<size_t>(cnt)) : all_entries.size();

  std::string output;
  output.append(redis::MultiLen(n));
  for (size_t i = 0; i < n; i++) {
    output.append(all_entries[i].ToRedisString());
  }
  return output;
}

// SizeWithFilter: Count entries matching filter across all namespaces
template <class T>
ssize_t LogCollector<T>::SizeWithFilter(const std::function<bool(const T &)> &filter) {
  std::shared_lock<std::shared_mutex> map_lock(ns_map_mu_);

  if (!filter) {
    ssize_t total = 0;
    for (const auto &[ns, ns_log] : ns_logs_) {
      std::lock_guard<std::mutex> ns_lock(ns_log->mu_);
      total += static_cast<ssize_t>(ns_log->entries_.size());
    }
    return total;
  }

  ssize_t count = 0;
  for (const auto &[ns, ns_log] : ns_logs_) {
    std::lock_guard<std::mutex> ns_lock(ns_log->mu_);
    for (const auto &entry : ns_log->entries_) {
      if (filter(*entry)) count++;
    }
  }
  return count;
}

// ResetWithFilter: Remove entries matching filter across all namespaces
template <class T>
void LogCollector<T>::ResetWithFilter(const std::function<bool(const T &)> &filter) {
  std::shared_lock<std::shared_mutex> map_lock(ns_map_mu_);

  if (!filter) {
    // No filter = clear all (same as Reset but with shared_lock on map)
    for (auto &[ns, ns_log] : ns_logs_) {
      std::lock_guard<std::mutex> ns_lock(ns_log->mu_);
      ns_log->entries_.clear();
    }
    return;
  }

  for (auto &[ns, ns_log] : ns_logs_) {
    std::lock_guard<std::mutex> ns_lock(ns_log->mu_);
    ns_log->entries_.erase(
        std::remove_if(ns_log->entries_.begin(), ns_log->entries_.end(),
                       [&filter](const auto &entry) { return filter(*entry); }),
        ns_log->entries_.end());
  }
}

// GetLatestEntriesWithFilter: Get entries matching filter, sorted by time
// Pattern: Copy-then-Reply (Konstitution Zeile 77-82) - copy data under lock, then process
template <class T>
std::string LogCollector<T>::GetLatestEntriesWithFilter(int64_t cnt, const std::function<bool(const T &)> &filter) {
  std::vector<T> filtered;  // COPIES, not pointers - safe after lock release

  {
    std::shared_lock<std::shared_mutex> map_lock(ns_map_mu_);
    for (const auto &[ns, ns_log] : ns_logs_) {
      std::lock_guard<std::mutex> ns_lock(ns_log->mu_);
      for (const auto &entry : ns_log->entries_) {
        if (!filter || filter(*entry)) {
          filtered.push_back(*entry);  // Copy under lock
        }
      }
    }
  }

  // Sort by time descending (newest first) - safe, working on copies
  std::sort(filtered.begin(), filtered.end(),
            [](const T &a, const T &b) { return a.time > b.time; });

  size_t n = (cnt > 0) ? std::min(filtered.size(), static_cast<size_t>(cnt)) : filtered.size();

  std::string output;
  output.append(redis::MultiLen(n));
  for (size_t i = 0; i < n; i++) {
    output.append(filtered[i].ToRedisString());
  }
  return output;
}

template class LogCollector<SlowEntry>;
template class LogCollector<PerfEntry>;
