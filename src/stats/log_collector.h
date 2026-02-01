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

#include <sys/types.h>
#include <time.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "spdlog/common.h"

class SlowEntry {
 public:
  uint64_t id;
  time_t time;
  uint64_t duration;
  std::vector<std::string> args;
  std::string client_name;
  std::string ip;
  uint32_t port;
  std::string ns;  // Namespace for tenant isolation
  std::string ToRedisString() const;
  void DumpToLogFile(spdlog::level::level_enum) const;
};

class PerfEntry {
 public:
  uint64_t id;
  time_t time;
  uint64_t duration;
  std::string cmd_name;
  std::string perf_context;
  std::string iostats_context;
  std::string ns;  // Namespace for tenant isolation

  std::string ToRedisString() const;
  void DumpToLogFile(spdlog::level::level_enum) const {};
};

// Per-namespace log data for tenant isolation
template <class T>
struct NamespaceLogData {
  std::mutex mu_;  // Per-namespace mutex - tenants don't block each other
  std::deque<std::unique_ptr<T>> entries_;
  uint64_t id_ = 0;
};

template <class T>
class LogCollector {
 public:
  LogCollector() = default;
  LogCollector(const LogCollector &) = delete;
  LogCollector &operator=(const LogCollector &) = delete;
  ~LogCollector();

  // Global methods (for admin - aggregates all namespaces)
  ssize_t Size();
  void Reset();
  std::string GetLatestEntries(int64_t cnt);

  // Config methods (global settings)
  void SetMaxEntries(int64_t max_entries);
  void SetDumpToLogfileLevel(spdlog::level::level_enum level);

  // Entry insertion (uses entry->ns for routing)
  void PushEntry(std::unique_ptr<T> &&entry);

  // Namespace-aware filter methods for tenant isolation
  ssize_t SizeWithFilter(const std::function<bool(const T &)> &filter);
  void ResetWithFilter(const std::function<bool(const T &)> &filter);
  std::string GetLatestEntriesWithFilter(int64_t cnt, const std::function<bool(const T &)> &filter);

 private:
  // Get or create namespace-specific log data
  NamespaceLogData<T> *GetOrCreateNsLog(const std::string &ns);

  // Map lock: shared_lock for lookup, unique_lock for create
  mutable std::shared_mutex ns_map_mu_;
  std::unordered_map<std::string, std::unique_ptr<NamespaceLogData<T>>> ns_logs_;

  // Global config (shared across all namespaces)
  std::atomic<int64_t> max_entries_per_ns_{128};
  std::atomic<spdlog::level::level_enum> dump_to_logfile_level_{spdlog::level::off};
};
