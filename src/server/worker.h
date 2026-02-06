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

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <event2/util.h>

#include <cstdint>
#include <cstring>
#include <lua.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "config/config.h"
#include "stats/stats.h"
#include "event_util.h"
#include "redis_connection.h"
#include "server/watched_keys_update.h"

class Server;
namespace rocksdb {
class WriteBatchWithIndex;
}  // namespace rocksdb

struct ClientCounts {
  int connected = 0;
  int monitor = 0;
};

// Connection dispatched from Acceptor thread to Worker
// Extensible for future features (SNI routing, peer address, etc.)
struct PendingConnection {
  int fd;
  bool is_tls;
  std::string sni;  // SNI hostname or peer IP for fair scheduling
};

enum class WorkerState : uint8_t {
  kRunning = 0,
  kStopping = 1,
  kStopped = 2,
};

class Worker : EventCallbackBase<Worker>, EvconnlistenerBase<Worker> {
 public:
  Worker(Server *srv, Config *config);
  ~Worker();
  Worker(const Worker &) = delete;
  Worker(Worker &&) = delete;
  Worker &operator=(const Worker &) = delete;

  void Stop(uint32_t wait_seconds);
  void Run(std::thread::id tid);
  bool IsTerminated() const { return is_terminated_; }
  bool IsAccepting() const { return state_.load(std::memory_order_acquire) == WorkerState::kRunning; }
  void StopAccepting() { state_.store(WorkerState::kStopping, std::memory_order_release); }

  // Worker index for debugging and CLIENT LIST
  uint32_t GetIndex() const { return index_; }
  void SetIndex(uint32_t idx) { index_ = idx; }

  void MigrateConnection(Worker *target, redis::Connection *conn);
  void DetachConnection(redis::Connection *conn);
  void FreeConnection(redis::Connection *conn);
  void FreeConnectionByID(int fd, uint64_t id);
  Status AddConnection(redis::Connection *c);
  Status EnableWriteEvent(int fd);
  Status EnableWriteEventByID(int fd, uint64_t id);
  Status Reply(int fd, const std::string &reply);
  Status ReplyByID(int fd, uint64_t id, const std::string &reply);
  void BecomeMonitorConn(redis::Connection *conn);
  void QuitMonitorConn(redis::Connection *conn);

  std::string GetClientsStr(redis::Connection *self);
  void KillClient(redis::Connection *self, uint64_t id, const std::string &addr, uint64_t type, bool skipme,
                  int64_t *killed);
  ClientCounts GetClientCounts(redis::Connection *self);
  void KickoutIdleClients(int timeout);

  Status ListenUnixSocket(const std::string &path, int perm, int backlog);

  // Accept-Dispatch: Called by Acceptor thread to dispatch a new connection to this Worker
  // Thread-safe: Uses mutex + eventfd to wake up Worker's event loop
  void DispatchConnection(PendingConnection conn);

  void TimerCB(int, int16_t events);
  void OnBatchTimer(int, int16_t events);
  bool IsBatchReplyDeferralActive() const { return batch_state_.active; }
  bool HasBatchContextForNamespace(const std::string &ns) const {
    return batch_state_.txn_active && batch_state_.active_ns == ns;
  }
  Status EnsureBatchContext(const std::string &ns);
  void CloseIdleBatchContext();
  bool OnBatchWrite(size_t estimated_bytes);
  void EnqueueBatchWatchUpdate(bool mark_all_keys, std::vector<std::string> keys);
  void EnqueueBatchReply(int fd, uint64_t conn_id, std::string reply);
  void FlushBatchReplies();

  lua_State *Lua() { return lua_; }
  void LuaReset();
  void LuaResetNamespace(const std::string &ns);
  void MarkNamespaceForReset(const std::string &ns);
  void CheckAndResetIfNeeded(const std::string &ns);
  int64_t GetLuaMemorySize();

  // Lua script timeout support
  void StartLuaScript(const std::string &ns, uint64_t start_ms) {
    {
      std::lock_guard<std::mutex> guard(lua_script_mu_);
      lua_script_ns_ = ns;
    }
    lua_script_start_ms_.store(start_ms, std::memory_order_relaxed);
    lua_script_kill_requested_.store(false, std::memory_order_relaxed);
    lua_script_running_.store(true, std::memory_order_release);
  }
  void StopLuaScript() {
    lua_script_running_.store(false, std::memory_order_release);
    std::lock_guard<std::mutex> guard(lua_script_mu_);
    lua_script_ns_.clear();
  }
  bool IsLuaScriptRunning() const { return lua_script_running_.load(std::memory_order_acquire); }
  bool IsLuaScriptKillRequested() const { return lua_script_kill_requested_.load(std::memory_order_acquire); }
  uint64_t GetLuaScriptStartMs() const { return lua_script_start_ms_.load(std::memory_order_relaxed); }
  std::string GetLuaScriptNs() const {
    std::lock_guard<std::mutex> guard(lua_script_mu_);
    return lua_script_ns_;
  }
  void RequestLuaScriptKill() { lua_script_kill_requested_.store(true, std::memory_order_release); }

  std::map<int, redis::Connection *> GetConnectionsSnapshot();
  Server *srv;

  // Per-namespace stats (with per-worker mutex for thread-safety)
  void IncrCallsForNamespace(const std::string &ns);
  void IncrInboundBytesForNamespace(const std::string &ns, uint64_t bytes);
  void IncrOutboundBytesForNamespace(const std::string &ns, uint64_t bytes);
  void IncrConnectionsForNamespace(const std::string &ns);
  std::unordered_map<std::string, NamespaceStatsSnapshot> GetNamespaceStatsSnapshot() const;
  NamespaceStatsSnapshot GetNamespaceStats(const std::string &ns) const;

  // Per-namespace command stats (with noisy-neighbor prevention)
  void IncrCommandStatForNamespace(const std::string &ns, const std::string &cmd, uint64_t latency);
  std::map<std::string, CommandStatSnapshot> GetCommandStatsForNamespace(const std::string &ns) const;

 private:
  Status listenFD(int fd, uint32_t expected_port, int backlog);
  Status listenTCP(const std::string &host, uint32_t port, int backlog);
  void newTCPConnection(evconnlistener *listener, evutil_socket_t fd, sockaddr *address, int socklen);
  void newUnixSocketConnection(evconnlistener *listener, evutil_socket_t fd, sockaddr *address, int socklen);
  redis::Connection *removeConnection(int fd);

  // Accept-Dispatch: eventfd callback when Acceptor dispatches connections
  void onDispatchEvent(int fd, int16_t events);
  // Create connection from dispatched fd (refactored from newTCPConnection)
  void createConnectionFromDispatch(const PendingConnection &pc);
  void DrainPendingConnections();

  enum class BatchFlushReason : uint8_t { kExplicit, kTimer };
  Status BatchEnsureContext(const std::string &ns);
  bool BatchOnWrite(size_t estimated_bytes, const Config::RuntimeConfigSnapshot &config);
  void BatchCloseIdleContext();
  void BatchFlushInternal(BatchFlushReason reason);
  void BatchDisarmTimer();
  void BatchResetState();

  event_base *base_;
  UniqueEvent timer_;
  UniqueEvent batch_timer_;
  std::thread::id tid_;
  std::vector<evconnlistener *> listen_events_;
  std::mutex conns_mu_;
  std::map<int, redis::Connection *> conns_;
  int last_iter_conn_fd_ = 0;  // fd of last processed connection in previous cron

  struct bufferevent_rate_limit_group *rate_limit_group_ = nullptr;
  struct ev_token_bucket_cfg *rate_limit_group_cfg_ = nullptr;
  std::atomic<lua_State *> lua_;
  std::atomic<bool> is_terminated_ = false;
  std::atomic<WorkerState> state_{WorkerState::kRunning};
  uint32_t index_ = 0;  // Worker index in server's worker array (for CLIENT LIST)

  struct BatchState {
    struct DeferredReply {
      int fd = -1;
      uint64_t conn_id = 0;
      std::string reply;
    };

    bool txn_active = false;
    std::string active_ns;
    std::unique_lock<std::shared_mutex> ns_guard;
    bool active = false;
    uint64_t ops = 0;
    uint64_t bytes = 0;
    uint64_t deadline_us = 0;
    redis::DeferredWatchKeysUpdate pending_watch_update;
    std::vector<DeferredReply> deferred_replies;
  };
  BatchState batch_state_;
  bool batch_timer_armed_ = false;

  // Lua script timeout state
  std::atomic<bool> lua_script_running_{false};
  std::atomic<bool> lua_script_kill_requested_{false};
  std::atomic<uint64_t> lua_script_start_ms_{0};
  mutable std::mutex lua_script_mu_;
  std::string lua_script_ns_;

  // Async script reset support
  std::mutex ns_reset_mutex_;
  std::unordered_set<std::string> namespaces_to_reset_;
  uint64_t last_script_reset_generation_{0};

  // Per-namespace stats for tenant isolation
  mutable std::mutex ns_stats_mu_;
  std::unordered_map<std::string, NamespaceStats> ns_stats_;

  // Per-namespace command stats with noisy-neighbor prevention
  // shared_mutex for map lookup (parallel reads), unique_lock only for insert
  // Per-NS mutex inside NamespaceCommandStats - tenants don't block each other
  mutable std::shared_mutex ns_cmd_stats_mu_;
  std::unordered_map<std::string, std::unique_ptr<NamespaceCommandStats>> ns_cmd_stats_;

  // Accept-Dispatch: Queue for connections dispatched from Acceptor threads
  std::queue<PendingConnection> pending_conns_;
  std::mutex pending_conns_mu_;
  int dispatch_fd_{-1};            // eventfd for wakeup from Acceptor
  UniqueEvent dispatch_event_;     // libevent event for dispatch_fd_
};

class WorkerThread {
 public:
  explicit WorkerThread(std::shared_ptr<Worker> worker) : worker_(std::move(worker)) {}
  ~WorkerThread() = default;
  WorkerThread(const WorkerThread &) = delete;
  WorkerThread(WorkerThread &&) = delete;
  WorkerThread &operator=(const WorkerThread &) = delete;

  Worker *GetWorker() { return worker_.get(); }
  std::shared_ptr<Worker> GetWorkerShared() { return worker_; }
  void Start();
  void Stop(uint32_t wait_seconds);
  void Join();
  bool IsTerminated() const { return worker_->IsTerminated(); }

 private:
  std::thread t_;
  std::shared_ptr<Worker> worker_;
};
