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
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "config/config.h"
#include "event_util.h"
#include "redis_connection.h"

class Server;

struct ClientCounts {
  int connected = 0;
  int monitor = 0;
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

  void MigrateConnection(Worker *target, redis::Connection *conn);
  void DetachConnection(redis::Connection *conn);
  void FreeConnection(redis::Connection *conn);
  void FreeConnectionByID(int fd, uint64_t id);
  Status AddConnection(redis::Connection *c);
  Status EnableWriteEvent(int fd);
  Status Reply(int fd, const std::string &reply);
  void BecomeMonitorConn(redis::Connection *conn);
  void QuitMonitorConn(redis::Connection *conn);
  void FeedMonitorConns(redis::Connection *conn, const std::string &response);

  std::string GetClientsStr(redis::Connection *self);
  void KillClient(redis::Connection *self, uint64_t id, const std::string &addr, uint64_t type, bool skipme,
                  int64_t *killed);
  ClientCounts GetClientCounts(redis::Connection *self);
  void KickoutIdleClients(int timeout);

  Status ListenUnixSocket(const std::string &path, int perm, int backlog);

  void TimerCB(int, int16_t events);

  lua_State *Lua() { return lua_; }
  void LuaReset();
  void LuaResetNamespace(const std::string &ns);
  void MarkNamespaceForReset(const std::string &ns);
  void CheckAndResetIfNeeded(const std::string &ns);
  int64_t GetLuaMemorySize();

  std::map<int, redis::Connection *> GetConnections() const { return conns_; }
  Server *srv;

 private:
  Status listenFD(int fd, uint32_t expected_port, int backlog);
  Status listenTCP(const std::string &host, uint32_t port, int backlog);
  void newTCPConnection(evconnlistener *listener, evutil_socket_t fd, sockaddr *address, int socklen);
  void newUnixSocketConnection(evconnlistener *listener, evutil_socket_t fd, sockaddr *address, int socklen);
  redis::Connection *removeConnection(int fd);

  event_base *base_;
  UniqueEvent timer_;
  std::thread::id tid_;
  std::vector<evconnlistener *> listen_events_;
  std::mutex conns_mu_;
  std::map<int, redis::Connection *> conns_;
  std::map<int, redis::Connection *> monitor_conns_;
  int last_iter_conn_fd_ = 0;  // fd of last processed connection in previous cron

  struct bufferevent_rate_limit_group *rate_limit_group_ = nullptr;
  struct ev_token_bucket_cfg *rate_limit_group_cfg_ = nullptr;
  std::atomic<lua_State *> lua_;
  std::atomic<bool> is_terminated_ = false;

  // Async script reset support
  std::mutex ns_reset_mutex_;
  std::unordered_set<std::string> namespaces_to_reset_;
  uint64_t last_script_reset_generation_{0};
};

class WorkerThread {
 public:
  explicit WorkerThread(std::unique_ptr<Worker> worker) : worker_(std::move(worker)) {}
  ~WorkerThread() = default;
  WorkerThread(const WorkerThread &) = delete;
  WorkerThread(WorkerThread &&) = delete;
  WorkerThread &operator=(const WorkerThread &) = delete;

  Worker *GetWorker() { return worker_.get(); }
  void Start();
  void Stop(uint32_t wait_seconds);
  void Join();
  bool IsTerminated() const { return worker_->IsTerminated(); }

 private:
  std::thread t_;
  std::unique_ptr<Worker> worker_;
};
