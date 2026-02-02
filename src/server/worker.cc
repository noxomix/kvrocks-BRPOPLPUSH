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

#include "worker.h"

#include <event2/util.h>
#include <unistd.h>

#include <cstdint>
#include <stdexcept>
#include <string>

#include "event2/bufferevent.h"
#include "io_util.h"
#include "logging.h"
#include "scope_exit.h"
#include "thread_util.h"

#ifdef ENABLE_OPENSSL
#include <event2/bufferevent_ssl.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>

#ifdef __linux__
#include <sys/eventfd.h>
#endif

#include <algorithm>
#include <utility>

#include "redis_connection.h"
#include "redis_request.h"
#include "server.h"
#include "storage/scripting.h"

Worker::Worker(Server *srv, [[maybe_unused]] Config *config) : srv(srv), base_(event_base_new()) {
  if (!base_) throw std::runtime_error{"event base failed to be created"};

  timer_.reset(NewEvent(base_, -1, EV_PERSIST));
  timeval tm = {10, 0};
  evtimer_add(timer_.get(), &tm);

  // Accept-Dispatch: Create eventfd for wakeup when Acceptor dispatches connections
  dispatch_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (dispatch_fd_ < 0) {
    throw std::runtime_error{"eventfd creation failed: " + std::string(strerror(errno))};
  }
  dispatch_event_.reset(event_new(base_, dispatch_fd_, EV_READ | EV_PERSIST,
                                  EventCallbackFunc<&Worker::onDispatchEvent>, this));
  event_add(dispatch_event_.get(), nullptr);

  // NOTE: TCP listening is handled by Acceptor threads now (Accept-Dispatch architecture)
  // Workers only handle dispatched connections via eventfd
  // Unix socket listening for Worker0 is handled separately via ListenUnixSocket()

  lua_ = lua::CreateState();
}

Worker::~Worker() {
  DrainPendingConnections();
  // All connections (including monitors) are now in conns_
  for (const auto &iter : conns_) {
    iter.second->Close();
  }

  timer_.reset();
  dispatch_event_.reset();

  // Close dispatch eventfd
  if (dispatch_fd_ >= 0) {
    close(dispatch_fd_);
  }

  if (rate_limit_group_) {
    bufferevent_rate_limit_group_free(rate_limit_group_);
  }
  if (rate_limit_group_cfg_) {
    ev_token_bucket_cfg_free(rate_limit_group_cfg_);
  }
  event_base_free(base_);
  lua::DestroyState(lua_);
}

void Worker::TimerCB(int, [[maybe_unused]] int16_t events) {
  auto config = srv->GetConfig();
  if (config->timeout == 0) return;
  KickoutIdleClients(config->timeout);
}

void Worker::newTCPConnection(evconnlistener *listener, evutil_socket_t fd, [[maybe_unused]] sockaddr *address,
                              [[maybe_unused]] int socklen) {
  int local_port = util::GetLocalPort(fd);  // NOLINT
  info("[DEBUG] New connection: fd={} from port: {} thread #{}", fd, local_port, fmt::streamed(tid_));

  auto s = util::SockSetTcpKeepalive(fd, 120);
  if (!s.IsOK()) {
    error("[worker] Failed to set tcp-keepalive on socket. Error: {}", s.Msg());
    evutil_closesocket(fd);
    return;
  }

  s = util::SockSetTcpNoDelay(fd, 1);
  if (!s.IsOK()) {
    error("[worker] Failed to set tcp-nodelay on socket. Error: {}", s.Msg());
    evutil_closesocket(fd);
    return;
  }

  event_base *base = evconnlistener_get_base(listener);
  auto ev_thread_safe_flags =
      BEV_OPT_THREADSAFE | BEV_OPT_DEFER_CALLBACKS | BEV_OPT_UNLOCK_CALLBACKS | BEV_OPT_CLOSE_ON_FREE;

  bufferevent *bev = nullptr;
  ssl_st *ssl = nullptr;
#ifdef ENABLE_OPENSSL
  if (uint32_t(local_port) == srv->GetConfig()->tls_port) {
    ssl = SSL_new(srv->ssl_ctx.get());
    if (!ssl) {
      error("[worker] Failed to construct SSL structure for new connection: {}", fmt::streamed(SSLErrors{}));
      evutil_closesocket(fd);
      return;
    }
    bev = bufferevent_openssl_socket_new(base, fd, ssl, BUFFEREVENT_SSL_ACCEPTING, ev_thread_safe_flags);
  } else {
    bev = bufferevent_socket_new(base, fd, ev_thread_safe_flags);
  }
#else
  bev = bufferevent_socket_new(base, fd, ev_thread_safe_flags);
#endif
  if (!bev) {
    auto socket_err = evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
#ifdef ENABLE_OPENSSL
    error("[worker] Failed to construct socket for new connection: {}, SSL error: {}", socket_err,
          fmt::streamed(SSLErrors{}));
    if (ssl) SSL_free(ssl);
#else
    error("[worker] Failed to construct socket for new connection: {}", socket_err);
#endif
    evutil_closesocket(fd);
    return;
  }
#ifdef ENABLE_OPENSSL
  if (uint32_t(local_port) == srv->GetConfig()->tls_port) {
    bufferevent_openssl_set_allow_dirty_shutdown(bev, 1);
  }
#endif
  auto conn = new redis::Connection(bev, this);
  conn->SetCB(bev);
  bufferevent_enable(bev, EV_READ);

  s = AddConnection(conn);
  if (!s.IsOK()) {
    std::string err_msg = redis::Error({Status::NotOK, s.Msg()});
    s = util::SockSend(fd, err_msg, ssl);
    if (!s.IsOK()) {
      warn("[worker] Failed to send error response to socket: {}", s.Msg());
    }
    conn->Close();
    return;
  }

  if (auto s = util::GetPeerAddr(fd)) {
    auto [ip, port] = std::move(*s);
    conn->SetAddr(ip, port);
  }

  if (rate_limit_group_) {
    bufferevent_add_to_rate_limit_group(bev, rate_limit_group_);
  }
}

void Worker::newUnixSocketConnection(evconnlistener *listener, evutil_socket_t fd, [[maybe_unused]] sockaddr *address,
                                     [[maybe_unused]] int socklen) {
  debug("[worker] New connection: fd={} from unixsocket: {} thread #{}", fd, srv->GetConfig()->unixsocket,
        fmt::streamed(tid_));
  event_base *base = evconnlistener_get_base(listener);
  auto ev_thread_safe_flags =
      BEV_OPT_THREADSAFE | BEV_OPT_DEFER_CALLBACKS | BEV_OPT_UNLOCK_CALLBACKS | BEV_OPT_CLOSE_ON_FREE;
  bufferevent *bev = bufferevent_socket_new(base, fd, ev_thread_safe_flags);

  auto conn = new redis::Connection(bev, this);
  conn->SetCB(bev);
  bufferevent_enable(bev, EV_READ);

  auto s = AddConnection(conn);
  if (!s.IsOK()) {
    s = util::SockSend(fd, redis::Error(s));
    if (!s.IsOK()) {
      warn("[worker] Failed to send error response to socket: {}", s.Msg());
    }
    conn->Close();
    return;
  }

  conn->SetAddr(srv->GetConfig()->unixsocket, 0);
  if (rate_limit_group_) {
    bufferevent_add_to_rate_limit_group(bev, rate_limit_group_);
  }
}

Status Worker::listenFD(int fd, uint32_t expected_port, int backlog) {
  const uint32_t port = util::GetLocalPort(fd);
  if (port != expected_port) {
    return {Status::NotOK, "The port of the provided socket fd doesn't match the configured port"};
  }
  const int dup_fd = dup(fd);
  if (dup_fd == -1) {
    return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
  }
  evconnlistener *lev =
      NewEvconnlistener<&Worker::newTCPConnection>(base_, LEV_OPT_THREADSAFE | LEV_OPT_CLOSE_ON_FREE, backlog, dup_fd);
  listen_events_.emplace_back(lev);
  info("[worker] Listening on dup'ed fd: {}", dup_fd);
  return Status::OK();
}

Status Worker::listenTCP(const std::string &host, uint32_t port, int backlog) {
  bool ipv6_used = strchr(host.data(), ':');

  addrinfo hints = {};
  hints.ai_family = ipv6_used ? AF_INET6 : AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  addrinfo *srv_info = nullptr;
  if (int rv = getaddrinfo(host.data(), std::to_string(port).c_str(), &hints, &srv_info); rv != 0) {
    return {Status::NotOK, gai_strerror(rv)};
  }
  auto exit = MakeScopeExit([srv_info] { freeaddrinfo(srv_info); });

  for (auto p = srv_info; p != nullptr; p = p->ai_next) {
    int fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd == -1) continue;

    int sock_opt = 1;
    if (ipv6_used && setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &sock_opt, sizeof(sock_opt)) == -1) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sock_opt, sizeof(sock_opt)) < 0) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    // to support multi-thread binding on macOS
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &sock_opt, sizeof(sock_opt)) < 0) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    if (bind(fd, p->ai_addr, p->ai_addrlen)) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    evutil_make_socket_nonblocking(fd);
    auto lev =
        NewEvconnlistener<&Worker::newTCPConnection>(base_, LEV_OPT_THREADSAFE | LEV_OPT_CLOSE_ON_FREE, backlog, fd);
    listen_events_.emplace_back(lev);
  }

  return Status::OK();
}

Status Worker::ListenUnixSocket(const std::string &path, int perm, int backlog) {
  unlink(path.c_str());
  sockaddr_un sa{};
  if (path.size() > sizeof(sa.sun_path) - 1) {
    return {Status::NotOK, "unix socket path too long"};
  }

  sa.sun_family = AF_LOCAL;
  strncpy(sa.sun_path, path.c_str(), sizeof(sa.sun_path) - 1);
  int fd = socket(AF_LOCAL, SOCK_STREAM, 0);
  if (fd == -1) {
    return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
  }

  if (bind(fd, (sockaddr *)&sa, sizeof(sa)) < 0) {
    return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
  }

  evutil_make_socket_nonblocking(fd);
  auto lev = NewEvconnlistener<&Worker::newUnixSocketConnection>(base_, LEV_OPT_CLOSE_ON_FREE, backlog, fd);
  listen_events_.emplace_back(lev);
  if (perm != 0) {
    chmod(sa.sun_path, (mode_t)perm);
  }

  return Status::OK();
}

void Worker::Run(std::thread::id tid) {
  tid_ = tid;
  if (event_base_dispatch(base_) != 0) {
    error("[worker] Failed to run server, err: {}", strerror(errno));
  }
  is_terminated_ = true;
  state_.store(WorkerState::kStopped, std::memory_order_release);
}

void Worker::Stop(uint32_t wait_seconds) {
  StopAccepting();
  DrainPendingConnections();
  for (const auto &lev : listen_events_) {
    // It's unnecessary to close the listener fd since we have set the LEV_OPT_CLOSE_ON_FREE flag
    evconnlistener_free(lev);
  }
  // wait_seconds == 0 means stop immediately, or it will wait N seconds
  // for the worker to process the remaining requests before stopping.
  if (wait_seconds > 0) {
    timeval tv = {wait_seconds, 0};
    event_base_loopexit(base_, &tv);
  } else {
    event_base_loopbreak(base_);
  }
}

Status Worker::AddConnection(redis::Connection *c) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(c->GetFD());
  if (iter != conns_.end()) {
    return {Status::NotOK, "connection was exists"};
  }

  int max_clients = srv->GetConfig()->maxclients;
  if (srv->IncrClientNum() >= max_clients) {
    srv->DecrClientNum();
    return {Status::NotOK, "max number of clients reached"};
  }

  conns_.emplace(c->GetFD(), c);
  uint64_t id = srv->GetClientID();
  c->SetID(id);

  return Status::OK();
}

redis::Connection *Worker::removeConnection(int fd) {
  redis::Connection *conn = nullptr;

  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    conn = iter->second;
    conns_.erase(iter);
    srv->DecrClientNum();
    // Unregister from central monitor registry if this was a monitor connection
    if (conn->IsFlagEnabled(redis::Connection::kMonitor)) {
      srv->UnregisterMonitorClient(conn->GetNamespace(), fd);
    }
  }

  return conn;
}

// MigrateConnection moves the connection to another worker
// when reducing the number of workers.
//
// To make it simple, we would close the connection if it's
// blocked on a key or stream.
void Worker::MigrateConnection(Worker *target, redis::Connection *conn) {
  if (!target || !conn) return;

  auto bev = conn->GetBufferEvent();
  // disable read/write event to prevent the connection from being processed during migration
  bufferevent_disable(bev, EV_READ | EV_WRITE);
  // We cannot migrate the connection if it has a running command
  // since it will cause data race since the old worker may still process the command.
  if (!conn->CanMigrate()) {
    // Need to enable read/write event again since we disabled them before
    bufferevent_enable(bev, EV_READ | EV_WRITE);
    return;
  }

  // remove the connection from current worker
  DetachConnection(conn);
  if (!target->AddConnection(conn).IsOK()) {
    conn->Close();
    return;
  }
  bufferevent_base_set(target->base_, bev);
  conn->SetCB(bev);
  bufferevent_enable(bev, EV_READ | EV_WRITE);
  conn->SetOwner(target);
}

void Worker::DetachConnection(redis::Connection *conn) {
  if (!conn) return;

  removeConnection(conn->GetFD());

  if (rate_limit_group_) {
    bufferevent_remove_from_rate_limit_group(conn->GetBufferEvent());
  }

  auto bev = conn->GetBufferEvent();
  bufferevent_disable(bev, EV_READ | EV_WRITE);
  bufferevent_setcb(bev, nullptr, nullptr, nullptr, nullptr);
}

void Worker::FreeConnection(redis::Connection *conn) {
  if (!conn) return;

  removeConnection(conn->GetFD());
  srv->ResetWatchedKeys(conn);
  srv->CleanupWaitConnection(conn);
  if (rate_limit_group_) {
    bufferevent_remove_from_rate_limit_group(conn->GetBufferEvent());
  }
  delete conn;
}

void Worker::FreeConnectionByID(int fd, uint64_t id) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end() && iter->second->GetID() == id) {
    auto *conn = iter->second;
    if (rate_limit_group_ != nullptr) {
      bufferevent_remove_from_rate_limit_group(conn->GetBufferEvent());
    }
    // Unregister from central monitor registry if this was a monitor connection
    if (conn->IsFlagEnabled(redis::Connection::kMonitor)) {
      srv->UnregisterMonitorClient(conn->GetNamespace(), fd);
    }
    delete conn;
    conns_.erase(iter);
    srv->DecrClientNum();
  }
}

Status Worker::EnableWriteEvent(int fd) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    auto bev = iter->second->GetBufferEvent();
    bufferevent_enable(bev, EV_WRITE);
    return Status::OK();
  }

  return {Status::NotOK, "connection doesn't exist"};
}

Status Worker::Reply(int fd, const std::string &reply) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    iter->second->SetLastInteraction();
    redis::Reply(iter->second->Output(), reply);
    return Status::OK();
  }

  return {Status::NotOK, "connection doesn't exist"};
}

void Worker::BecomeMonitorConn(redis::Connection *conn) {
  // Prevent double registration if MONITOR is called twice
  if (conn->IsFlagEnabled(redis::Connection::kMonitor)) {
    return;
  }
  // Connection stays in conns_, only flag changes + central registration
  conn->EnableFlag(redis::Connection::kMonitor);
  srv->RegisterMonitorClient(conn->GetNamespace(), this, conn->GetFD());
}

void Worker::QuitMonitorConn(redis::Connection *conn) {
  // Connection stays in conns_, only flag changes + central unregistration
  conn->DisableFlag(redis::Connection::kMonitor);
  srv->UnregisterMonitorClient(conn->GetNamespace(), conn->GetFD());
}

std::string Worker::GetClientsStr(redis::Connection *self) {
  std::unique_lock<std::mutex> lock(conns_mu_);

  std::string output;
  for (const auto &iter : conns_) {
    redis::Connection *conn = iter.second;
    // Non-admin users can only see connections in their own namespace
    if (!self->IsAdmin() && conn->GetNamespace() != self->GetNamespace()) {
      continue;
    }
    output.append(conn->ToString());
  }

  return output;
}

void Worker::KillClient(redis::Connection *self, uint64_t id, const std::string &addr, uint64_t type, bool skipme,
                        int64_t *killed) {
  std::lock_guard<std::mutex> guard(conns_mu_);

  for (const auto &iter : conns_) {
    redis::Connection *conn = iter.second;
    if (skipme && self == conn) continue;

    // Non-admin users can only kill connections in their own namespace
    if (!self->IsAdmin() && conn->GetNamespace() != self->GetNamespace()) {
      continue;
    }

    // no need to kill the client again if the kCloseAfterReply flag is set
    if (conn->IsFlagEnabled(redis::Connection::kCloseAfterReply)) {
      continue;
    }

    if ((type & conn->GetClientType()) ||
        (!addr.empty() && (conn->GetAddr() == addr || conn->GetAnnounceAddr() == addr)) ||
        (id != 0 && conn->GetID() == id)) {
      conn->EnableFlag(redis::Connection::kCloseAfterReply);
      // enable write event to notify worker wake up ASAP, and remove the connection
      if (!conn->IsFlagEnabled(redis::Connection::kSlave)) {  // don't enable any event in slave connection
        auto bev = conn->GetBufferEvent();
        bufferevent_enable(bev, EV_WRITE);
      }
      (*killed)++;
    }
  }
}

ClientCounts Worker::GetClientCounts(redis::Connection *self) {
  std::lock_guard<std::mutex> guard(conns_mu_);
  ClientCounts counts;
  for (const auto &[fd, conn] : conns_) {
    // Non-admin only sees own namespace
    if (!self->IsAdmin() && conn->GetNamespace() != self->GetNamespace()) {
      continue;
    }
    counts.connected++;
    // Count monitor connections via flag
    if (conn->IsFlagEnabled(redis::Connection::kMonitor)) {
      counts.monitor++;
    }
  }
  return counts;
}

void Worker::LuaReset() {
  auto lua = lua_.exchange(lua::CreateState());
  lua::DestroyState(lua);
}

void Worker::LuaResetNamespace(const std::string &ns) {
  lua_State *lua = Lua();
  std::string ns_prefix = ns + "_";

  // Part A: Clean up FUNCTIONs (library-based scripts)
  // Only if REDIS_FUNCTION_LIBRARIES table exists
  lua_getglobal(lua, REDIS_FUNCTION_LIBRARIES);
  if (lua_istable(lua, -1)) {
    // Collect all library keys and function names that match this namespace
    std::vector<std::string> libs_to_delete;
    std::vector<std::string> funcs_to_delete;

    lua_pushnil(lua);
    while (lua_next(lua, -2) != 0) {
      // Key at -2, value (array of func names) at -1
      const char *lib_key = lua_tostring(lua, -2);
      if (lib_key && std::string_view(lib_key).substr(0, ns_prefix.size()) == ns_prefix) {
        libs_to_delete.emplace_back(lib_key);

        // Iterate through function names in this library's array
        if (lua_istable(lua, -1)) {
          size_t len = lua_objlen(lua, -1);
          for (size_t i = 1; i <= len; ++i) {
            lua_rawgeti(lua, -1, static_cast<int>(i));
            const char *func_name = lua_tostring(lua, -1);
            if (func_name) {
              // Function globals use ns_prefixed_name: <ns>_<funcname>
              funcs_to_delete.push_back(ns_prefix + func_name);
            }
            lua_pop(lua, 1);
          }
        }
      }
      lua_pop(lua, 1);  // Pop value, keep key for next iteration
    }

    // Delete function globals: __redis_registered_<ns>_<func> and __redis_registered_flags_<ns>_<func>
    for (const auto &ns_prefixed_func : funcs_to_delete) {
      lua_pushnil(lua);
      lua_setglobal(lua, (REDIS_LUA_REGISTER_FUNC_PREFIX + ns_prefixed_func).c_str());

      lua_pushnil(lua);
      lua_setglobal(lua, (REDIS_LUA_REGISTER_FUNC_FLAGS_PREFIX + ns_prefixed_func).c_str());
    }

    // Delete library entries from REDIS_FUNCTION_LIBRARIES
    for (const auto &lib : libs_to_delete) {
      lua_pushnil(lua);
      lua_setfield(lua, -2, lib.c_str());
    }
  }
  lua_pop(lua, 1);  // Pop REDIS_FUNCTION_LIBRARIES table (or nil)

  // Part B: Clean up EVAL scripts: f_<ns>_<sha> and f_<ns>_<sha>_flags_
  // This ALWAYS runs, regardless of whether FUNCTION libraries exist
  std::string script_prefix = std::string(REDIS_LUA_FUNC_SHA_PREFIX) + ns + "_";
  std::vector<std::string> scripts_to_delete;

  // Iterate _G to find all script globals matching the namespace
  // Note: Using LUA_GLOBALSINDEX for Lua 5.1 compatibility (LuaJIT)
  lua_pushvalue(lua, LUA_GLOBALSINDEX);
  lua_pushnil(lua);
  while (lua_next(lua, -2) != 0) {
    if (lua_type(lua, -2) == LUA_TSTRING) {
      const char *key = lua_tostring(lua, -2);
      if (key && strncmp(key, script_prefix.c_str(), script_prefix.size()) == 0) {
        scripts_to_delete.emplace_back(key);
      }
    }
    lua_pop(lua, 1);  // Pop value, keep key for next iteration
  }
  lua_pop(lua, 1);  // Pop _G

  // Delete collected script globals
  for (const auto &script : scripts_to_delete) {
    lua_pushnil(lua);
    lua_setglobal(lua, script.c_str());
  }
}

void Worker::MarkNamespaceForReset(const std::string &ns) {
  std::lock_guard<std::mutex> lock(ns_reset_mutex_);
  namespaces_to_reset_.insert(ns);
}

void Worker::CheckAndResetIfNeeded(const std::string &ns) {
  // 1. Check global reset (SCRIPT FLUSH)
  auto current_gen = srv->GetScriptResetGeneration();
  if (current_gen > last_script_reset_generation_) {
    LuaReset();  // Full reset
    last_script_reset_generation_ = current_gen;
    // Clear namespace set - full reset covers everything
    std::lock_guard<std::mutex> lock(ns_reset_mutex_);
    namespaces_to_reset_.clear();
    return;
  }

  // 2. Check namespace-specific reset (FUNCTION FLUSH)
  std::lock_guard<std::mutex> lock(ns_reset_mutex_);
  auto it = namespaces_to_reset_.find(ns);
  if (it != namespaces_to_reset_.end()) {
    namespaces_to_reset_.erase(it);
    LuaResetNamespace(ns);
  }
}

// Per-namespace stats methods for tenant isolation
void Worker::IncrCallsForNamespace(const std::string &ns) {
  std::lock_guard<std::mutex> lock(ns_stats_mu_);
  ns_stats_[ns].total_calls.fetch_add(1, std::memory_order_relaxed);
}

void Worker::IncrInboundBytesForNamespace(const std::string &ns, uint64_t bytes) {
  std::lock_guard<std::mutex> lock(ns_stats_mu_);
  ns_stats_[ns].in_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

void Worker::IncrOutboundBytesForNamespace(const std::string &ns, uint64_t bytes) {
  std::lock_guard<std::mutex> lock(ns_stats_mu_);
  ns_stats_[ns].out_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

void Worker::IncrConnectionsForNamespace(const std::string &ns) {
  std::lock_guard<std::mutex> lock(ns_stats_mu_);
  ns_stats_[ns].total_connections.fetch_add(1, std::memory_order_relaxed);
}

NamespaceStatsSnapshot Worker::GetNamespaceStats(const std::string &ns) const {
  std::lock_guard<std::mutex> lock(ns_stats_mu_);
  auto it = ns_stats_.find(ns);
  if (it == ns_stats_.end()) {
    return {};  // Empty stats if namespace not found
  }
  return {it->second.total_calls.load(std::memory_order_relaxed),
          it->second.in_bytes.load(std::memory_order_relaxed),
          it->second.out_bytes.load(std::memory_order_relaxed),
          it->second.total_connections.load(std::memory_order_relaxed)};
}

std::unordered_map<std::string, NamespaceStatsSnapshot> Worker::GetNamespaceStatsSnapshot() const {
  std::lock_guard<std::mutex> lock(ns_stats_mu_);
  std::unordered_map<std::string, NamespaceStatsSnapshot> snapshot;
  for (const auto &[ns, stats] : ns_stats_) {
    snapshot[ns] = {stats.total_calls.load(std::memory_order_relaxed),
                    stats.in_bytes.load(std::memory_order_relaxed),
                    stats.out_bytes.load(std::memory_order_relaxed),
                    stats.total_connections.load(std::memory_order_relaxed)};
  }
  return snapshot;
}

// Per-namespace command stats with noisy-neighbor prevention
void Worker::IncrCommandStatForNamespace(const std::string &ns, const std::string &cmd, uint64_t latency) {
  NamespaceCommandStats *stats = nullptr;

  // Fast path: shared_lock for lookup (parallel with other tenants)
  {
    std::shared_lock<std::shared_mutex> lock(ns_cmd_stats_mu_);
    auto it = ns_cmd_stats_.find(ns);
    if (it != ns_cmd_stats_.end()) {
      stats = it->second.get();
    }
  }

  // Slow path: namespace not yet known → unique_lock for insert
  if (!stats) {
    std::unique_lock<std::shared_mutex> lock(ns_cmd_stats_mu_);
    // Double-check after lock upgrade
    auto it = ns_cmd_stats_.find(ns);
    if (it == ns_cmd_stats_.end()) {
      ns_cmd_stats_[ns] = std::make_unique<NamespaceCommandStats>();
      it = ns_cmd_stats_.find(ns);
    }
    stats = it->second.get();
  }

  // Per-NS lock (other tenants not affected)
  {
    std::lock_guard<std::mutex> lock(stats->mu);
    auto &cmd_stat = stats->commands[cmd];
    cmd_stat.calls.fetch_add(1, std::memory_order_relaxed);
    cmd_stat.latency.fetch_add(latency, std::memory_order_relaxed);
  }
}

std::map<std::string, CommandStatSnapshot> Worker::GetCommandStatsForNamespace(const std::string &ns) const {
  std::map<std::string, CommandStatSnapshot> result;

  // shared_lock for lookup (parallel with other tenants)
  std::shared_lock<std::shared_mutex> map_lock(ns_cmd_stats_mu_);
  auto it = ns_cmd_stats_.find(ns);
  if (it == ns_cmd_stats_.end()) {
    return result;
  }

  // Per-NS lock for snapshot (short, only atomic loads)
  {
    std::lock_guard<std::mutex> lock(it->second->mu);
    for (const auto &[cmd, stat] : it->second->commands) {
      result[cmd] = {stat.calls.load(std::memory_order_relaxed),
                     stat.latency.load(std::memory_order_relaxed)};
    }
  }

  return result;
}

int64_t Worker::GetLuaMemorySize() { return (int64_t)lua_gc(lua_, LUA_GCCOUNT, 0) * 1024; }

void Worker::KickoutIdleClients(int timeout) {
  std::vector<std::pair<int, uint64_t>> to_be_killed_conns;

  {
    std::lock_guard<std::mutex> guard(conns_mu_);
    if (conns_.empty()) {
      return;
    }

    int iterations = std::min(static_cast<int>(conns_.size()), 50);
    auto iter = conns_.upper_bound(last_iter_conn_fd_);
    while (iterations--) {
      if (iter == conns_.end()) iter = conns_.begin();
      if (static_cast<int>(iter->second->GetIdleTime()) >= timeout) {
        to_be_killed_conns.emplace_back(iter->first, iter->second->GetID());
      }
      iter++;
    }
    iter--;
    last_iter_conn_fd_ = iter->first;
  }

  for (const auto &conn : to_be_killed_conns) {
    FreeConnectionByID(conn.first, conn.second);
  }
}

// Accept-Dispatch: Called by Acceptor thread to dispatch a new connection to this Worker
// Thread-safe: Adds to queue and wakes up Worker's event loop via eventfd
void Worker::DispatchConnection(PendingConnection conn) {
  // Don't dispatch to terminated workers
  if (!IsAccepting() || is_terminated_.load(std::memory_order_acquire)) {
    debug("[worker] Dropping connection fd={} - worker not accepting", conn.fd);
    close(conn.fd);
    return;
  }

  size_t limit = srv->GetConfig()->acceptor_queue_limit;
  bool drop = false;
  {
    std::lock_guard<std::mutex> lock(pending_conns_mu_);
    if (!IsAccepting() || is_terminated_.load(std::memory_order_acquire)) {
      drop = true;
    } else if (limit > 0 && pending_conns_.size() >= limit) {
      drop = true;
    } else {
      pending_conns_.push(std::move(conn));
    }
  }

  if (drop) {
    close(conn.fd);
    return;
  }
  // Wake up the event loop via eventfd
  uint64_t val = 1;
  if (write(dispatch_fd_, &val, sizeof(val)) < 0) {
    error("[worker] Failed to write to eventfd: {}", strerror(errno));
  }
}

// Accept-Dispatch: Event callback when Acceptor dispatches connections
void Worker::onDispatchEvent(int, int16_t) {
  // Clear the eventfd
  uint64_t val;
  if (read(dispatch_fd_, &val, sizeof(val)) < 0 && errno != EAGAIN) {
    error("[worker] Failed to read from eventfd: {}", strerror(errno));
  }

  // Process all pending connections (batch processing for efficiency)
  std::vector<PendingConnection> conns;
  {
    std::lock_guard<std::mutex> lock(pending_conns_mu_);
    while (!pending_conns_.empty()) {
      conns.push_back(std::move(pending_conns_.front()));
      pending_conns_.pop();
    }
  }

  for (auto &pc : conns) {
    if (!IsAccepting() || is_terminated_.load(std::memory_order_acquire)) {
      close(pc.fd);
      continue;
    }
    createConnectionFromDispatch(pc);
  }
}

void Worker::DrainPendingConnections() {
  std::queue<PendingConnection> pending;
  {
    std::lock_guard<std::mutex> lock(pending_conns_mu_);
    std::swap(pending, pending_conns_);
  }
  while (!pending.empty()) {
    close(pending.front().fd);
    pending.pop();
  }
}

// Accept-Dispatch: Create connection from dispatched fd
// Similar to newTCPConnection but uses is_tls flag instead of port check
void Worker::createConnectionFromDispatch(const PendingConnection &pc) {
  debug("[worker] Dispatched connection: fd={} is_tls={} thread #{}", pc.fd, pc.is_tls, fmt::streamed(tid_));

  auto s = util::SockSetTcpKeepalive(pc.fd, 120);
  if (!s.IsOK()) {
    error("[worker] Failed to set tcp-keepalive on socket. Error: {}", s.Msg());
    evutil_closesocket(pc.fd);
    return;
  }

  s = util::SockSetTcpNoDelay(pc.fd, 1);
  if (!s.IsOK()) {
    error("[worker] Failed to set tcp-nodelay on socket. Error: {}", s.Msg());
    evutil_closesocket(pc.fd);
    return;
  }

  auto ev_thread_safe_flags =
      BEV_OPT_THREADSAFE | BEV_OPT_DEFER_CALLBACKS | BEV_OPT_UNLOCK_CALLBACKS | BEV_OPT_CLOSE_ON_FREE;

  bufferevent *bev = nullptr;
  ssl_st *ssl = nullptr;
#ifdef ENABLE_OPENSSL
  if (pc.is_tls) {
    ssl = SSL_new(srv->ssl_ctx.get());
    if (!ssl) {
      error("[worker] Failed to construct SSL structure for new connection: {}", fmt::streamed(SSLErrors{}));
      evutil_closesocket(pc.fd);
      return;
    }
    bev = bufferevent_openssl_socket_new(base_, pc.fd, ssl, BUFFEREVENT_SSL_ACCEPTING, ev_thread_safe_flags);
  } else {
    bev = bufferevent_socket_new(base_, pc.fd, ev_thread_safe_flags);
  }
#else
  bev = bufferevent_socket_new(base_, pc.fd, ev_thread_safe_flags);
#endif
  if (!bev) {
    auto socket_err = evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
#ifdef ENABLE_OPENSSL
    error("[worker] Failed to construct socket for new connection: {}, SSL error: {}", socket_err,
          fmt::streamed(SSLErrors{}));
    if (ssl) SSL_free(ssl);
#else
    error("[worker] Failed to construct socket for new connection: {}", socket_err);
#endif
    evutil_closesocket(pc.fd);
    return;
  }
#ifdef ENABLE_OPENSSL
  if (pc.is_tls) {
    bufferevent_openssl_set_allow_dirty_shutdown(bev, 1);
  }
#endif
  auto conn = new redis::Connection(bev, this);
  conn->SetCB(bev);
  bufferevent_enable(bev, EV_READ);

  s = AddConnection(conn);
  if (!s.IsOK()) {
    std::string err_msg = redis::Error({Status::NotOK, s.Msg()});
    s = util::SockSend(pc.fd, err_msg, ssl);
    if (!s.IsOK()) {
      warn("[worker] Failed to send error response to socket: {}", s.Msg());
    }
    conn->Close();
    return;
  }

  if (auto s = util::GetPeerAddr(pc.fd)) {
    auto [ip, port] = std::move(*s);
    conn->SetAddr(ip, port);
  }

  if (rate_limit_group_) {
    bufferevent_add_to_rate_limit_group(bev, rate_limit_group_);
  }
}

void WorkerThread::Start() {
  auto s = util::CreateThread("worker", [this] { this->worker_->Run(std::this_thread::get_id()); });

  if (s) {
    t_ = std::move(*s);
  } else {
    error("[worker] Failed to start worker thread, err: {}", s.Msg());
    return;
  }

  info("[worker] Thread #{} started", fmt::streamed(t_.get_id()));
}

void WorkerThread::Stop(uint32_t wait_seconds) { worker_->Stop(wait_seconds); }

void WorkerThread::Join() {
  if (auto s = util::ThreadJoin(t_); !s) {
    warn("[worker] {}", s.Msg());
  }
}
