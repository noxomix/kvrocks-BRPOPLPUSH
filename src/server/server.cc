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

#include "server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <rocksdb/convenience.h>
#include <rocksdb/statistics.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/statvfs.h>
#include <sys/utsname.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <jsoncons/json.hpp>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <tuple>
#include <utility>

#include "commands/command_parser.h"
#include "commands/commander.h"
#include "common/string_util.h"
#include "config/config.h"
#include "fmt/format.h"
#include "logging.h"
#include "redis_connection.h"
#include "redis_reply.h"
#include "rocksdb/version.h"
#include "storage/compaction_checker.h"
#include "storage/redis_db.h"
#include "storage/scripting.h"
#include "storage/storage.h"
#include "thread_util.h"
#include "time_util.h"
#include "version.h"
#include "worker.h"

Server::Server(engine::Storage *storage, Config *config)
    : stats(config->histogram_bucket_boundaries),
      storage(storage),
      indexer(storage),
      index_mgr(&indexer, storage),
      start_time_secs_(util::GetTimeStamp()),
      config_(config),
      namespace_(storage) {
  // init commands stats here to prevent concurrent insert, and cause core
  auto commands = redis::CommandTable::GetOriginal();

  for (const auto &iter : *commands) {
    stats.commands_stats[iter.first].calls = 0;
    stats.commands_stats[iter.first].latency = 0;

    if (stats.bucket_boundaries.size() > 0) {
      // NB: Extra index for the last bucket (Inf)
      for (std::size_t i{0}; i <= stats.bucket_boundaries.size(); ++i) {
        stats.commands_histogram[iter.first].buckets.push_back(std::make_unique<std::atomic<uint64_t>>(0));
      }
      stats.commands_histogram[iter.first].calls = 0;
      stats.commands_histogram[iter.first].sum = 0;
    }
  }

  // init cursor_dict_
  cursor_dict_ = std::make_unique<CursorDictType>();

#ifdef ENABLE_OPENSSL
  // init ssl context
  if (config->tls_port || config->tls_replication) {
    ssl_ctx = CreateSSLContext(config);
    if (!ssl_ctx) {
      exit(1);
    }
  }
#endif

  // Init cluster
  cluster = std::make_unique<Cluster>(this, config_->binds, config_->port);

  // init shard pub/sub channels
  pubsub_shard_channels_.resize(config->cluster_enabled ? HASH_SLOTS_SIZE : 1);

  for (int i = 0; i < config->workers; i++) {
    auto worker = std::make_shared<Worker>(this, config);
    worker->SetIndex(static_cast<uint32_t>(i));
    // multiple workers can't listen to the same unix socket, so
    // listen unix socket only from a single worker - the first one
    if (!config->unixsocket.empty() && i == 0) {
      Status s = worker->ListenUnixSocket(config->unixsocket, config->unixsocketperm, config->backlog);
      if (!s.IsOK()) {
        error("[server] Failed to listen on unix socket: {}. Error: {}", config->unixsocket, s.Msg());
        exit(1);
      }
      info("[server] Listening on unix socket: {}", config->unixsocket);
    }
    worker_threads_.emplace_back(std::make_unique<WorkerThread>(std::move(worker)));
  }
  PublishWorkerSnapshot();

  // Initialize SNI-based fair scheduler (Phase 3)
  fair_scheduler_ = std::make_unique<FairScheduler>(this);

  AdjustOpenFilesLimit();
  slow_log_.SetMaxEntries(config->slowlog_max_len);
  slow_log_.SetDumpToLogfileLevel(config->slowlog_dump_logfile_level);
  perf_log_.SetMaxEntries(config->profiling_sample_record_max_len);
}

Server::~Server() {
  DisconnectSlaves();
  // Wait for all fetch file threads stop and exit and force destroy the server after 60s.
  int counter = 0;
  while (GetFetchFileThreadNum() != 0) {
    usleep(100000);
    if (++counter == 600) {
      warn("[server] Will force destroy the server after waiting 60s, leave {} fetch file threads are still running",
           GetFetchFileThreadNum());
      break;
    }
  }

  for (auto &worker_thread : worker_threads_) {
    worker_thread.reset();
  }
  cleanupExitedWorkerThreads(true /* force */);
  CleanupExitedSlaves();
}

// Kvrocks threads list:
// - Work-thread: process client's connections and requests
// - Task-runner: one thread pool, handle some jobs that may freeze server if run directly
// - Cron-thread: server's crontab, clean backups, resize sst and memtable size
// - Compaction-checker: active compaction according to collected statistics
// - Replication-thread: replicate incremental stream from master if in slave role, there
//   are some dynamic threads to fetch files when full sync.
//     - fetch-file-thread: fetch SST files from master
// - Feed-slave-thread: feed data to slaves if having slaves, but there also are some dynamic
//   threads when full sync, TODO(@shooterit) we should manage this threads uniformly.
//     - feed-replica-data-info: generate checkpoint and send files list when full sync
//     - feed-replica-file: send SST files when slaves ask for full sync
Status Server::Start() {
  auto s = namespace_.LoadAndRewrite();
  if (!s.IsOK()) {
    return s;
  }
  if (!config_->master_host.empty()) {
    s = AddMaster(config_->master_host, static_cast<uint32_t>(config_->master_port), false);
    if (!s.IsOK()) return s;
  } else {
    // Generate new replication id if not a replica
    engine::Context ctx(storage);
    s = storage->ShiftReplId(ctx);
    if (!s.IsOK()) {
      return s.Prefixed("failed to shift replication id");
    }
  }

  if (!config_->cluster_enabled) {
    engine::Context no_txn_ctx = engine::Context::NoTransactionContext(storage);
    GET_OR_RET(index_mgr.Load(no_txn_ctx, kDefaultNamespace));
    for (const auto &[_, ns] : namespace_.List()) {
      GET_OR_RET(index_mgr.Load(no_txn_ctx, ns));
    }
  }

  if (config_->cluster_enabled) {
    // Create objects used for slot migration
    slot_migrator = std::make_unique<SlotMigrator>(this);

    if (config_->persist_cluster_nodes_enabled) {
      auto s = cluster->LoadClusterNodes(config_->NodesFilePath());
      if (!s.IsOK()) {
        return s.Prefixed("failed to load cluster nodes info");
      }
    }

    auto s = slot_migrator->CreateMigrationThread();
    if (!s.IsOK()) {
      return s.Prefixed("failed to create migration thread");
    }

    slot_import = std::make_unique<SlotImport>(this);
  }

  for (const auto &worker : worker_threads_) {
    worker->Start();
  }

  // Start acceptor threads AFTER workers are ready
  auto acceptor_status = StartAcceptors();
  if (!acceptor_status.IsOK()) {
    for (const auto &worker : worker_threads_) {
      worker->Stop(0 /* immediately terminate */);
    }
    for (const auto &worker : worker_threads_) {
      worker->Join();
    }
    return acceptor_status.Prefixed("failed to start acceptors");
  }

  if (auto s = task_runner_.Start(); !s) {
    warn("Failed to start task runner: {}", s.Msg());
  }
  // setup server cron thread
  cron_thread_ = GET_OR_RET(util::CreateThread("server-cron", [this] { this->cron(); }));

  compaction_checker_thread_ = GET_OR_RET(util::CreateThread("compact-check", [this] {
    uint64_t counter = 0;
    int64_t last_compact_date = 0;
    CompactionChecker compaction_checker{this->storage};

    while (!stop_) {
      // Sleep first
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      // To guarantee accessing DB safely
      auto guard = storage->ReadLockGuard();
      if (storage->IsClosing()) continue;

      if (!is_loading_ && ++counter % 600 == 0  // check every minute
          && config_->compaction_checker_cron.IsEnabled()) {
        auto t_now = static_cast<time_t>(util::GetTimeStamp());
        std::tm now{};
        localtime_r(&t_now, &now);
        if (config_->compaction_checker_cron.IsTimeMatch(&now)) {
          const auto &column_family_list = engine::ColumnFamilyConfigs::ListAllColumnFamilies();
          for (auto &column_family : column_family_list) {
            compaction_checker.PickCompactionFilesForCf(column_family);
          }
        }
        // compact once per day
        auto now_hours = t_now / 3600;
        if (now_hours != 0 && last_compact_date != now_hours / 24) {
          last_compact_date = now_hours / 24;
          compaction_checker.CompactPropagateAndPubSubFiles();
        }
      }
    }
  }));

  memory_startup_use_.store(Stats::GetMemoryRSS(), std::memory_order_relaxed);
  info("[server] Ready to accept connections");

  return Status::OK();
}

void Server::Stop() {
  stop_ = true;

  // Stop acceptors first (no new connections)
  StopAcceptors();

  slaveof_mu_.lock();
  if (replication_thread_) replication_thread_->Stop();
  slaveof_mu_.unlock();

  for (const auto &worker : worker_threads_) {
    worker->Stop(0 /* immediately terminate  */);
  }

  task_runner_.Cancel();
}

void Server::Join() {
  if (auto s = util::ThreadJoin(cron_thread_); !s) {
    warn("Cron thread operation failed: {}", s.Msg());
  }
  if (auto s = util::ThreadJoin(compaction_checker_thread_); !s) {
    warn("Compaction checker thread operation failed: {}", s.Msg());
  }
  if (auto s = task_runner_.Join(); !s) {
    warn("{}", s.Msg());
  }
  for (const auto &worker : worker_threads_) {
    worker->Join();
  }
}

Status Server::AddMaster(const std::string &host, uint32_t port, bool force_reconnect) {
  std::lock_guard<std::mutex> guard(slaveof_mu_);

  // Don't check host and port if 'force_reconnect' argument is set to true
  if (!force_reconnect && !master_host_.empty() && master_host_ == host && master_port_ == port) {
    return Status::OK();
  }

  // Master is changed
  if (!master_host_.empty()) {
    if (replication_thread_) replication_thread_->Stop();
    replication_thread_ = nullptr;
  }

  // For master using old version, it uses replication thread to implement
  // replication, and uses 'listen-port + 1' as thread listening port.
  uint32_t master_listen_port = port;
  if (GetConfig()->GetSnapshot()->master_use_repl_port) master_listen_port += 1;

  replication_thread_ = std::make_unique<ReplicationThread>(host, master_listen_port, this);
  auto s = replication_thread_->Start([this]() { return PrepareRestoreDB(); },
                                      [this]() {
                                        this->is_loading_ = false;
                                        if (auto s = task_runner_.Start(); !s) {
                                          warn("Failed to start task runner: {}", s.Msg());
                                        }
                                      });
  if (s.IsOK()) {
    master_host_ = host;
    master_port_ = port;
    config_->SetMaster(host, port);
  } else {
    replication_thread_ = nullptr;
  }
  return s;
}

Status Server::RemoveMaster() {
  std::lock_guard<std::mutex> guard(slaveof_mu_);

  if (!master_host_.empty()) {
    master_host_.clear();
    master_port_ = 0;
    config_->ClearMaster();
    if (replication_thread_) {
      replication_thread_->Stop();
      replication_thread_ = nullptr;
    }
    engine::Context ctx(storage);
    return storage->ShiftReplId(ctx);
  }
  return Status::OK();
}

Status Server::AddSlave(redis::Connection *conn, rocksdb::SequenceNumber next_repl_seq) {
  auto t = std::make_unique<FeedSlaveThread>(this, conn, next_repl_seq);
  auto s = t->Start();
  if (!s.IsOK()) {
    return s;
  }

  std::unique_lock<std::shared_mutex> lg(slave_threads_mu_);
  slave_threads_.emplace_back(std::move(t));
  return Status::OK();
}

void Server::DisconnectSlaves() {
  std::unique_lock<std::shared_mutex> lg(slave_threads_mu_);

  for (auto &slave_thread : slave_threads_) {
    if (!slave_thread->IsStopped()) slave_thread->Stop();
  }

  while (!slave_threads_.empty()) {
    auto slave_thread = std::move(slave_threads_.front());
    slave_threads_.pop_front();
    slave_thread->Join();
  }
}

void Server::CleanupExitedSlaves() {
  std::unique_lock<std::shared_mutex> lg(slave_threads_mu_);

  for (auto it = slave_threads_.begin(); it != slave_threads_.end();) {
    if ((*it)->IsStopped()) {
      auto thread = std::move(*it);
      it = slave_threads_.erase(it);
      thread->Join();
    } else {
      ++it;
    }
  }
}

std::vector<std::string> Server::RedactSensitiveTokens(const std::vector<std::string> &tokens) {
  if (tokens.empty()) return tokens;
  std::string cmd = util::ToLower(tokens[0]);
  if (cmd != "auth" && cmd != "hello") return tokens;

  std::vector<std::string> redacted_tokens = tokens;
  if (cmd == "auth" && tokens.size() >= 2) {
    // AUTH password -> redact password (arg 1)
    redacted_tokens[1] = "(redacted)";
  } else if (cmd == "hello" && tokens.size() >= 3) {
    // HELLO [version] [AUTH [username] password] [SETNAME name]
    for (size_t i = 1; i < tokens.size(); ++i) {
      std::string arg = util::ToLower(tokens[i]);
      if (arg == "auth" && i + 1 < tokens.size()) {
        size_t remaining_args = tokens.size() - i - 1;
        if (remaining_args >= 2) {
          // Check if this follows the pattern AUTH username password
          // In this case, redact the password (arg i+2)
          redacted_tokens[i + 2] = "(redacted)";
        } else if (remaining_args == 1) {
          // This follows the pattern AUTH password
          // Redact the password (arg i+1)
          redacted_tokens[i + 1] = "(redacted)";
        }
        break;
      }
    }
  }
  return redacted_tokens;
}

void Server::RegisterMonitorClient(const std::string &ns, Worker *worker, int fd) {
  {
    std::unique_lock<std::shared_mutex> map_lock(monitor_namespaces_mu_);
    auto it = monitor_namespaces_.find(ns);
    if (it == monitor_namespaces_.end()) {
      it = monitor_namespaces_.emplace(ns, std::make_unique<NamespaceMonitors>()).first;
    }
    std::lock_guard<std::mutex> ns_lock(it->second->mu);
    it->second->clients.emplace(fd, worker);
  }
  monitor_clients_.fetch_add(1);
}

void Server::UnregisterMonitorClient(const std::string &ns, int fd) {
  bool found = false;
  {
    std::shared_lock<std::shared_mutex> map_lock(monitor_namespaces_mu_);
    auto it = monitor_namespaces_.find(ns);
    if (it != monitor_namespaces_.end()) {
      std::lock_guard<std::mutex> ns_lock(it->second->mu);
      found = (it->second->clients.erase(fd) > 0);
    }
  }
  if (found) {
    monitor_clients_.fetch_sub(1);
  }
}

void Server::FeedMonitorConns(redis::Connection *conn, const std::vector<std::string> &tokens) {
  if (monitor_clients_ <= 0) return;

  const std::string &ns = conn->GetNamespace();

  // Collect monitor clients under lock
  std::vector<std::pair<Worker *, int>> to_notify;
  {
    std::shared_lock<std::shared_mutex> map_lock(monitor_namespaces_mu_);
    auto it = monitor_namespaces_.find(ns);
    if (it != monitor_namespaces_.end()) {
      std::lock_guard<std::mutex> ns_lock(it->second->mu);
      for (const auto &[fd, worker] : it->second->clients) {
        if (fd != conn->GetFD()) {  // Skip self
          to_notify.emplace_back(worker, fd);
        }
      }
    }
  }

  if (to_notify.empty()) return;

  // Format output
  auto now_us = util::GetTimeStampUS();
  std::string output =
      fmt::format("{}.{} [{} {}]", now_us / 1000000, now_us % 1000000, ns, conn->GetAddr());

  auto redacted_tokens = RedactSensitiveTokens(tokens);

  for (const auto &token : redacted_tokens) {
    output += " \"";
    output += util::EscapeString(token);
    output += "\"";
  }

  // Reply outside lock (Copy-then-Reply pattern)
  for (const auto &[worker, fd] : to_notify) {
    std::ignore = worker->Reply(fd, redis::SimpleString(output));
  }
}

// Cleanup PubSub resources when namespace is deleted
void Server::CleanupPubSubNamespace(const std::string &ns) {
  std::unique_lock<std::shared_mutex> lock(pubsub_namespaces_mu_);
  pubsub_namespaces_.erase(ns);
}

// Cleanup Monitor resources when namespace is deleted
void Server::CleanupMonitorNamespace(const std::string &ns) {
  std::unique_lock<std::shared_mutex> lock(monitor_namespaces_mu_);
  auto it = monitor_namespaces_.find(ns);
  if (it != monitor_namespaces_.end()) {
    // Adjust global counter by number of clients being removed
    monitor_clients_.fetch_sub(static_cast<int>(it->second->clients.size()));
    monitor_namespaces_.erase(it);
  }
}

// Get pattern count for a specific namespace
size_t Server::GetPubSubPatternSize(const std::string &ns) const {
  std::shared_lock<std::shared_mutex> lock(pubsub_namespaces_mu_);

  // Each namespace (including admin/default) sees only their own pattern count
  auto it = pubsub_namespaces_.find(ns);
  if (it == pubsub_namespaces_.end()) {
    return 0;
  }
  std::lock_guard<std::mutex> ns_lock(it->second->mu);
  return it->second->patterns.size();
}

int Server::PublishMessage(const std::string &ns, const std::string &channel, const std::string &msg) {
  int cnt = 0;
  int index = 0;

  // Collect subscribers under lock, reply outside lock
  std::vector<std::tuple<Worker *, int, uint64_t>> to_publish_conn_ctxs;
  std::vector<std::string> patterns;
  std::vector<std::tuple<Worker *, int, uint64_t>> to_publish_patterns_conn_ctxs;

  {
    std::shared_lock<std::shared_mutex> ns_map_lock(pubsub_namespaces_mu_);

    // Only publish to subscribers in the same namespace (tenant isolation)
    auto it = pubsub_namespaces_.find(ns);
    if (it != pubsub_namespaces_.end()) {
      std::lock_guard<std::mutex> ns_lock(it->second->mu);

      // Collect channel subscribers
      if (auto iter = it->second->channels.find(channel); iter != it->second->channels.end()) {
        for (const auto &conn_ctx : iter->second) {
          to_publish_conn_ctxs.emplace_back(conn_ctx.owner, conn_ctx.fd, conn_ctx.conn_id);
        }
      }

      // Collect pattern subscribers
      for (const auto &iter : it->second->patterns) {
        if (util::StringMatch(iter.first, channel, false)) {
          for (const auto &conn_ctx : iter.second) {
            to_publish_patterns_conn_ctxs.emplace_back(conn_ctx.owner, conn_ctx.fd, conn_ctx.conn_id);
            patterns.emplace_back(iter.first);
          }
        }
      }
    }
  }

  // Reply outside lock (avoids holding lock during I/O)
  std::string channel_reply;
  channel_reply.append(redis::MultiLen(3));
  channel_reply.append(redis::BulkString("message"));
  channel_reply.append(redis::BulkString(channel));
  channel_reply.append(redis::BulkString(msg));
  for (const auto &[owner, fd, conn_id] : to_publish_conn_ctxs) {
    auto s = owner->ReplyByID(fd, conn_id, channel_reply);
    if (s.IsOK()) {
      cnt++;
    }
  }

  // We should publish corresponding pattern and message for connections
  for (const auto &[owner, fd, conn_id] : to_publish_patterns_conn_ctxs) {
    std::string pattern_reply;
    pattern_reply.append(redis::MultiLen(4));
    pattern_reply.append(redis::BulkString("pmessage"));
    pattern_reply.append(redis::BulkString(patterns[index++]));
    pattern_reply.append(redis::BulkString(channel));
    pattern_reply.append(redis::BulkString(msg));
    auto s = owner->ReplyByID(fd, conn_id, pattern_reply);
    if (s.IsOK()) {
      cnt++;
    }
  }

  return cnt;
}

void Server::SubscribeChannel(const std::string &channel, redis::Connection *conn) {
  const std::string &ns = conn->GetNamespace();

  // Use unique_lock for Subscribe since we may need to create the namespace entry.
  // This also protects against CleanupPubSubNamespace() deleting the entry.
  std::unique_lock<std::shared_mutex> ns_map_lock(pubsub_namespaces_mu_);

  // Get or create namespace PubSub (inline to avoid releasing lock)
  auto it = pubsub_namespaces_.find(ns);
  if (it == pubsub_namespaces_.end()) {
    auto pubsub = std::make_unique<NamespacePubSub>();
    it = pubsub_namespaces_.emplace(ns, std::move(pubsub)).first;
  }

  std::lock_guard<std::mutex> guard(it->second->mu);
  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD(), conn->GetID(), ns);
  if (auto iter = it->second->channels.find(channel); iter == it->second->channels.end()) {
    it->second->channels.emplace(channel, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }
}

void Server::UnsubscribeChannel(const std::string &channel, redis::Connection *conn) {
  const std::string &ns = conn->GetNamespace();

  std::shared_lock<std::shared_mutex> ns_map_lock(pubsub_namespaces_mu_);
  auto it = pubsub_namespaces_.find(ns);
  if (it == pubsub_namespaces_.end()) {
    return;
  }

  std::lock_guard<std::mutex> guard(it->second->mu);
  auto iter = it->second->channels.find(channel);
  if (iter == it->second->channels.end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->GetID() == conn_ctx.conn_id && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        it->second->channels.erase(iter);
      }
      break;
    }
  }
}

void Server::GetChannelsByPattern(const std::string &ns, const std::string &pattern, std::vector<std::string> *channels) {
  std::shared_lock<std::shared_mutex> ns_map_lock(pubsub_namespaces_mu_);

  // Each namespace (including admin/default) sees only their own channels
  auto it = pubsub_namespaces_.find(ns);
  if (it == pubsub_namespaces_.end()) {
    return;
  }

  std::lock_guard<std::mutex> guard(it->second->mu);
  for (const auto &iter : it->second->channels) {
    if (pattern.empty() || util::StringMatch(pattern, iter.first, false)) {
      channels->emplace_back(iter.first);
    }
  }
}

void Server::ListChannelSubscribeNum(const std::string &ns, const std::vector<std::string> &channels,
                                     std::vector<ChannelSubscribeNum> *channel_subscribe_nums) {
  std::shared_lock<std::shared_mutex> ns_map_lock(pubsub_namespaces_mu_);

  // Each namespace (including admin/default) sees only their own subscriber counts
  auto it = pubsub_namespaces_.find(ns);
  if (it == pubsub_namespaces_.end()) {
    // No subscriptions in this namespace, return all channels with 0 count
    for (const auto &chan : channels) {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{chan, 0});
    }
    return;
  }

  std::lock_guard<std::mutex> guard(it->second->mu);
  for (const auto &chan : channels) {
    if (auto iter = it->second->channels.find(chan); iter != it->second->channels.end()) {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{iter->first, iter->second.size()});
    } else {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{chan, 0});
    }
  }
}

void Server::PSubscribeChannel(const std::string &pattern, redis::Connection *conn) {
  const std::string &ns = conn->GetNamespace();

  // Use unique_lock for Subscribe since we may need to create the namespace entry.
  // This also protects against CleanupPubSubNamespace() deleting the entry.
  std::unique_lock<std::shared_mutex> ns_map_lock(pubsub_namespaces_mu_);

  // Get or create namespace PubSub (inline to avoid releasing lock)
  auto it = pubsub_namespaces_.find(ns);
  if (it == pubsub_namespaces_.end()) {
    auto pubsub = std::make_unique<NamespacePubSub>();
    it = pubsub_namespaces_.emplace(ns, std::move(pubsub)).first;
  }

  std::lock_guard<std::mutex> guard(it->second->mu);
  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD(), conn->GetID(), ns);
  if (auto iter = it->second->patterns.find(pattern); iter == it->second->patterns.end()) {
    it->second->patterns.emplace(pattern, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }
}

void Server::PUnsubscribeChannel(const std::string &pattern, redis::Connection *conn) {
  const std::string &ns = conn->GetNamespace();

  std::shared_lock<std::shared_mutex> ns_map_lock(pubsub_namespaces_mu_);
  auto it = pubsub_namespaces_.find(ns);
  if (it == pubsub_namespaces_.end()) {
    return;
  }

  std::lock_guard<std::mutex> guard(it->second->mu);
  auto iter = it->second->patterns.find(pattern);
  if (iter == it->second->patterns.end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->GetID() == conn_ctx.conn_id && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        it->second->patterns.erase(iter);
      }
      break;
    }
  }
}

void Server::SSubscribeChannel(const std::string &channel, redis::Connection *conn, uint16_t slot) {
  assert((config_->cluster_enabled && slot < HASH_SLOTS_SIZE) || slot == 0);
  std::lock_guard<std::mutex> guard(pubsub_shard_channels_mu_);

  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD(), conn->GetID(), conn->GetNamespace());
  if (auto iter = pubsub_shard_channels_[slot].find(channel); iter == pubsub_shard_channels_[slot].end()) {
    pubsub_shard_channels_[slot].emplace(channel, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }
}

void Server::SUnsubscribeChannel(const std::string &channel, redis::Connection *conn, uint16_t slot) {
  assert((config_->cluster_enabled && slot < HASH_SLOTS_SIZE) || slot == 0);
  std::lock_guard<std::mutex> guard(pubsub_shard_channels_mu_);

  auto iter = pubsub_shard_channels_[slot].find(channel);
  if (iter == pubsub_shard_channels_[slot].end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->GetID() == conn_ctx.conn_id && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        pubsub_shard_channels_[slot].erase(iter);
      }
      break;
    }
  }
}

void Server::GetSChannelsByPattern(const std::string &pattern, std::vector<std::string> *channels) {
  std::lock_guard<std::mutex> guard(pubsub_shard_channels_mu_);

  for (const auto &shard_channels : pubsub_shard_channels_) {
    for (const auto &iter : shard_channels) {
      if (pattern.empty() || util::StringMatch(pattern, iter.first, false)) {
        channels->emplace_back(iter.first);
      }
    }
  }
}

void Server::ListSChannelSubscribeNum(const std::vector<std::string> &channels,
                                      std::vector<ChannelSubscribeNum> *channel_subscribe_nums) {
  std::lock_guard<std::mutex> guard(pubsub_shard_channels_mu_);

  for (const auto &chan : channels) {
    uint16_t slot = config_->cluster_enabled ? GetSlotIdFromKey(chan) : 0;
    if (auto iter = pubsub_shard_channels_[slot].find(chan); iter != pubsub_shard_channels_[slot].end()) {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{iter->first, iter->second.size()});
    } else {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{chan, 0});
    }
  }
}

void Server::BlockOnKey(const std::string &key, redis::Connection *conn) {
  const std::string &ns = conn->GetNamespace();

  // Use unique_lock since we may need to create the namespace entry
  std::unique_lock<std::shared_mutex> ns_map_lock(blocking_keys_ns_mu_);

  // Get or create namespace entry (inline to avoid race condition)
  auto it = blocking_keys_by_ns_.find(ns);
  if (it == blocking_keys_by_ns_.end()) {
    it = blocking_keys_by_ns_.emplace(ns, std::make_unique<NamespaceBlockingKeys>()).first;
  }

  std::lock_guard<std::mutex> guard(it->second->mu);
  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD(), conn->GetID(), ns);

  if (auto iter = it->second->keys.find(key); iter == it->second->keys.end()) {
    it->second->keys.emplace(key, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }

  IncrBlockedClientNum();
}

void Server::UnblockOnKey(const std::string &key, redis::Connection *conn) {
  const std::string &ns = conn->GetNamespace();

  std::shared_lock<std::shared_mutex> ns_map_lock(blocking_keys_ns_mu_);
  auto it = blocking_keys_by_ns_.find(ns);
  if (it == blocking_keys_by_ns_.end()) {
    return;
  }

  std::lock_guard<std::mutex> guard(it->second->mu);
  auto iter = it->second->keys.find(key);
  if (iter == it->second->keys.end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->GetID() == conn_ctx.conn_id && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        it->second->keys.erase(iter);
      }
      break;
    }
  }

  DecrBlockedClientNum();
}

void Server::BlockOnStreams(const std::vector<std::string> &keys, const std::vector<redis::StreamEntryID> &entry_ids,
                            redis::Connection *conn) {
  const std::string &ns = conn->GetNamespace();

  // Use unique_lock since we may need to create the namespace entry
  std::unique_lock<std::shared_mutex> ns_map_lock(stream_consumers_ns_mu_);

  // Get or create namespace entry (inline to avoid race condition)
  auto it = stream_consumers_by_ns_.find(ns);
  if (it == stream_consumers_by_ns_.end()) {
    it = stream_consumers_by_ns_.emplace(ns, std::make_unique<NamespaceStreamConsumers>()).first;
  }

  std::lock_guard<std::mutex> guard(it->second->mu);
  IncrBlockedClientNum();

  for (size_t i = 0; i < keys.size(); ++i) {
    auto consumer = std::make_shared<StreamConsumer>(conn->Owner(), conn->GetFD(), conn->GetID(), ns, entry_ids[i]);
    if (auto iter = it->second->consumers.find(keys[i]); iter == it->second->consumers.end()) {
      std::set<std::shared_ptr<StreamConsumer>> consumers;
      consumers.insert(consumer);
      it->second->consumers.emplace(keys[i], consumers);
    } else {
      iter->second.insert(consumer);
    }
  }
}

void Server::UnblockOnStreams(const std::vector<std::string> &keys, redis::Connection *conn) {
  const std::string &ns = conn->GetNamespace();

  std::shared_lock<std::shared_mutex> ns_map_lock(stream_consumers_ns_mu_);
  auto it = stream_consumers_by_ns_.find(ns);
  if (it == stream_consumers_by_ns_.end()) {
    return;
  }

  std::lock_guard<std::mutex> guard(it->second->mu);
  DecrBlockedClientNum();

  for (const auto &key : keys) {
    auto iter = it->second->consumers.find(key);
    if (iter == it->second->consumers.end()) {
      continue;
    }

    for (auto consumer_it = iter->second.begin(); consumer_it != iter->second.end();) {
      const auto &consumer = *consumer_it;
      if (conn->GetFD() == consumer->fd && conn->GetID() == consumer->conn_id && conn->Owner() == consumer->owner) {
        iter->second.erase(consumer_it);
        if (iter->second.empty()) {
          it->second->consumers.erase(iter);
        }
        break;
      }
      ++consumer_it;
    }
  }
}

void Server::WakeupBlockingConns(const std::string &ns, const std::string &key, size_t n_conns) {
  std::vector<std::tuple<Worker *, int, uint64_t>> to_wakeup;

  {
    std::shared_lock<std::shared_mutex> ns_map_lock(blocking_keys_ns_mu_);
    auto it = blocking_keys_by_ns_.find(ns);
    if (it == blocking_keys_by_ns_.end()) {
      return;
    }

    std::lock_guard<std::mutex> guard(it->second->mu);
    auto iter = it->second->keys.find(key);
    if (iter == it->second->keys.end() || iter->second.empty()) {
      return;
    }

    while (n_conns-- && !iter->second.empty()) {
      auto conn_ctx = std::move(iter->second.front());
      iter->second.pop_front();
      to_wakeup.emplace_back(conn_ctx.owner, conn_ctx.fd, conn_ctx.conn_id);
    }
  }  // Lock released

  // I/O outside lock - EnableWriteEvent validates fd internally
  for (const auto &[owner, fd, conn_id] : to_wakeup) {
    auto s = owner->EnableWriteEventByID(fd, conn_id);
    if (!s.IsOK()) {
      error("[server] Failed to enable write event on blocked client {}: {}", fd, s.Msg());
    }
  }
}

void Server::OnEntryAddedToStream(const std::string &ns, const std::string &key, const redis::StreamEntryID &entry_id) {
  std::vector<std::tuple<Worker *, int, uint64_t>> to_wakeup;

  {
    std::shared_lock<std::shared_mutex> ns_map_lock(stream_consumers_ns_mu_);
    auto ns_it = stream_consumers_by_ns_.find(ns);
    if (ns_it == stream_consumers_by_ns_.end()) {
      return;
    }

    std::lock_guard<std::mutex> guard(ns_it->second->mu);
    auto iter = ns_it->second->consumers.find(key);
    if (iter == ns_it->second->consumers.end() || iter->second.empty()) {
      return;
    }

    for (auto it = iter->second.begin(); it != iter->second.end();) {
      auto consumer = *it;
      // No need to check consumer->ns == ns since we already filtered by namespace
      if (entry_id > consumer->last_consumed_id) {
        to_wakeup.emplace_back(consumer->owner, consumer->fd, consumer->conn_id);
        it = iter->second.erase(it);
      } else {
        ++it;
      }
    }
  }  // Lock released

  // I/O outside lock - EnableWriteEvent validates fd internally
  for (const auto &[owner, fd, conn_id] : to_wakeup) {
    auto s = owner->EnableWriteEventByID(fd, conn_id);
    if (!s.IsOK()) {
      error("[server] Failed to enable write event on blocked stream consumer {}: {}", fd, s.Msg());
    }
  }
}

void Server::CleanupBlockingNamespace(const std::string &ns) {
  {
    std::unique_lock<std::shared_mutex> lock(blocking_keys_ns_mu_);
    blocking_keys_by_ns_.erase(ns);
  }
  {
    std::unique_lock<std::shared_mutex> lock(stream_consumers_ns_mu_);
    stream_consumers_by_ns_.erase(ns);
  }
}

void Server::BlockOnWait(redis::Connection *conn, rocksdb::SequenceNumber target_seq, uint64_t num_replicas) {
  std::unique_lock<std::shared_mutex> guard(wait_contexts_mu_);

  wait_contexts_.emplace(target_seq, WaitContext(conn, target_seq, num_replicas));
  IncrBlockedClientNum();
}

void Server::WakeupWaitConnections(rocksdb::SequenceNumber seq) {
  std::vector<std::tuple<Worker *, int, uint64_t, size_t>> to_wakeup;

  {
    std::unique_lock<std::shared_mutex> guard(wait_contexts_mu_);

    // find the last entry with target_seq > seq, which cannot wakeup
    auto end_it = wait_contexts_.upper_bound(seq);
    for (auto it = wait_contexts_.begin(); it != end_it;) {
      // Count how many replicas have reached the target sequence
      size_t reached_replicas = GetReplicasReachedSequence(it->second.target_seq);

      // If enough replicas have reached the target sequence, collect for wakeup
      if (reached_replicas >= it->second.num_replicas) {
        to_wakeup.emplace_back(it->second.owner, it->second.fd, it->second.conn_id, reached_replicas);
        it = wait_contexts_.erase(it);
        continue;
      }
      ++it;
    }
  }  // Lock released

  // I/O + counter outside lock - Worker::Reply validates fd internally
  for (const auto &[owner, fd, conn_id, reached_replicas] : to_wakeup) {
    auto reply = redis::Integer(reached_replicas);
    auto s = owner->ReplyByID(fd, conn_id, reply);
    if (s.IsOK()) {
      std::ignore = owner->EnableWriteEventByID(fd, conn_id);
    }
    // Note: Connection might already be closed, but counter must still be decremented
    DecrBlockedClientNum();
  }
}

void Server::WakeupWaitConnection(redis::Connection *conn, rocksdb::SequenceNumber seq) {
  std::unique_lock<std::shared_mutex> guard(wait_contexts_mu_);
  cleanupWaitConnection(conn);

  size_t reached_replicas = GetReplicasReachedSequence(seq);
  auto reply = redis::Integer(reached_replicas);
  auto s = conn->Owner()->ReplyByID(conn->GetFD(), conn->GetID(), reply);

  if (s.IsOK()) {
    s = conn->Owner()->EnableWriteEventByID(conn->GetFD(), conn->GetID());
  }
  if (!s.IsOK()) {
    error("[server] Failed to enable write event on WAIT connection {}: {}", conn->GetFD(), s.Msg());
  }
}

void Server::CleanupWaitConnection(redis::Connection *conn) {
  std::unique_lock<std::shared_mutex> guard(wait_contexts_mu_);
  cleanupWaitConnection(conn);
}

void Server::cleanupWaitConnection(redis::Connection *conn) {
  // Remove all wait contexts that match the given connection
  auto it = wait_contexts_.begin();
  int erased_count = 0;
  while (it != wait_contexts_.end()) {
    if (it->second.owner == conn->Owner() && it->second.fd == conn->GetFD() && it->second.conn_id == conn->GetID()) {
      it = wait_contexts_.erase(it);
      erased_count++;
      // Technically only one client is unblocked, but we call IncrBlockedClientNum for each added wait context,
      // so we need to call DecrBlockedClientNum for each erased wait context.
      // Multiple wait contexts on the same connection should not happen, but we should be defensive.
      DecrBlockedClientNum();
    } else {
      ++it;
    }
  }

  if (erased_count > 1) {
    warn("[server] {} wait contexts found for connection with fd {}, expect 1", erased_count, conn->GetFD());
  }
}

size_t Server::GetReplicasReachedSequence(rocksdb::SequenceNumber target_seq) {
  std::shared_lock<std::shared_mutex> slave_guard(slave_threads_mu_);
  size_t reached_replicas = 0;
  for (const auto &slave : slave_threads_) {
    if (!slave->IsStopped() && slave->GetAckSeq() >= target_seq) {
      reached_replicas++;
    }
  }
  return reached_replicas;
}

rocksdb::SequenceNumber Server::LargestTargetSeqToWakeup(rocksdb::SequenceNumber seq) {
  std::shared_lock<std::shared_mutex> guard(wait_contexts_mu_);
  if (wait_contexts_.empty()) {
    return 0;
  }

  // Use upper_bound to find the first entry with target_seq > seq
  // the largest seq that can wakeup is the last element before it
  auto it = wait_contexts_.upper_bound(seq);

  // when wait_contexts_ is empty, it == wait_contexts_.begin().
  // when all wait_contexts_.target_seq > seq, it == wait_contexts_.begin().
  // both cases should return 0.
  if (it == wait_contexts_.begin()) {
    return 0;
  }

  // Return the largest target_seq that could potentially be unblocked
  auto last_it = std::prev(it);
  return last_it->second.target_seq;
}

void Server::updateCachedTime() { unix_time_secs.store(util::GetTimeStamp()); }

int Server::IncrClientNum() {
  total_clients_.fetch_add(1, std::memory_order_relaxed);
  return connected_clients_.fetch_add(1, std::memory_order_relaxed);
}

int Server::DecrClientNum() { return connected_clients_.fetch_sub(1, std::memory_order_relaxed); }

int Server::IncrMonitorClientNum() { return monitor_clients_.fetch_add(1, std::memory_order_relaxed); }

int Server::DecrMonitorClientNum() { return monitor_clients_.fetch_sub(1, std::memory_order_relaxed); }

int Server::IncrBlockedClientNum() { return blocked_clients_.fetch_add(1, std::memory_order_relaxed); }

int Server::DecrBlockedClientNum() { return blocked_clients_.fetch_sub(1, std::memory_order_relaxed); }

int Server::GetBlockedClientsCount(redis::Connection *self) {
  // Admin sees global counter (O(1))
  if (self->IsAdmin()) {
    return blocked_clients_.load(std::memory_order_relaxed);
  }

  const std::string &ns = self->GetNamespace();
  int count = 0;

  // 1. blocking_keys (BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP)
  {
    std::shared_lock<std::shared_mutex> ns_map_lock(blocking_keys_ns_mu_);
    auto it = blocking_keys_by_ns_.find(ns);
    if (it != blocking_keys_by_ns_.end()) {
      std::lock_guard<std::mutex> guard(it->second->mu);
      for (const auto &[key, contexts] : it->second->keys) {
        count += static_cast<int>(contexts.size());
      }
    }
  }

  // 2. stream_consumers (XREAD BLOCK, XREADGROUP BLOCK)
  {
    std::shared_lock<std::shared_mutex> ns_map_lock(stream_consumers_ns_mu_);
    auto it = stream_consumers_by_ns_.find(ns);
    if (it != stream_consumers_by_ns_.end()) {
      std::lock_guard<std::mutex> guard(it->second->mu);
      for (const auto &[key, consumers] : it->second->consumers) {
        count += static_cast<int>(consumers.size());
      }
    }
  }

  // 3. wait_contexts_ (WAIT)
  {
    std::shared_lock<std::shared_mutex> guard(wait_contexts_mu_);
    for (const auto &[seq, ctx] : wait_contexts_) {
      if (ctx.ns == ns) count++;
    }
  }

  return count;
}

std::shared_lock<std::shared_mutex> Server::WorkConcurrencyGuard() {
  return std::shared_lock(works_concurrency_rw_lock_);
}

std::unique_lock<std::shared_mutex> Server::WorkExclusivityGuard() {
  return std::unique_lock(works_concurrency_rw_lock_);
}

std::shared_lock<std::shared_mutex> Server::WorkConcurrencyGuard(const std::string &ns) {
  std::shared_mutex *ns_lock = nullptr;

  // Fast-path: shared_lock for existing namespaces (common case)
  {
    std::shared_lock map_lock(ns_locks_mutex_);
    auto it = ns_locks_.find(ns);
    if (it != ns_locks_.end()) {
      ns_lock = &it->second;
    }
  }

  // Slow-path: unique_lock only when namespace is new (rare)
  if (!ns_lock) {
    std::unique_lock map_lock(ns_locks_mutex_);
    ns_lock = &ns_locks_[ns];
  }

  return std::shared_lock(*ns_lock);
}

std::unique_lock<std::shared_mutex> Server::WorkExclusivityGuard(const std::string &ns) {
  std::shared_mutex *ns_lock = nullptr;

  // Fast-path: shared_lock for existing namespaces (common case)
  {
    std::shared_lock map_lock(ns_locks_mutex_);
    auto it = ns_locks_.find(ns);
    if (it != ns_locks_.end()) {
      ns_lock = &it->second;
    }
  }

  // Slow-path: unique_lock only when namespace is new (rare)
  if (!ns_lock) {
    std::unique_lock map_lock(ns_locks_mutex_);
    ns_lock = &ns_locks_[ns];
  }

  return std::unique_lock(*ns_lock);
}

uint64_t Server::GetClientID() { return client_id_.fetch_add(1, std::memory_order_relaxed); }

void Server::recordInstantaneousMetrics() {
  auto rocksdb_stats = storage->GetDB()->GetDBOptions().statistics;
  stats.TrackInstantaneousMetric(STATS_METRIC_COMMAND, stats.total_calls);
  stats.TrackInstantaneousMetric(STATS_METRIC_NET_INPUT, stats.in_bytes);
  stats.TrackInstantaneousMetric(STATS_METRIC_NET_OUTPUT, stats.out_bytes);
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_PUT,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_WRITTEN));
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_GET,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_READ));
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_MULTIGET,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_MULTIGET_KEYS_READ));
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_SEEK,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_SEEK));
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_NEXT,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_NEXT));
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_PREV,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_PREV));
}

void Server::cron() {
  uint64_t counter = 0;
  while (!stop_) {
    // Sleep first
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // To guarantee accessing DB safely
    auto guard = storage->ReadLockGuard();
    if (storage->IsClosing()) continue;

    updateCachedTime();
    counter++;

    if (is_loading_) {
      // We need to skip the cron operations since `is_loading_` means the db is restoring,
      // and the db pointer will be modified after that. It will panic if we use the db pointer
      // before the new db was reopened.
      continue;
    }

    // check every 20s (use 20s instead of 60s so that cron will execute in critical condition)
    if (counter != 0 && counter % 200 == 0) {
      auto t = static_cast<time_t>(util::GetTimeStamp());
      std::tm now{};
      localtime_r(&t, &now);
      // disable compaction cron when the compaction checker was enabled
      if (!config_->compaction_checker_cron.IsEnabled() && config_->compact_cron.IsEnabled() &&
          config_->compact_cron.IsTimeMatch(&now)) {
        Status s = AsyncCompactDB();
        info("[server] Schedule to compact the db, result: {}", s.Msg());
      }
      if (config_->bgsave_cron.IsEnabled() && config_->bgsave_cron.IsTimeMatch(&now)) {
        Status s = AsyncBgSaveDB();
        info("[server] Schedule to bgsave the db, result: {}", s.Msg());
      }
      if (config_->dbsize_scan_cron.IsEnabled() && config_->dbsize_scan_cron.IsTimeMatch(&now)) {
        auto tokens = namespace_.List();
        std::vector<std::string> namespaces;

        // Number of namespaces (custom namespaces + default one)
        namespaces.reserve(tokens.size() + 1);
        for (auto &token : tokens) {
          namespaces.emplace_back(token.second);  // namespace
        }

        // add default namespace as fallback
        namespaces.emplace_back(kDefaultNamespace);

        for (auto &ns : namespaces) {
          Status s = AsyncScanDBSize(ns);
          info("[server] Schedule to recalculate the db size on namespace: {}, result: {}", ns, s.Msg());
        }
      }
    }
    // check every 10s
    if (counter != 0 && counter % 100 == 0) {
      Status s = AsyncPurgeOldBackups(config_->max_backup_to_keep, config_->max_backup_keep_hours);

      // Purge backup if needed, it will cost much disk space if we keep backup and full sync
      // checkpoints at the same time
      if (config_->purge_backup_on_fullsync && (storage->ExistCheckpoint() || storage->ExistSyncCheckpoint())) {
        s = AsyncPurgeOldBackups(0, 0);
      }
    }

    // No replica uses this checkpoint, we can remove it.
    if (counter != 0 && counter % 100 == 0) {
      int64_t create_time_secs = storage->GetCheckpointCreateTimeSecs();
      int64_t access_time_secs = storage->GetCheckpointAccessTimeSecs();

      if (storage->ExistCheckpoint()) {
        // TODO(shooterit): support to config the alive time of checkpoint
        int64_t now_secs = util::GetTimeStamp<std::chrono::seconds>();
        if ((GetFetchFileThreadNum() == 0 && now_secs - access_time_secs > 30) ||
            (now_secs - create_time_secs > 24 * 60 * 60)) {
          auto s = rocksdb::DestroyDB(config_->checkpoint_dir, rocksdb::Options());
          if (!s.ok()) {
            warn("[server] Fail to clean checkpoint, error: {}", s.ToString());
          } else {
            info("[server] Clean checkpoint successfully");
          }
        }
      }
    }
    // check if DB need to be resumed every minute
    // Rocksdb has auto resume feature after retryable io error, earlier version(before v6.22.1) had
    // bug when encounter no space error. The current version fixes the no space error issue, but it
    // does not completely resolve, which still exists when encountered disk quota exceeded error.
    // In order to properly handle all possible situations on rocksdb, we manually resume here
    // when encountering no space error and disk quota exceeded error.
    if (counter != 0 && counter % 600 == 0 && storage->IsDBInRetryableIOError()) {
      auto s = storage->GetDB()->Resume();
      if (s.ok()) {
        warn("[server] Successfully resumed DB after retryable IO error");
      } else {
        error("[server] Failed to resume DB after retryable IO error: {}", s.ToString());
      }
      storage->SetDBInRetryableIOError(false);
    }

    // check if we need to clean up exited worker threads every 5s
    if (counter != 0 && counter % 50 == 0) {
      cleanupExitedWorkerThreads(false);
    }

    // Cleanup inactive SNIs from fair scheduler every 60s
    if (counter != 0 && counter % 600 == 0 && fair_scheduler_) {
      fair_scheduler_->CleanupInactiveSNIs();
    }

    CleanupExitedSlaves();
    recordInstantaneousMetrics();
  }
}

Server::InfoEntries Server::GetRocksDBInfo() {
  InfoEntries entries;
  if (is_loading_) return entries;

  rocksdb::DB *db = storage->GetDB();

  uint64_t memtable_sizes = 0, cur_memtable_sizes = 0, num_snapshots = 0, num_running_flushes = 0;
  uint64_t num_immutable_tables = 0, memtable_flush_pending = 0, compaction_pending = 0;
  uint64_t num_running_compaction = 0, num_live_versions = 0, num_super_version = 0, num_background_errors = 0;

  db->GetAggregatedIntProperty(rocksdb::DB::Properties::kSizeAllMemTables, &memtable_sizes);
  db->GetAggregatedIntProperty(rocksdb::DB::Properties::kCurSizeAllMemTables, &cur_memtable_sizes);
  db->GetAggregatedIntProperty(rocksdb::DB::Properties::kNumImmutableMemTable, &num_immutable_tables);
  db->GetAggregatedIntProperty(rocksdb::DB::Properties::kMemTableFlushPending, &memtable_flush_pending);
  db->GetAggregatedIntProperty(rocksdb::DB::Properties::kCurrentSuperVersionNumber, &num_super_version);
  db->GetAggregatedIntProperty(rocksdb::DB::Properties::kBackgroundErrors, &num_background_errors);
  db->GetAggregatedIntProperty(rocksdb::DB::Properties::kCompactionPending, &compaction_pending);
  db->GetAggregatedIntProperty(rocksdb::DB::Properties::kNumLiveVersions, &num_live_versions);

  {
    // All column families share the same block cache, so it's good to count a single one.
    uint64_t block_cache_usage = 0;
    uint64_t block_cache_pinned_usage = 0;
    auto subkey_cf_handle = storage->GetCFHandle(ColumnFamilyID::PrimarySubkey);
    db->GetIntProperty(subkey_cf_handle, rocksdb::DB::Properties::kBlockCacheUsage, &block_cache_usage);
    entries.emplace_back("block_cache_usage", block_cache_usage);
    db->GetIntProperty(subkey_cf_handle, rocksdb::DB::Properties::kBlockCachePinnedUsage, &block_cache_pinned_usage);
    entries.emplace_back("block_cache_pinned_usage[" + subkey_cf_handle->GetName() + "]", block_cache_pinned_usage);

    // All column faimilies share the same property of the DB, so it's good to count a single one.
    db->GetIntProperty(subkey_cf_handle, rocksdb::DB::Properties::kNumSnapshots, &num_snapshots);
    db->GetIntProperty(subkey_cf_handle, rocksdb::DB::Properties::kNumRunningFlushes, &num_running_flushes);
    db->GetIntProperty(subkey_cf_handle, rocksdb::DB::Properties::kNumRunningCompactions, &num_running_compaction);
  }

  for (const auto &cf_handle : *storage->GetCFHandles()) {
    uint64_t estimate_keys = 0;
    uint64_t index_and_filter_cache_usage = 0;
    std::map<std::string, std::string> cf_stats_map;
    db->GetIntProperty(cf_handle, rocksdb::DB::Properties::kEstimateNumKeys, &estimate_keys);
    entries.emplace_back("estimate_keys[" + cf_handle->GetName() + "]", estimate_keys);
    db->GetIntProperty(cf_handle, rocksdb::DB::Properties::kEstimateTableReadersMem, &index_and_filter_cache_usage);
    entries.emplace_back("index_and_filter_cache_usage[" + cf_handle->GetName() + "]", index_and_filter_cache_usage);
    db->GetMapProperty(cf_handle, rocksdb::DB::Properties::kCFStats, &cf_stats_map);
    entries.emplace_back("level0_file_limit_slowdown[" + cf_handle->GetName() + "]",
                         cf_stats_map["l0-file-count-limit-delays"]);
    entries.emplace_back("level0_file_limit_stop[" + cf_handle->GetName() + "]",
                         cf_stats_map["l0-file-count-limit-stops"]);
    entries.emplace_back("pending_compaction_bytes_slowdown[" + cf_handle->GetName() + "]",
                         cf_stats_map["pending-compaction-bytes-delays"]);
    entries.emplace_back("pending_compaction_bytes_stop[" + cf_handle->GetName() + "]",
                         cf_stats_map["pending-compaction-bytes-stops"]);
    entries.emplace_back("level0_file_limit_stop_with_ongoing_compaction[" + cf_handle->GetName() + "]",
                         cf_stats_map["cf-l0-file-count-limit-stops-with-ongoing-compaction"]);
    entries.emplace_back("level0_file_limit_slowdown_with_ongoing_compaction[" + cf_handle->GetName() + "]",
                         cf_stats_map["cf-l0-file-count-limit-delays-with-ongoing-compaction"]);
    entries.emplace_back("memtable_count_limit_slowdown[" + cf_handle->GetName() + "]",
                         cf_stats_map["memtable-limit-delays"]);
    entries.emplace_back("memtable_count_limit_stop[" + cf_handle->GetName() + "]",
                         cf_stats_map["memtable-limit-stops"]);

    // Get the SST file count in all levels
    std::string sst_file_at_level = "[";
    for (int level = 0; level < KVROCKS_MAX_LSM_LEVEL; level++) {
      std::string sst_file_count;
      db->GetProperty(cf_handle, rocksdb::DB::Properties::kNumFilesAtLevelPrefix + std::to_string(level),
                      &sst_file_count);
      if (level != 0) {
        sst_file_at_level += ",";
      }
      sst_file_at_level += sst_file_count;
    }
    entries.emplace_back("num_files_at_level[" + cf_handle->GetName() + "]", sst_file_at_level + "]");

    // Get the estimate pending compaction bytes for the current column family
    std::string estimate_pending_compaction_bytes;
    db->GetProperty(cf_handle, rocksdb::DB::Properties::kEstimatePendingCompactionBytes,
                    &estimate_pending_compaction_bytes);
    entries.emplace_back("estimate_pending_compaction_bytes[" + cf_handle->GetName() + "]",
                         estimate_pending_compaction_bytes);
  }

  auto rocksdb_stats = storage->GetDB()->GetDBOptions().statistics;
  if (rocksdb_stats) {
    std::map<std::string, uint32_t> block_cache_stats = {
        {"block_cache_hit", rocksdb::Tickers::BLOCK_CACHE_HIT},
        {"block_cache_index_hit", rocksdb::Tickers::BLOCK_CACHE_INDEX_HIT},
        {"block_cache_filter_hit", rocksdb::Tickers::BLOCK_CACHE_FILTER_HIT},
        {"block_cache_data_hit", rocksdb::Tickers::BLOCK_CACHE_DATA_HIT},
        {"block_cache_miss", rocksdb::Tickers::BLOCK_CACHE_MISS},
        {"block_cache_index_miss", rocksdb::Tickers::BLOCK_CACHE_INDEX_MISS},
        {"block_cache_filter_miss", rocksdb::Tickers::BLOCK_CACHE_FILTER_MISS},
        {"block_cache_data_miss", rocksdb::Tickers::BLOCK_CACHE_DATA_MISS},
    };
    for (const auto &iter : block_cache_stats) {
      entries.emplace_back(iter.first, rocksdb_stats->getTickerCount(iter.second));
    }
  }

  entries.emplace_back("all_mem_tables", memtable_sizes);
  entries.emplace_back("cur_mem_tables", cur_memtable_sizes);
  entries.emplace_back("snapshots", num_snapshots);
  entries.emplace_back("num_immutable_tables", num_immutable_tables);
  entries.emplace_back("num_running_flushes", num_running_flushes);
  entries.emplace_back("memtable_flush_pending", memtable_flush_pending);
  entries.emplace_back("compaction_pending", compaction_pending);
  entries.emplace_back("num_running_compactions", num_running_compaction);
  entries.emplace_back("num_live_versions", num_live_versions);
  entries.emplace_back("num_super_version", num_super_version);
  entries.emplace_back("num_background_errors", num_background_errors);
  auto db_stats = storage->GetDBStats();
  entries.emplace_back("flush_count", db_stats->flush_count.load());
  entries.emplace_back("compaction_count", db_stats->compaction_count.load());
  entries.emplace_back("put_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_PUT));
  entries.emplace_back("get_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_GET) +
                                          stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_MULTIGET));
  entries.emplace_back("seek_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_SEEK));
  entries.emplace_back("next_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_NEXT));
  entries.emplace_back("prev_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_PREV));
  {
    std::lock_guard<std::mutex> lg(db_job_mu_);
    entries.emplace_back("is_bgsaving", (is_bgsave_in_progress_ ? "yes" : "no"));
    entries.emplace_back("is_compacting", (db_compacting_ ? "yes" : "no"));
  }

  return entries;
}

Server::InfoEntries Server::GetServerInfo() {
  static int call_uname = 1;
  static utsname name;
  if (call_uname) {
    /* Uname can be slow and is always the same output. Cache it. */
    uname(&name);
    call_uname = 0;
  }

  Server::InfoEntries entries;
  entries.emplace_back("version", VERSION);  // deprecated
  entries.emplace_back("kvrocks_version", VERSION);
  entries.emplace_back("redis_version", REDIS_VERSION);
  entries.emplace_back("git_sha1", GIT_COMMIT);  // deprecated
  entries.emplace_back("kvrocks_git_sha1", GIT_COMMIT);
  entries.emplace_back("redis_mode", (config_->cluster_enabled ? "cluster" : "standalone"));
  entries.emplace_back("kvrocks_mode", (config_->cluster_enabled ? "cluster" : "standalone"));
  entries.emplace_back("os", fmt::format("{} {} {}", name.sysname, name.release, name.machine));
#if defined(__GNUC__) && !defined(__clang__)
  entries.emplace_back("gcc_version", fmt::format("{}.{}.{}", __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__));
#endif
#ifdef __clang__
  entries.emplace_back("clang_version",
                       fmt::format("{}.{}.{}", __clang_major__, __clang_minor__, __clang_patchlevel__));
#endif
  entries.emplace_back("rocksdb_version", fmt::format("{}.{}.{}", ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH));
  entries.emplace_back("arch_bits", sizeof(void *) * 8);
  entries.emplace_back("process_id", getpid());
  entries.emplace_back("tcp_port", config_->port);
  entries.emplace_back("server_time_usec", util::GetTimeStampUS());
  int64_t now_secs = util::GetTimeStamp<std::chrono::seconds>();
  entries.emplace_back("uptime_in_seconds", now_secs - start_time_secs_);
  entries.emplace_back("uptime_in_days", (now_secs - start_time_secs_) / 86400);
#ifdef __linux__
  if (auto exec_path = realpath("/proc/self/exe", nullptr)) {
    entries.emplace_back("executable", exec_path);
    free(exec_path);  // NOLINT(cppcoreguidelines-no-malloc)
  }
#endif
  entries.emplace_back("config_file", config_->ConfigFilePath());
  return entries;
}

Server::InfoEntries Server::GetClientsInfo(redis::Connection *self) {
  InfoEntries entries;
  entries.emplace_back("maxclients", config_->maxclients);

  // Count clients per namespace (admin sees all, tenants see only their own)
  int connected = 0, monitor = 0;
  if (auto snapshot = GetWorkerSnapshot()) {
    for (const auto &worker : *snapshot) {
      auto counts = worker->GetClientCounts(self);
      connected += counts.connected;
      monitor += counts.monitor;
    }
  }

  entries.emplace_back("connected_clients", connected);
  entries.emplace_back("monitor_clients", monitor);
  entries.emplace_back("blocked_clients", GetBlockedClientsCount(self));
  return entries;
}

Server::InfoEntries Server::GetSchedulerInfo() {
  InfoEntries entries;
  if (!fair_scheduler_) return entries;

  auto sni_stats = fair_scheduler_->GetSNIStats();
  std::sort(sni_stats.begin(), sni_stats.end(),
            [](const SNIStats &a, const SNIStats &b) { return a.sni < b.sni; });

  entries.emplace_back("active_snis", fair_scheduler_->GetActiveSNICountRaw());
  entries.emplace_back("tracked_snis", sni_stats.size());

  for (const auto &stats : sni_stats) {
    entries.emplace_back("sni[" + stats.sni + "]",
                         fmt::format("active_connections={},fair_share={},overdraft_limit={},target_pool_size={}",
                                     stats.active_connections, stats.fair_share, stats.overdraft_limit,
                                     stats.target_pool_size));
  }

  return entries;
}

Server::InfoEntries Server::GetMemoryInfo(redis::Connection *conn) {
  int64_t rss = Stats::GetMemoryRSS();
  std::string used_memory_rss_human = util::BytesToHuman(rss);

  InfoEntries entries;
  entries.emplace_back("used_memory_rss", rss);
  entries.emplace_back("used_memory_rss_human", used_memory_rss_human);

  // Lua memory only for admin (Lua VM is shared per worker, not per namespace)
  if (conn->IsAdmin()) {
    int64_t memory_lua = 0;
    if (auto snapshot = GetWorkerSnapshot()) {
      for (auto &worker : *snapshot) {
        memory_lua += worker->GetLuaMemorySize();
      }
    }
    std::string used_memory_lua_human = util::BytesToHuman(memory_lua);
    entries.emplace_back("used_memory_lua", memory_lua);
    entries.emplace_back("used_memory_lua_human", used_memory_lua_human);
  }

  entries.emplace_back("used_memory_startup", memory_startup_use_.load(std::memory_order_relaxed));
  entries.emplace_back("mem_allocator", memory_profiler.AllocatorName());
  return entries;
}

Server::InfoEntries Server::GetReplicationInfo() {
  InfoEntries entries;
  if (is_loading_) return entries;

  entries.emplace_back("role", (IsSlave() ? "slave" : "master"));
  if (IsSlave()) {
    int64_t now_secs = util::GetTimeStamp<std::chrono::seconds>();
    entries.emplace_back("master_host", master_host_);
    entries.emplace_back("master_port", master_port_);
    ReplState state = GetReplicationState();
    entries.emplace_back("master_link_status", (state == kReplConnected ? "up" : "down"));
    entries.emplace_back("master_sync_unrecoverable_error", (state == kReplError ? "yes" : "no"));
    entries.emplace_back("master_sync_in_progress", (state == kReplFetchMeta || state == kReplFetchSST));
    entries.emplace_back("master_last_io_seconds_ago", now_secs - replication_thread_->LastIOTimeSecs());
    entries.emplace_back("slave_repl_offset", storage->LatestSeqNumber());
    entries.emplace_back("slave_priority", config_->slave_priority);
  }

  int idx = 0;
  rocksdb::SequenceNumber latest_seq = storage->LatestSeqNumber();

  {
    std::shared_lock<std::shared_mutex> guard(slave_threads_mu_);
    entries.emplace_back("connected_slaves", slave_threads_.size());
    for (const auto &slave : slave_threads_) {
      if (slave->IsStopped()) continue;

      entries.emplace_back(
          "slave" + std::to_string(idx),
          fmt::format("ip={},port={},offset={},lag={}", slave->GetConn()->GetAnnounceIP(),
                      slave->GetConn()->GetAnnouncePort(), slave->GetAckSeq(), latest_seq - slave->GetAckSeq()));
      ++idx;
    }
  }

  entries.emplace_back("master_repl_offset", latest_seq);

  return entries;
}

std::string Server::GetRoleInfo() {
  if (IsSlave()) {
    std::vector<std::string> roles;
    roles.emplace_back("slave");
    roles.emplace_back(master_host_);
    roles.emplace_back(std::to_string(master_port_));

    auto state = GetReplicationState();
    if (state == kReplConnected) {
      roles.emplace_back("connected");
    } else if (state == kReplFetchMeta || state == kReplFetchSST) {
      roles.emplace_back("sync");
    } else {
      roles.emplace_back("connecting");
    }
    roles.emplace_back(std::to_string(storage->LatestSeqNumber()));
    return redis::ArrayOfBulkStrings(roles);
  } else {
    std::vector<std::string> list;

    {
      std::shared_lock<std::shared_mutex> guard(slave_threads_mu_);
      for (const auto &slave : slave_threads_) {
        if (slave->IsStopped()) continue;

        list.emplace_back(redis::ArrayOfBulkStrings({
            slave->GetConn()->GetAnnounceIP(),
            std::to_string(slave->GetConn()->GetListeningPort()),
            std::to_string(slave->GetAckSeq()),
        }));
      }
    }

    auto multi_len = 2;
    if (list.size() > 0) {
      multi_len = 3;
    }
    std::string info;
    info.append(redis::MultiLen(multi_len));
    info.append(redis::BulkString("master"));
    info.append(redis::BulkString(std::to_string(storage->LatestSeqNumber())));
    if (list.size() > 0) {
      info.append(redis::Array(list));
    }
    return info;
  }
}

std::string Server::GetLastRandomKeyCursor() {
  std::string cursor;
  std::lock_guard<std::mutex> guard(last_random_key_cursor_mu_);
  cursor = last_random_key_cursor_;
  return cursor;
}

void Server::SetLastRandomKeyCursor(const std::string &cursor) {
  std::lock_guard<std::mutex> guard(last_random_key_cursor_mu_);
  last_random_key_cursor_ = cursor;
}

int64_t Server::GetCachedUnixTime() {
  if (unix_time_secs.load() == 0) {
    updateCachedTime();
  }
  return unix_time_secs.load();
}

int64_t Server::GetLastBgsaveTime() {
  std::lock_guard<std::mutex> lg(db_job_mu_);
  return last_bgsave_timestamp_secs_ == -1 ? start_time_secs_ : last_bgsave_timestamp_secs_;
}

NamespaceStatsSnapshot Server::AggregateNamespaceStats(const std::string &ns) {
  NamespaceStatsSnapshot result;
  if (auto snapshot = GetWorkerSnapshot()) {
    for (const auto &worker : *snapshot) {
      // Optimization: Only fetch stats for requested namespace, not entire map
      auto worker_stats = worker->GetNamespaceStats(ns);
      result.total_calls += worker_stats.total_calls;
      result.in_bytes += worker_stats.in_bytes;
      result.out_bytes += worker_stats.out_bytes;
      result.total_connections += worker_stats.total_connections;
    }
  }
  return result;
}

std::map<std::string, CommandStatSnapshot> Server::AggregateCommandStatsForNamespace(const std::string &ns) {
  std::map<std::string, CommandStatSnapshot> result;
  if (auto snapshot = GetWorkerSnapshot()) {
    for (const auto &worker : *snapshot) {
      auto worker_stats = worker->GetCommandStatsForNamespace(ns);
      for (const auto &[cmd, stat] : worker_stats) {
        result[cmd].calls += stat.calls;
        result[cmd].latency += stat.latency;
      }
    }
  }
  return result;
}

NamespaceRates Server::GetInstantaneousRatesForNamespace(const std::string &ns) {
  auto now = util::GetTimeStampMS();

  // FAST PATH: Cache Hit mit shared_lock (parallel für ALLE Tenants!)
  {
    std::shared_lock<std::shared_mutex> lock(ns_rate_mu_);
    auto it = ns_rate_trackers_.find(ns);
    if (it != ns_rate_trackers_.end()) {
      if (now < it->second->cache_valid_until.load(std::memory_order_acquire)) {
        // Cache gültig - lock-free read der atomics (kein per-NS lock nötig!)
        return {
            it->second->cached_ops_rate.load(std::memory_order_relaxed),
            it->second->cached_in_rate.load(std::memory_order_relaxed),
            it->second->cached_out_rate.load(std::memory_order_relaxed)};
      }
    }
  }

  // SLOW PATH: Cache Miss - erst Map-Eintrag holen/erstellen
  NamespaceRateData *data = nullptr;
  {
    std::shared_lock<std::shared_mutex> lock(ns_rate_mu_);
    auto it = ns_rate_trackers_.find(ns);
    if (it != ns_rate_trackers_.end()) {
      data = it->second.get();
    }
  }

  // Falls nicht vorhanden: Erstellen (seltener Fall - nur bei erstem INFO pro NS)
  if (!data) {
    std::unique_lock<std::shared_mutex> lock(ns_rate_mu_);
    // Double-check nach Lock-Upgrade
    auto it = ns_rate_trackers_.find(ns);
    if (it == ns_rate_trackers_.end()) {
      ns_rate_trackers_[ns] = std::make_unique<NamespaceRateData>();
    }
    data = ns_rate_trackers_[ns].get();
  }

  // PER-NS LOCK: Nur DIESER Namespace ist blockiert, andere laufen parallel!
  std::lock_guard<std::mutex> ns_lock(data->mu);

  // Double-check nach per-NS Lock (anderer Thread könnte Rate schon berechnet haben)
  if (now < data->cache_valid_until.load(std::memory_order_acquire)) {
    return {
        data->cached_ops_rate.load(std::memory_order_relaxed),
        data->cached_in_rate.load(std::memory_order_relaxed),
        data->cached_out_rate.load(std::memory_order_relaxed)};
  }

  // Stats aggregieren (O(n_workers), ~8µs)
  auto ns_stats = AggregateNamespaceStats(ns);

  // Rates berechnen
  uint64_t elapsed = now - data->last_sample_time_ms;
  if (elapsed > 0 && data->last_sample_time_ms > 0) {
    uint64_t ops_delta = ns_stats.total_calls - data->last_total_calls;
    uint64_t in_delta = ns_stats.in_bytes - data->last_in_bytes;
    uint64_t out_delta = ns_stats.out_bytes - data->last_out_bytes;

    data->cached_ops_rate.store((ops_delta * 1000) / elapsed, std::memory_order_relaxed);
    data->cached_in_rate.store((in_delta * 1000) / elapsed, std::memory_order_relaxed);
    data->cached_out_rate.store((out_delta * 1000) / elapsed, std::memory_order_relaxed);
  }

  // Tracker aktualisieren
  data->last_sample_time_ms = now;
  data->last_total_calls = ns_stats.total_calls;
  data->last_in_bytes = ns_stats.in_bytes;
  data->last_out_bytes = ns_stats.out_bytes;
  data->cache_valid_until.store(now + 100, std::memory_order_release);  // 100ms Cache

  return {
      data->cached_ops_rate.load(std::memory_order_relaxed),
      data->cached_in_rate.load(std::memory_order_relaxed),
      data->cached_out_rate.load(std::memory_order_relaxed)};
}

Server::InfoEntries Server::GetStatsInfo(const std::string &ns, [[maybe_unused]] bool is_admin) {
  Server::InfoEntries entries;

  // All users (including admin) see only their own namespace stats
  auto ns_stats = AggregateNamespaceStats(ns);
  entries.emplace_back("total_connections_received", ns_stats.total_connections);
  entries.emplace_back("total_commands_processed", ns_stats.total_calls);
  entries.emplace_back("total_net_input_bytes", ns_stats.in_bytes);
  entries.emplace_back("total_net_output_bytes", ns_stats.out_bytes);

  // Per-namespace instantaneous rates (on-demand calculation with 100ms cache)
  auto rates = GetInstantaneousRatesForNamespace(ns);
  entries.emplace_back("instantaneous_ops_per_sec", rates.ops_per_sec);
  entries.emplace_back("instantaneous_input_kbps", static_cast<float>(rates.in_bytes_per_sec) / 1024.0f);
  entries.emplace_back("instantaneous_output_kbps", static_cast<float>(rates.out_bytes_per_sec) / 1024.0f);
  entries.emplace_back("sync_full", stats.fullsync_count.load());
  entries.emplace_back("sync_partial_ok", stats.psync_ok_count.load());
  entries.emplace_back("sync_partial_err", stats.psync_err_count.load());

  auto db_stats = storage->GetDBStats();
  entries.emplace_back("keyspace_hits", db_stats->keyspace_hits.load());
  entries.emplace_back("keyspace_misses", db_stats->keyspace_misses.load());

  {
    // Each namespace (including admin/default) sees only their own pubsub stats
    size_t total_channels = 0;
    size_t total_patterns = 0;
    std::shared_lock<std::shared_mutex> ns_map_lock(pubsub_namespaces_mu_);
    auto it = pubsub_namespaces_.find(ns);
    if (it != pubsub_namespaces_.end()) {
      std::lock_guard<std::mutex> ns_lock(it->second->mu);
      total_channels = it->second->channels.size();
      total_patterns = it->second->patterns.size();
    }
    entries.emplace_back("pubsub_channels", total_channels);
    entries.emplace_back("pubsub_patterns", total_patterns);
  }

  return entries;
}

Server::InfoEntries Server::GetCommandsStatsInfo(const std::string &ns, bool is_admin) {
  InfoEntries entries;

  // cmdstat_* - Per-Namespace (each tenant sees only their own stats)
  auto cmd_stats = AggregateCommandStatsForNamespace(ns);
  for (const auto &[cmd, stat] : cmd_stats) {
    if (stat.calls == 0) continue;
    entries.emplace_back("cmdstat_" + cmd,
                         fmt::format("calls={},usec={},usec_per_call={}", stat.calls, stat.latency,
                                     static_cast<double>(stat.latency) / static_cast<double>(stat.calls)));
  }

  // cmdstathist_* - Admin-only (global histogram)
  if (is_admin) {
    for (const auto &cmd_hist : stats.commands_histogram) {
      auto command_name = cmd_hist.first;
      auto calls = stats.commands_histogram[command_name].calls.load();
      if (calls == 0) continue;

      auto sum = stats.commands_histogram[command_name].sum.load();
      std::string result;
      for (std::size_t i{0}; i < stats.commands_histogram[command_name].buckets.size(); ++i) {
        auto bucket_value = stats.commands_histogram[command_name].buckets[i]->load();
        auto bucket_bound = std::numeric_limits<double>::infinity();
        if (i < stats.bucket_boundaries.size()) {
          bucket_bound = stats.bucket_boundaries[i];
        }

        result.append(fmt::format("{}={},", bucket_bound, bucket_value));
      }
      result.append(fmt::format("sum={},count={}", sum, calls));

      entries.emplace_back("cmdstathist_" + command_name, result);
    }
  }

  return entries;
}

Server::InfoEntries Server::GetClusterInfo() {
  InfoEntries entries;

  entries.emplace_back("cluster_enabled", config_->cluster_enabled);

  return entries;
}

Server::InfoEntries Server::GetPersistenceInfo() {
  InfoEntries entries;

  entries.emplace_back("loading", is_loading_.load());

  std::lock_guard<std::mutex> lg(db_job_mu_);
  entries.emplace_back("bgsave_in_progress", is_bgsave_in_progress_);
  entries.emplace_back("last_bgsave_time",
                       (last_bgsave_timestamp_secs_ == -1 ? start_time_secs_ : last_bgsave_timestamp_secs_));
  entries.emplace_back("last_bgsave_status", last_bgsave_status_);
  entries.emplace_back("last_bgsave_time_sec", last_bgsave_duration_secs_);

  return entries;
}

Server::InfoEntries Server::GetCpuInfo() {  // NOLINT(readability-convert-member-functions-to-static)
  InfoEntries entries;

  rusage self_ru;
  getrusage(RUSAGE_SELF, &self_ru);
  entries.emplace_back("used_cpu_sys", static_cast<float>(self_ru.ru_stime.tv_sec) +
                                           static_cast<float>(self_ru.ru_stime.tv_usec / 1000000));
  entries.emplace_back("used_cpu_user", static_cast<float>(self_ru.ru_utime.tv_sec) +
                                            static_cast<float>(self_ru.ru_utime.tv_usec / 1000000));

  return entries;
}

Server::InfoEntries Server::GetKeyspaceInfo(const std::string &ns, bool is_admin) {
  InfoEntries entries;
  if (is_loading_) return entries;

  KeyNumStats stats;
  GetLatestKeyNumStats(ns, &stats);
  auto last_dbsize_scan_timestamp = static_cast<time_t>(GetLastScanTime(ns));

  entries.emplace_back("last_dbsize_scan_timestamp", last_dbsize_scan_timestamp);
  entries.emplace_back("db0", fmt::format("keys={},expires={},avg_ttl={},expired={}", stats.n_key, stats.n_expires,
                                          stats.avg_ttl, stats.n_expired));
  // Global sequence number reveals write activity across all namespaces - admin only
  if (is_admin) {
    entries.emplace_back("sequence", storage->GetDB()->GetLatestSequenceNumber());
  }
  entries.emplace_back("used_db_size", storage->GetTotalSize(ns));
  entries.emplace_back("max_db_size", config_->max_db_size * GiB);
  double used_percent = config_->max_db_size ? static_cast<double>(storage->GetTotalSize(ns) * 100) /
                                                   static_cast<double>(config_->max_db_size * GiB)
                                             : 0;
  entries.emplace_back("used_percent", fmt::format("{}%", used_percent));

  struct statvfs stat;
  if (statvfs(config_->db_dir.c_str(), &stat) == 0) {
    auto disk_capacity = stat.f_blocks * stat.f_frsize;
    auto used_disk_size = (stat.f_blocks - stat.f_bavail) * stat.f_frsize;
    entries.emplace_back("disk_capacity", disk_capacity);
    entries.emplace_back("used_disk_size", used_disk_size);
    double used_disk_percent = static_cast<double>(used_disk_size * 100) / static_cast<double>(disk_capacity);
    entries.emplace_back("used_disk_percent", fmt::format("{}%", used_disk_percent));
  }

  return entries;
}

// WARNING: we must not access DB(i.e. RocksDB) when server is loading since
// DB is closed and the pointer is invalid. Server may crash if we access DB during loading.
// If you add new fields which access DB into INFO command output, make sure
// this section can't be shown when loading(i.e. !is_loading_).
std::string Server::GetInfo(redis::Connection *conn, const std::vector<std::string> &sections) {
  std::string ns = conn->GetNamespace();
  bool is_admin = conn->IsAdmin();

  // Sections that expose internal infrastructure details (network topology, storage internals, system metrics)
  static const std::set<std::string> admin_only_sections = {"RocksDB", "Replication", "CPU", "Persistence",
                                                             "Scheduler"};

  std::vector<std::pair<std::string, std::function<InfoEntries(Server *)>>> info_funcs = {
      {"Server", &Server::GetServerInfo},
      {"Clients", [conn](Server *srv) { return srv->GetClientsInfo(conn); }},
      {"Scheduler", &Server::GetSchedulerInfo},
      {"Memory", [conn](Server *srv) { return srv->GetMemoryInfo(conn); }},
      {"Persistence", &Server::GetPersistenceInfo},
      {"Stats", [&ns, is_admin](Server *srv) { return srv->GetStatsInfo(ns, is_admin); }},
      {"Replication", &Server::GetReplicationInfo},
      {"CPU", &Server::GetCpuInfo},
      {"CommandStats", [&ns, is_admin](Server *srv) { return srv->GetCommandsStatsInfo(ns, is_admin); }},
      {"Cluster", &Server::GetClusterInfo},
      {"Keyspace", [&ns, is_admin](Server *srv) { return srv->GetKeyspaceInfo(ns, is_admin); }},
      {"RocksDB", &Server::GetRocksDBInfo},
  };

  std::string info_str;

  bool all = sections.empty() || util::FindICase(sections.begin(), sections.end(), "all") != sections.end();

  bool first = true;
  for (const auto &[sec, fn] : info_funcs) {
    // Skip admin-only sections for non-admin users
    if (!is_admin && admin_only_sections.count(sec)) continue;

    if (all || util::FindICase(sections.begin(), sections.end(), sec) != sections.end()) {
      if (first)
        first = false;
      else
        info_str.append("\r\n");

      info_str.append("# " + sec + "\r\n");

      for (const auto &entry : fn(this)) {
        info_str.append(fmt::format("{}:{}\r\n", entry.name, entry.val));
      }
    }
  }

  return info_str;
}

std::string Server::GetRocksDBStatsJson() const {
  jsoncons::json stats_json;

  auto stats = storage->GetDB()->GetDBOptions().statistics;
  for (const auto &iter : rocksdb::TickersNameMap) {
    stats_json[iter.second] = stats->getTickerCount(iter.first);
  }

  for (const auto &iter : rocksdb::HistogramsNameMap) {
    rocksdb::HistogramData hist_data;
    stats->histogramData(iter.first, &hist_data);
    /* P50 P95 P99 P100 COUNT SUM */
    stats_json[iter.second] =
        jsoncons::json(jsoncons::json_array_arg, {hist_data.median, hist_data.percentile95, hist_data.percentile99,
                                                  hist_data.max, hist_data.count, hist_data.sum});
  }

  return stats_json.to_string();
}

// This function is called by replication thread when finished fetching all files from its master.
// Before restoring the db from backup or checkpoint, we should
// guarantee other threads don't access DB and its column families, then close db.
bool Server::PrepareRestoreDB() {
  // Stop feeding slaves thread
  info("[server] Disconnecting slaves...");
  DisconnectSlaves();

  // If the DB is restored, the object 'db_' will be destroyed, but
  // 'db_' will be accessed in data migration task. To avoid wrong
  // accessing, data migration task should be stopped before restoring DB
  WaitNoMigrateProcessing();

  // Workers will disallow to run commands which may access DB, so we should
  // enable this flag to stop workers from running new commands. And wait for
  // the exclusive guard to be released to guarantee no worker is running.
  is_loading_ = true;

  // To guarantee work threads don't access DB, we should release 'ExclusivityGuard'
  // ASAP to avoid user can't receive responses for long time, because the following
  // 'CloseDB' may cost much time to acquire DB mutex.
  info("[server] Waiting workers for finishing executing commands...");
  while (!works_concurrency_rw_lock_.try_lock()) {
    if (replication_thread_->IsStopped()) {
      is_loading_ = false;
      return false;
    }
    usleep(1000);
  }
  works_concurrency_rw_lock_.unlock();

  // Stop task runner
  info("[server] Stopping the task runner and clear task queue...");
  task_runner_.Cancel();
  if (auto s = task_runner_.Join(); !s) {
    warn("[server] {}", s.Msg());
  }

  // Cron thread, compaction checker thread, full synchronization thread
  // may always run in the background, we need to close db, so they don't actually work.
  info("[server] Waiting for closing DB...");
  storage->CloseDB();
  return true;
}

void Server::WaitNoMigrateProcessing() {
  if (config_->cluster_enabled) {
    info("[server] Waiting until no migration task is running...");
    slot_migrator->SetStopMigrationFlag(true);
    while (slot_migrator->GetCurrentSlotMigrationStage() != SlotMigrationStage::kNone) {
      usleep(500);
    }
  }
}

Status Server::AsyncCompactDB(const std::string &begin_key, const std::string &end_key) {
  if (is_loading_) {
    return {Status::NotOK, "loading in-progress"};
  }

  std::lock_guard<std::mutex> lg(db_job_mu_);
  if (db_compacting_) {
    return {Status::NotOK, "compact in-progress"};
  }

  db_compacting_ = true;
  auto publish_status = task_runner_.TryPublish([begin_key, end_key, this] {
    std::unique_ptr<Slice> begin = nullptr, end = nullptr;
    if (!begin_key.empty()) begin = std::make_unique<Slice>(begin_key);
    if (!end_key.empty()) end = std::make_unique<Slice>(end_key);

    auto s = storage->Compact(nullptr, begin.get(), end.get());
    if (!s.ok()) {
      error("[task runner] Failed to do compaction: {}", s.ToString());
    }

    std::lock_guard<std::mutex> lg(db_job_mu_);
    db_compacting_ = false;
  });
  if (!publish_status.IsOK()) {
    db_compacting_ = false;
  }
  return publish_status;
}

Status Server::AsyncBgSaveDB() {
  std::lock_guard<std::mutex> lg(db_job_mu_);
  if (is_bgsave_in_progress_) {
    return {Status::NotOK, "bgsave in-progress"};
  }

  is_bgsave_in_progress_ = true;
  auto publish_status = task_runner_.TryPublish([this] {
    auto start_bgsave_time_secs = util::GetTimeStamp<std::chrono::seconds>();
    Status s = storage->CreateBackup();
    auto stop_bgsave_time_secs = util::GetTimeStamp<std::chrono::seconds>();

    std::lock_guard<std::mutex> lg(db_job_mu_);
    is_bgsave_in_progress_ = false;
    last_bgsave_timestamp_secs_ = start_bgsave_time_secs;
    last_bgsave_status_ = s.IsOK() ? "ok" : "err";
    last_bgsave_duration_secs_ = stop_bgsave_time_secs - start_bgsave_time_secs;
  });
  if (!publish_status.IsOK()) {
    is_bgsave_in_progress_ = false;
  }
  return publish_status;
}

Status Server::AsyncPurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours) {
  return task_runner_.TryPublish([num_backups_to_keep, backup_max_keep_hours, this] {
    storage->PurgeOldBackups(num_backups_to_keep, backup_max_keep_hours);
  });
}

Status Server::AsyncScanDBSize(const std::string &ns) {
  std::lock_guard<std::mutex> lg(db_job_mu_);

  if (auto iter = db_scan_infos_.find(ns); iter == db_scan_infos_.end()) {
    db_scan_infos_[ns] = DBScanInfo{};
  }

  if (db_scan_infos_[ns].is_scanning) {
    return {Status::NotOK, "scanning the db now"};
  }

  db_scan_infos_[ns].is_scanning = true;
  auto publish_status = task_runner_.TryPublish([ns, this] {
    redis::Database db(storage, ns);

    KeyNumStats stats;
    engine::Context ctx(storage);
    auto s = db.GetKeyNumStats(ctx, "", &stats);
    if (!s.ok()) {
      error("failed to retrieve key num stats: {}", s.ToString());
    }

    std::lock_guard<std::mutex> lg(db_job_mu_);

    db_scan_infos_[ns].key_num_stats = stats;
    db_scan_infos_[ns].last_scan_time_secs = util::GetTimeStamp();
    db_scan_infos_[ns].is_scanning = false;
  });
  if (!publish_status.IsOK()) {
    db_scan_infos_[ns].is_scanning = false;
  }
  return publish_status;
}

void Server::GetLatestKeyNumStats(const std::string &ns, KeyNumStats *stats) {
  std::lock_guard<std::mutex> lg(db_job_mu_);
  auto iter = db_scan_infos_.find(ns);
  if (iter != db_scan_infos_.end()) {
    *stats = iter->second.key_num_stats;
  }
}

int64_t Server::GetLastScanTime(const std::string &ns) {
  std::lock_guard<std::mutex> lg(db_job_mu_);
  auto iter = db_scan_infos_.find(ns);
  if (iter != db_scan_infos_.end()) {
    return iter->second.last_scan_time_secs;
  }
  return 0;
}

StatusOr<std::vector<rocksdb::BatchResult>> Server::PollUpdates(uint64_t next_sequence, int64_t count,
                                                                bool is_strict) const {
  std::vector<rocksdb::BatchResult> batches;
  auto latest_sequence = storage->LatestSeqNumber();
  if (next_sequence == latest_sequence + 1) {
    // return empty result if there is no new updates
    return batches;
  } else if (next_sequence > latest_sequence + 1) {
    return {Status::NotOK, "next sequence is out of range"};
  }

  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  if (auto s = storage->GetWALIter(next_sequence, &iter); !s.IsOK()) return s;
  if (!iter) {
    return Status{Status::NotOK, "unable to get WAL iterator"};
  }

  for (int64_t i = 0; i < count && iter->Valid() && iter->status().ok(); ++i, iter->Next()) {
    // The first batch should have the same sequence number as the next sequence number
    // if it requires strictly matched.
    auto batch = iter->GetBatch();
    if (i == 0 && is_strict && batch.sequence != next_sequence) {
      return {Status::NotOK,
              fmt::format("mismatched sequence number, expected {} but got {}", next_sequence, batch.sequence)};
    }
    batches.emplace_back(std::move(batch));
  }
  return batches;
}

void Server::SlowlogPushEntryIfNeeded(const std::vector<std::string> *args, uint64_t duration,
                                      const redis::Connection *conn) {
  int64_t threshold = config_->slowlog_log_slower_than;
  if (threshold < 0 || static_cast<int64_t>(duration) < threshold) return;

  auto entry = std::make_unique<SlowEntry>();
  size_t argc = args->size() > kSlowLogMaxArgc ? kSlowLogMaxArgc : args->size();
  for (size_t i = 0; i < argc; i++) {
    if (argc != args->size() && i == argc - 1) {
      entry->args.emplace_back(fmt::format("... ({} more arguments)", args->size() - argc + 1));
      break;
    }

    if ((*args)[i].length() <= kSlowLogMaxString) {
      entry->args.emplace_back((*args)[i]);
    } else {
      entry->args.emplace_back(fmt::format("{}... ({} more bytes)", (*args)[i].substr(0, kSlowLogMaxString),
                                           (*args)[i].length() - kSlowLogMaxString));
    }
  }

  entry->duration = duration;
  entry->client_name = conn->GetName();
  entry->ip = conn->GetIP();
  entry->port = conn->GetPort();
  entry->ns = conn->GetNamespace();
  slow_log_.PushEntry(std::move(entry));
}

std::string Server::GetClientsStr(redis::Connection *self) {
  std::string clients;
  if (auto snapshot = GetWorkerSnapshot()) {
    for (const auto &worker : *snapshot) {
      clients.append(worker->GetClientsStr(self));
    }
  }

  // Slave connections are only visible to admin
  if (self->IsAdmin()) {
    std::shared_lock<std::shared_mutex> guard(slave_threads_mu_);
    for (const auto &st : slave_threads_) {
      clients.append(st->GetConn()->ToString());
    }
  }

  return clients;
}

void Server::KillClient(int64_t *killed, const std::string &addr, uint64_t id, uint64_t type, bool skipme,
                        redis::Connection *conn) {
  *killed = 0;

  // Normal clients and pubsub clients
  if (auto snapshot = GetWorkerSnapshot()) {
    for (const auto &worker : *snapshot) {
      int64_t killed_in_worker = 0;
      worker->KillClient(conn, id, addr, type, skipme, &killed_in_worker);
      *killed += killed_in_worker;
    }
  }

  // Slave clients (only admin can kill these)
  if (conn->IsAdmin()) {
    std::unique_lock<std::shared_mutex> guard(slave_threads_mu_);
    for (const auto &st : slave_threads_) {
      if ((type & kTypeSlave) ||
          (!addr.empty() && (st->GetConn()->GetAddr() == addr || st->GetConn()->GetAnnounceAddr() == addr)) ||
          (id != 0 && st->GetConn()->GetID() == id)) {
        st->Stop();
        (*killed)++;
      }
    }
  }

  // Master client (only admin can kill this)
  if (conn->IsAdmin() && IsSlave() &&
      (type & kTypeMaster || (!addr.empty() && addr == master_host_ + ":" + std::to_string(master_port_)))) {
    // Stop replication thread and start a new one to replicate
    if (auto s = AddMaster(master_host_, master_port_, true); !s.IsOK()) {
      error("[server] Failed to add master {}:{} with error: {}", master_host_, master_port_, s.Msg());
    }
    (*killed)++;
  }
}

ReplState Server::GetReplicationState() {
  std::lock_guard<std::mutex> guard(slaveof_mu_);
  if (IsSlave() && replication_thread_) {
    return replication_thread_->State();
  }
  return kReplConnecting;
}

StatusOr<std::unique_ptr<redis::Commander>> Server::LookupAndCreateCommand(const std::string &cmd_name) {
  if (cmd_name.empty()) return {Status::RedisUnknownCmd};

  auto commands = redis::CommandTable::Get();
  auto cmd_iter = commands->find(util::ToLower(cmd_name));
  if (cmd_iter == commands->end()) {
    return {Status::RedisUnknownCmd};
  }

  auto cmd_attr = cmd_iter->second;
  auto cmd = cmd_attr->factory();
  cmd->SetAttributes(cmd_attr);

  return std::move(cmd);
}

Status Server::ScriptExists(const rocksdb::Slice &ns, const std::string &sha) const {
  std::string body;
  return ScriptGet(ns, sha, &body);
}

Status Server::ScriptGet(const rocksdb::Slice &ns, const std::string &sha, std::string *body) const {
  std::string func_name = engine::ComposeFunctionKey(engine::kLuaFuncSHAPrefix, ns, sha);
  auto cf = storage->GetCFHandle(ColumnFamilyID::Propagate);
  engine::Context ctx(storage);
  auto s = storage->Get(ctx, ctx.GetReadOptions(), cf, func_name, body);
  if (!s.ok()) {
    return {s.IsNotFound() ? Status::NotFound : Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Server::ScriptSet(const rocksdb::Slice &ns, const std::string &sha, const std::string &body) const {
  std::string func_name = engine::ComposeFunctionKey(engine::kLuaFuncSHAPrefix, ns, sha);
  engine::Context ctx(storage);
  return storage->WriteToPropagateCF(ctx, func_name, body);
}

Status Server::FunctionGetCode(const rocksdb::Slice &ns, const std::string &lib, std::string *code) const {
  std::string key = engine::ComposeFunctionKey(engine::kLuaLibCodePrefix, ns, lib);
  auto cf = storage->GetCFHandle(ColumnFamilyID::Propagate);
  engine::Context ctx(storage);
  auto s = storage->Get(ctx, ctx.GetReadOptions(), cf, key, code);
  if (!s.ok()) {
    return {s.IsNotFound() ? Status::NotFound : Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Server::FunctionGetLib(const rocksdb::Slice &ns, const std::string &func, std::string *lib) const {
  std::string key = engine::ComposeFunctionKey(engine::kLuaFuncLibPrefix, ns, func);
  auto cf = storage->GetCFHandle(ColumnFamilyID::Propagate);
  engine::Context ctx(storage);
  auto s = storage->Get(ctx, ctx.GetReadOptions(), cf, key, lib);
  if (!s.ok()) {
    return {s.IsNotFound() ? Status::NotFound : Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Server::FunctionSetCode(const rocksdb::Slice &ns, const std::string &lib, const std::string &code) const {
  std::string key = engine::ComposeFunctionKey(engine::kLuaLibCodePrefix, ns, lib);
  engine::Context ctx(storage);
  return storage->WriteToPropagateCF(ctx, key, code);
}

Status Server::FunctionSetLib(const rocksdb::Slice &ns, const std::string &func, const std::string &lib) const {
  std::string key = engine::ComposeFunctionKey(engine::kLuaFuncLibPrefix, ns, func);
  engine::Context ctx(storage);
  return storage->WriteToPropagateCF(ctx, key, lib);
}

void Server::ScriptReset() {
  // Increment generation - workers will reset lazily when they see the new generation
  script_reset_generation_.fetch_add(1);
}

void Server::ScriptResetNamespace(const std::string &ns, Worker *exclude) {
  // Mark namespace for reset on all workers - they will reset lazily
  // Exclude the current worker if specified (it already did the operation)
  if (auto snapshot = GetWorkerSnapshot()) {
    for (const auto &worker : *snapshot) {
      if (worker.get() != exclude) {
        worker->MarkNamespaceForReset(ns);
      }
    }
  }
}

Status Server::ScriptFlush(const std::string &ns) {
  auto cf = storage->GetCFHandle(ColumnFamilyID::Propagate);
  engine::Context ctx(storage);
  auto s = storage->FlushScripts(ctx, storage->DefaultWriteOptions(), cf, ns);
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  ScriptResetNamespace(ns);
  return Status::OK();
}

// Generally, we store data into RocksDB and just replicate WAL instead of propagating
// commands. But sometimes, we need to update inner states or do special operations
// for specific commands, such as `script flush`.
// channel: we put the same function commands into one channel to handle uniformly
// tokens: the serialized commands
Status Server::Propagate(const std::string &channel, const std::vector<std::string> &tokens) const {
  std::string value = redis::MultiLen(tokens.size());
  for (const auto &iter : tokens) {
    value += redis::BulkString(iter);
  }
  engine::Context ctx(storage);
  return storage->WriteToPropagateCF(ctx, channel, value);
}

Status Server::ExecPropagatedCommand(const std::vector<std::string> &tokens) {
  CommandParser parser(tokens);
  if (parser.EatEqICase("script")) {
    if (parser.EatEqICase("flush")) {
      // here we must acquire the global lock to guarantee that
      // no EVAL or FCALL is executing while resetting lua state.
      auto guard = WorkExclusivityGuard();
      ScriptReset();
    }
  } else if (parser.EatEqICase("function")) {
    if (parser.EatEqICase("delete") || parser.EatEqICase("flush")) {
      // same as above to acquire the global lock
      auto guard = WorkExclusivityGuard();
      ScriptReset();
    }
  }

  return Status::OK();
}

Status Server::KillLuaScript(const std::string &ns) {
  info("[DEBUG] KillLuaScript called for ns={}", ns);
  bool found = false;
  for (const auto &wt : worker_threads_) {
    auto *w = wt->GetWorker();
    info("[DEBUG] Worker: running={} script_ns={}", w->IsLuaScriptRunning(), w->GetLuaScriptNs());
    if (w->IsLuaScriptRunning() && w->GetLuaScriptNs() == ns) {
      w->RequestLuaScriptKill();
      found = true;
      info("[DEBUG] Requested kill for worker");
    }
  }
  if (!found) {
    info("[DEBUG] No scripts found");
    return {Status::NotOK, "No scripts in execution right now."};
  }
  info("[DEBUG] Kill requested, returning OK");
  return Status::OK();
}

// AdjustOpenFilesLimit only try best to raise the max open files according to
// the max clients and RocksDB open file configuration. It also reserves a number
// of file descriptors(128) for extra operations of persistence, listening sockets,
// log files and so forth.
void Server::AdjustOpenFilesLimit() {
  const int min_reserved_fds = 128;
  auto rocksdb_max_open_file = static_cast<rlim_t>(config_->rocks_db.max_open_files);
  auto max_clients = static_cast<rlim_t>(config_->maxclients);
  auto max_files = max_clients + rocksdb_max_open_file + min_reserved_fds;

  rlimit limit;
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    return;
  }

  rlim_t old_limit = limit.rlim_cur;
  // Set the max number of files only if the current limit is not enough
  if (old_limit >= max_files) {
    return;
  }

  int setrlimit_error = 0;
  rlim_t best_limit = max_files;

  while (best_limit > old_limit) {
    limit.rlim_cur = best_limit;
    limit.rlim_max = best_limit;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) break;

    setrlimit_error = errno;

    rlim_t decr_step = 16;
    if (best_limit < decr_step) {
      best_limit = old_limit;
      break;
    }

    best_limit -= decr_step;
  }

  if (best_limit < old_limit) best_limit = old_limit;

  if (best_limit < max_files) {
    if (best_limit <= static_cast<int>(min_reserved_fds)) {
      warn(
          "[server] Your current 'ulimit -n' of {} is not enough for the server to start. "
          "Please increase your open file limit to at least {}. Exiting.",
          old_limit, max_files);
      exit(1);
    }

    warn(
        "[server] You requested max clients of {} and RocksDB max open files of {} "
        "requiring at least {} max file descriptors.",
        max_clients, rocksdb_max_open_file, max_files);

    warn(
        "[server] Server can't set maximum open files to {} "
        "because of OS error: {}",
        max_files, strerror(setrlimit_error));
  } else {
    warn("[server] Increased maximum number of open files to {} (it's originally set to {})", max_files, old_limit);
  }
}

void Server::AdjustWorkerThreads() {
  std::lock_guard<std::mutex> guard(worker_threads_mu_);
  auto new_worker_threads = static_cast<size_t>(config_->workers);
  if (new_worker_threads == worker_threads_.size()) {
    return;
  }
  size_t delta = 0;
  if (new_worker_threads > worker_threads_.size()) {
    delta = new_worker_threads - worker_threads_.size();
    increaseWorkerThreads(delta);
    info("[server] Increase worker threads from {} to {}", worker_threads_.size(), new_worker_threads);
    PublishWorkerSnapshot();
    return;
  }

  delta = worker_threads_.size() - new_worker_threads;
  info("[server] Decrease worker threads from {} to {}", worker_threads_.size(), new_worker_threads);
  decreaseWorkerThreads(delta);
  PublishWorkerSnapshot();
}

void Server::increaseWorkerThreads(size_t delta) {
  for (size_t i = 0; i < delta; i++) {
    auto worker = std::make_shared<Worker>(this, config_);
    worker->SetIndex(static_cast<uint32_t>(worker_threads_.size()));
    auto worker_thread = std::make_unique<WorkerThread>(std::move(worker));
    worker_thread->Start();
    worker_threads_.emplace_back(std::move(worker_thread));
  }
}

void Server::decreaseWorkerThreads(size_t delta) {
  auto current_worker_threads = worker_threads_.size();
  CHECK(current_worker_threads > delta);
  auto remain_worker_threads = current_worker_threads - delta;
  for (size_t i = remain_worker_threads; i < current_worker_threads; i++) {
    // Unix socket will be listening on the first worker,
    // so it MUST remove workers from the end of the vector.
    // Otherwise, the unix socket will be closed.
    auto worker_thread = std::move(worker_threads_.back());
    worker_threads_.pop_back();
    // Migrate connections to other workers before stopping the worker,
    // we use round-robin to choose the target worker here.
    auto connections = worker_thread->GetWorker()->GetConnectionsSnapshot();
    worker_thread->GetWorker()->StopAccepting();
    for (const auto &iter : connections) {
      auto target_worker = worker_threads_[iter.first % remain_worker_threads]->GetWorker();
      worker_thread->GetWorker()->MigrateConnection(target_worker, iter.second);
    }
    worker_thread->Stop(10 /* graceful timeout */);
    // Don't join the worker thread here, because it may join itself.
    recycle_worker_threads_.push(std::move(worker_thread));
  }
}

void Server::cleanupExitedWorkerThreads(bool force) {
  std::unique_ptr<WorkerThread> worker_thread = nullptr;
  auto total = recycle_worker_threads_.unsafe_size();
  for (size_t i = 0; i < total; i++) {
    if (!recycle_worker_threads_.try_pop(worker_thread)) {
      break;
    }
    if (worker_thread->IsTerminated() || force) {
      worker_thread->Join();
      worker_thread.reset();
    } else {
      // Push the worker thread back to the queue if it's still running.
      recycle_worker_threads_.push(std::move(worker_thread));
    }
  }
}

std::string ServerLogData::Encode() const {
  if (type_ == kReplIdLog) {
    return std::string(1, kReplIdTag) + " " + content_;
  }
  return content_;
}

Status ServerLogData::Decode(const rocksdb::Slice &blob) {
  if (blob.size() == 0) {
    return {Status::NotOK};
  }

  const char *header = blob.data();
  // Only support `kReplIdTag` now
  if (*header == kReplIdTag && blob.size() == 2 + kReplIdLength) {
    type_ = kReplIdLog;
    content_ = std::string(blob.data() + 2, blob.size() - 2);
    return Status::OK();
  }
  return {Status::NotOK};
}

// Creates a namespace-prefixed key for watched_key_map_ to ensure namespace isolation.
// Format: ns_length (1 byte) + ns + key - similar to ComposeNamespaceKey but without slot_id.
static std::string MakeWatchedKey(const std::string &ns, const std::string &key) {
  std::string result;
  result.reserve(1 + ns.size() + key.size());
  result.push_back(static_cast<char>(ns.size()));
  result.append(ns);
  result.append(key);
  return result;
}

void Server::updateWatchedKeysFromRange(const std::string &ns, const std::vector<std::string> &args,
                                        const redis::CommandKeyRange &range) {
  std::shared_lock lock(watched_key_mutex_);

  for (size_t i = range.first_key; range.last_key > 0 ? i <= size_t(range.last_key) : i <= args.size() + range.last_key;
       i += range.key_step) {
    auto watched_key = MakeWatchedKey(ns, args[i]);
    if (auto iter = watched_key_map_.find(watched_key); iter != watched_key_map_.end()) {
      for (auto *conn : iter->second) {
        conn->watched_keys_modified = true;
      }
    }
  }
}

void Server::updateAllWatchedKeys() {
  std::shared_lock lock(watched_key_mutex_);

  for (auto &[_, conn_map] : watched_key_map_) {
    for (auto *conn : conn_map) {
      conn->watched_keys_modified = true;
    }
  }
}

void Server::UpdateWatchedKeysFromArgs(const std::string &ns, const std::vector<std::string> &args,
                                       const redis::CommandAttributes &attr) {
  auto config = GetConfig()->GetSnapshot();
  if ((attr.GenerateFlags(args, *config) & redis::kCmdWrite) && watched_key_size_ > 0) {
    attr.ForEachKeyRange([this, &ns](const std::vector<std::string> &args,
                                     redis::CommandKeyRange range) { updateWatchedKeysFromRange(ns, args, range); },
                         args, [this](const std::vector<std::string> &) { updateAllWatchedKeys(); });
  }
}

void Server::UpdateWatchedKeysManually(const std::string &ns, const std::vector<std::string> &keys) {
  std::shared_lock lock(watched_key_mutex_);

  for (const auto &key : keys) {
    auto watched_key = MakeWatchedKey(ns, key);
    if (auto iter = watched_key_map_.find(watched_key); iter != watched_key_map_.end()) {
      for (auto *conn : iter->second) {
        conn->watched_keys_modified = true;
      }
    }
  }
}

void Server::WatchKey(redis::Connection *conn, const std::vector<std::string> &keys) {
  std::unique_lock lock(watched_key_mutex_);

  const auto &ns = conn->GetNamespace();
  for (const auto &key : keys) {
    auto watched_key = MakeWatchedKey(ns, key);
    if (auto iter = watched_key_map_.find(watched_key); iter != watched_key_map_.end()) {
      iter->second.emplace(conn);
    } else {
      watched_key_map_.emplace(watched_key, std::set<redis::Connection *>{conn});
    }

    conn->watched_keys.insert(watched_key);
  }

  watched_key_size_ = watched_key_map_.size();
}

bool Server::IsWatchedKeysModified(redis::Connection *conn) { return conn->watched_keys_modified; }

void Server::ResetWatchedKeys(redis::Connection *conn) {
  if (watched_key_size_ != 0) {
    std::unique_lock lock(watched_key_mutex_);

    for (const auto &key : conn->watched_keys) {
      if (auto iter = watched_key_map_.find(key); iter != watched_key_map_.end()) {
        iter->second.erase(conn);

        if (iter->second.empty()) {
          watched_key_map_.erase(iter);
        }
      }
    }

    conn->watched_keys.clear();
    conn->watched_keys_modified = false;
    watched_key_size_ = watched_key_map_.size();
  }
}

std::list<std::pair<std::string, uint32_t>> Server::GetSlaveHostAndPort() {
  std::list<std::pair<std::string, uint32_t>> result;
  {
    std::shared_lock<std::shared_mutex> guard(slave_threads_mu_);
    for (const auto &slave : slave_threads_) {
      if (slave->IsStopped()) continue;
      std::pair<std::string, int> host_port_pair = {slave->GetConn()->GetAnnounceIP(),
                                                    slave->GetConn()->GetListeningPort()};
      result.emplace_back(host_port_pair);
    }
  }
  return result;
}

// The numeric cursor consists of a 16-bit counter, a 16-bit time stamp, a 29-bit hash,and a 3-bit cursor type. The
// hash is used to prevent information leakage. The time_stamp is used to prevent the generation of the same cursor in
// the extremely short period before and after a restart.
NumberCursor::NumberCursor(CursorType cursor_type, uint16_t counter, const std::string &key_name) {
  auto hash = static_cast<uint32_t>(std::hash<std::string>{}(key_name));
  auto time_stamp = static_cast<uint16_t>(util::GetTimeStamp());
  // make hash top 3-bit zero
  constexpr uint64_t hash_mask = 0x1FFFFFFFFFFFFFFF;
  cursor_ = static_cast<uint64_t>(counter) | static_cast<uint64_t>(time_stamp) << 16 |
            (static_cast<uint64_t>(hash) << 32 & hash_mask) | static_cast<uint64_t>(cursor_type) << 61;
}

bool NumberCursor::IsMatch(const CursorDictElement &element, CursorType cursor_type) const {
  return cursor_ == element.cursor.cursor_ && cursor_type == getCursorType();
}

std::string Server::GenerateCursorFromKeyName(const std::string &key_name, CursorType cursor_type, const char *prefix) {
  if (!config_->redis_cursor_compatible) {
    // add prefix for SCAN
    return prefix + key_name;
  }
  auto counter = cursor_counter_.fetch_add(1);
  auto number_cursor = NumberCursor(cursor_type, counter, key_name);
  cursor_dict_->at(number_cursor.GetIndex()) = {number_cursor, key_name};
  return number_cursor.ToString();
}

std::string Server::GetKeyNameFromCursor(const std::string &cursor, CursorType cursor_type) {
  // When cursor is 0, cursor string is empty
  if (cursor.empty() || !config_->redis_cursor_compatible) {
    return cursor;
  }

  auto s = ParseInt<uint64_t>(cursor, 10);
  // When Cursor 0 or not a Integer return empty string.
  // Although the parameter 'cursor' is not expected to be 0, we still added a check for 0 to increase the robustness of
  // the code.
  if (!s.IsOK() || *s == 0) {
    return {};
  }
  auto number_cursor = NumberCursor(*s);
  // Because the index information is fully stored in the cursor, we can directly obtain the index from the cursor.
  auto item = cursor_dict_->at(number_cursor.GetIndex());
  if (number_cursor.IsMatch(item, cursor_type)) {
    return item.key_name;
  }

  return {};
}

AuthResult Server::AuthenticateUser(const std::string &user_password, std::string *ns) {
  const auto &requirepass = GetConfig()->GetSnapshot()->requirepass;
  if (requirepass.empty()) {
    return AuthResult::NO_REQUIRE_PASS;
  }

  auto get_ns = GetNamespace()->GetByToken(user_password);
  if (get_ns.IsOK()) {
    *ns = get_ns.GetValue();
    return AuthResult::IS_USER;
  }

  if (user_password != requirepass) {
    return AuthResult::INVALID_PASSWORD;
  }
  *ns = kDefaultNamespace;
  return AuthResult::IS_ADMIN;
}

// Accept-Dispatch Architecture Implementation
// Acceptor threads handle TCP accept(), dispatch FDs to Workers via eventfd

// Helper function to create a listen socket for a given host:port
static StatusOr<int> CreateListenSocket(const std::string &host, uint32_t port, int backlog) {
  bool ipv6_used = strchr(host.data(), ':');

  addrinfo hints = {};
  hints.ai_family = ipv6_used ? AF_INET6 : AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  addrinfo *srv_info = nullptr;
  if (int rv = getaddrinfo(host.data(), std::to_string(port).c_str(), &hints, &srv_info); rv != 0) {
    return {Status::NotOK, gai_strerror(rv)};
  }

  int fd = -1;
  for (auto p = srv_info; p != nullptr; p = p->ai_next) {
    fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd == -1) continue;

    int sock_opt = 1;
    if (ipv6_used && setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &sock_opt, sizeof(sock_opt)) == -1) {
      close(fd);
      freeaddrinfo(srv_info);
      return {Status::NotOK, fmt::format("setsockopt IPV6_V6ONLY failed: {}", strerror(errno))};
    }

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sock_opt, sizeof(sock_opt)) < 0) {
      close(fd);
      freeaddrinfo(srv_info);
      return {Status::NotOK, fmt::format("setsockopt SO_REUSEADDR failed: {}", strerror(errno))};
    }

    // SO_REUSEPORT for kernel load-balancing across multiple acceptor threads
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &sock_opt, sizeof(sock_opt)) < 0) {
      close(fd);
      freeaddrinfo(srv_info);
      return {Status::NotOK, fmt::format("setsockopt SO_REUSEPORT failed: {}", strerror(errno))};
    }

    if (bind(fd, p->ai_addr, p->ai_addrlen)) {
      close(fd);
      freeaddrinfo(srv_info);
      return {Status::NotOK, fmt::format("bind failed: {}", strerror(errno))};
    }

    if (listen(fd, backlog) < 0) {
      close(fd);
      freeaddrinfo(srv_info);
      return {Status::NotOK, fmt::format("listen failed: {}", strerror(errno))};
    }

    // Make socket non-blocking for poll-based accept loop
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    break;  // Successfully created socket
  }

  freeaddrinfo(srv_info);
  if (fd == -1) {
    return {Status::NotOK, "failed to create socket for any address"};
  }
  return fd;
}

Status Server::StartAcceptors() {
  // Create listen sockets for each bind address and port
  // With SO_REUSEPORT, each acceptor thread gets its own socket for kernel load balancing
  for (int i = 0; i < config_->acceptor_threads; i++) {
    for (const auto &bind_addr : config_->binds) {
      // TCP port
      if (config_->port > 0) {
        auto fd_or = CreateListenSocket(bind_addr, config_->port, config_->backlog);
        if (!fd_or.IsOK()) {
          return fd_or.ToStatus().Prefixed(fmt::format("failed to listen on {}:{}", bind_addr, config_->port));
        }
        int fd = *fd_or;
        listen_fds_.push_back(fd);
        auto acceptor_thread = GET_OR_RET(util::CreateThread(
            "acceptor", [this, fd] { AcceptorLoop(fd, /*is_tls=*/false); }));
        acceptor_threads_.push_back(std::move(acceptor_thread));
        info("[acceptor] Listening on {}:{}", bind_addr, config_->port);
      }

#ifdef ENABLE_OPENSSL
      // TLS port (if configured)
      if (config_->tls_port > 0) {
        auto tls_fd_or = CreateListenSocket(bind_addr, config_->tls_port, config_->backlog);
        if (!tls_fd_or.IsOK()) {
          return tls_fd_or.ToStatus().Prefixed(fmt::format("failed to listen on {}:{} (TLS)", bind_addr, config_->tls_port));
        }
        int tls_fd = *tls_fd_or;
        tls_listen_fds_.push_back(tls_fd);
        auto acceptor_thread = GET_OR_RET(util::CreateThread(
            "acceptor-tls", [this, tls_fd] { AcceptorLoop(tls_fd, /*is_tls=*/true); }));
        acceptor_threads_.push_back(std::move(acceptor_thread));
        info("[acceptor] Listening on {}:{} (TLS)", bind_addr, config_->tls_port);
      }
#endif
    }
  }

  // Handle systemd socket activation
  if (config_->socket_fd >= 0) {
    // Duplicate the FD so we don't close systemd's original on shutdown
    int fd = dup(config_->socket_fd);
    if (fd < 0) {
      return {Status::NotOK, fmt::format("dup(socket_fd) failed: {}", strerror(errno))};
    }
    // Make the socket non-blocking
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    listen_fds_.push_back(fd);
    auto acceptor_thread = GET_OR_RET(util::CreateThread(
        "acceptor-sd", [this, fd] { AcceptorLoop(fd, /*is_tls=*/false); }));
    acceptor_threads_.push_back(std::move(acceptor_thread));
    info("[acceptor] Using systemd socket fd={} (dup'd from {})", fd, config_->socket_fd);
  }

  info("[acceptor] Started {} acceptor threads", acceptor_threads_.size());
  return Status::OK();
}

void Server::StopAcceptors() {
  acceptor_stop_.store(true);

  // FIRST: Join all acceptor threads (they exit via stop_ flag + poll timeout)
  // This ensures no thread is using the FDs when we close them
  for (auto &t : acceptor_threads_) {
    if (auto s = util::ThreadJoin(t); !s) {
      warn("[acceptor] Failed to join acceptor thread: {}", s.Msg());
    }
  }

  // THEN: Close listen sockets (safe, no threads active anymore)
  for (int fd : listen_fds_) {
    close(fd);
  }
  for (int fd : tls_listen_fds_) {
    close(fd);
  }

  acceptor_threads_.clear();
  listen_fds_.clear();
  tls_listen_fds_.clear();
  info("[acceptor] All acceptor threads stopped");
}

void Server::AcceptorLoop(int listen_fd, bool is_tls) {
  struct pollfd pfd;
  pfd.fd = listen_fd;
  pfd.events = POLLIN;

  while (!acceptor_stop_.load()) {
    // Poll with 100ms timeout to check stop flag periodically
    int ret = poll(&pfd, 1, 100);
    if (ret < 0) {
      if (errno == EINTR) continue;
      if (errno == EBADF) break;  // Socket closed during shutdown - normal
      error("[acceptor] poll() failed: {}", strerror(errno));
      break;
    }
    if (ret == 0) continue;  // Timeout, check stop flag

    if (!(pfd.revents & POLLIN)) continue;

    // Accept new connections
    while (!acceptor_stop_.load()) {
      sockaddr_storage addr;
      socklen_t addrlen = sizeof(addr);
      int fd = accept4(listen_fd, reinterpret_cast<sockaddr *>(&addr), &addrlen, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          break;  // No more pending connections
        }
        if (errno == EINTR) continue;
        if (errno == EMFILE || errno == ENFILE) {
          // Too many open files, back off
          warn("[acceptor] accept() failed (too many open files): {}", strerror(errno));
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          break;
        }
        // Socket may have been closed for shutdown
      if (acceptor_stop_.load()) break;
      error("[acceptor] accept() failed: {}", strerror(errno));
      continue;
    }

    // Extract SNI for fair scheduling (Phase 3)
    std::string sni = GetSchedulingKey(fd, is_tls);

    // Select a worker using FairScheduler
    auto worker = fair_scheduler_->SelectWorker(sni);
    if (!worker) {
      // Fallback to old SelectWorker if FairScheduler fails
      worker = SelectWorker();
    }
    if (!worker) {
      error("[acceptor] No workers available, closing connection");
      close(fd);
      continue;
    }

    PendingConnection pc;
    pc.fd = fd;
    pc.is_tls = is_tls;
    pc.sni = std::move(sni);
    worker->DispatchConnection(std::move(pc));
  }
  }
}

std::shared_ptr<std::vector<std::shared_ptr<Worker>>> Server::GetWorkerSnapshot() const {
  return std::atomic_load_explicit(&worker_snapshot_, std::memory_order_acquire);
}

void Server::PublishWorkerSnapshot() {
  auto snapshot = std::make_shared<std::vector<std::shared_ptr<Worker>>>();
  snapshot->reserve(worker_threads_.size());
  for (const auto &wt : worker_threads_) {
    snapshot->push_back(wt->GetWorkerShared());
  }
  std::atomic_store_explicit(&worker_snapshot_, std::move(snapshot), std::memory_order_release);
}

std::shared_ptr<Worker> Server::SelectWorker() {
  auto snapshot = GetWorkerSnapshot();
  if (!snapshot || snapshot->empty()) return nullptr;

  const size_t size = snapshot->size();
  size_t idx = next_worker_.fetch_add(1, std::memory_order_relaxed) % size;

  // Phase 2: Prefer workers NOT running Lua scripts
  for (size_t i = 0; i < size; i++) {
    auto &worker = (*snapshot)[(idx + i) % size];
    if (worker->IsAccepting() && !worker->IsLuaScriptRunning()) {
      return worker;
    }
  }

  // Fallback: All workers running Lua, return any accepting worker
  for (size_t i = 0; i < size; i++) {
    auto &worker = (*snapshot)[(idx + i) % size];
    if (worker->IsAccepting()) {
      return worker;
    }
  }

  return nullptr;
}
