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

#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>

#include <mutex>
#include <shared_mutex>

#include "commands/commander.h"
#include "commands/error_constants.h"
#include "fmt/format.h"
#include "logging.h"
#include "nonstd/span.hpp"
#include "search/indexer.h"
#include "server/redis_reply.h"
#include "string_util.h"
#ifdef ENABLE_OPENSSL
#include <event2/bufferevent_ssl.h>
#endif

#include "commands/blocking_commander.h"
#include "redis_connection.h"
#include "scope_exit.h"
#include "server.h"
#include "time_util.h"
#include "tls_util.h"
#include "worker.h"

namespace redis {

Connection::Connection(bufferevent *bev, Worker *owner)
    : need_free_bev_(true), bev_(bev), req_(owner->srv, this), owner_(owner), srv_(owner->srv) {
  int64_t now = util::GetTimeStamp();
  create_time_ = now;
  last_interaction_.store(now, std::memory_order_relaxed);
}

Connection::~Connection() {
  if (bev_) {
    if (need_free_bev_) {
      bufferevent_free(bev_);
    } else {
      // cleanup event callbacks here to prevent using Connection's resource
      bufferevent_setcb(bev_, nullptr, nullptr, nullptr, nullptr);
    }
  }
  // unsubscribe all channels and patterns if exists
  UnsubscribeAll();
  PUnsubscribeAll();
  SUnsubscribeAll();
}

std::string Connection::ToString() { return FormatClientInfo(GetClientInfo(true)); }

Connection::ClientInfo Connection::GetClientInfo(bool include_buffers) const {
  ClientInfo info;
  info.id = GetID();
  info.type = GetClientType();
  info.flags = GetFlags();
  info.age = GetAge();
  info.idle = GetIdleTime();
  info.close_after_reply = IsFlagEnabled(kCloseAfterReply);
  info.is_slave = IsFlagEnabled(kSlave);
  info.is_monitor = IsFlagEnabled(kMonitor);
  info.worker = owner_->GetIndex();
  info.fd = bufferevent_getfd(bev_);

  std::lock_guard<std::mutex> lock(client_mu_);
  info.addr = addr_;
  info.name = name_;
  info.ns = ns_;
  info.last_cmd = last_cmd_;
  if (!announce_ip_.empty()) {
    info.announce_addr = announce_ip_ + ":" + std::to_string(GetAnnouncePort());
  } else {
    info.announce_addr = addr_;
  }

  if (include_buffers) {
    info.qbuf = evbuffer_get_length(bufferevent_get_input(bev_));
    info.obuf = evbuffer_get_length(bufferevent_get_output(bev_));
  }
  return info;
}

std::string Connection::FormatClientInfo(const ClientInfo &info) {
  return fmt::format("id={} addr={} fd={} name={} age={} idle={} flags={} namespace={} qbuf={} obuf={} cmd={} worker={}\n",
                     info.id, info.addr, info.fd, info.name, info.age, info.idle, info.flags, info.ns, info.qbuf,
                     info.obuf, info.last_cmd, info.worker);
}

void Connection::Close() {
  if (close_cb) close_cb(GetFD());
  owner_->FreeConnection(this);
}

void Connection::Detach() { owner_->DetachConnection(this); }

void Connection::OnRead([[maybe_unused]] struct bufferevent *bev) {
  is_running_ = true;
  MakeScopeExit([this] { is_running_ = false; });

  SetLastInteraction();
  auto config = srv_->GetConfig()->GetSnapshot();
  size_t max_cmds = static_cast<size_t>(config->read_event_max_commands);
  int64_t max_time_us = config->read_event_max_time_us;
  auto s = req_.Tokenize(Input(), max_cmds);
  if (!s.IsOK()) {
    EnableFlag(redis::Connection::kCloseAfterReply);
    Reply(redis::Error(s));
    info("[connection] Failed to tokenize the request. Error: {}", s.Msg());
    return;
  }

  auto result = ExecuteCommandsWithBudget(req_.GetCommands(), max_cmds, max_time_us, true);
  if (IsFlagEnabled(kCloseAsync)) {
    Close();
    return;
  }

  if (result == ExecuteResult::kYielded) {
    PauseReadForQuota();
    ScheduleResume();
  } else if (result == ExecuteResult::kBlocked) {
    read_paused_for_quota_ = false;
  } else {
    ResumeReadIfPaused();
  }
}

void Connection::PauseReadForQuota() {
  if (read_paused_for_quota_) return;
  read_paused_for_quota_ = true;
  bufferevent_disable(bev_, EV_READ);
}

void Connection::ResumeReadIfPaused() {
  if (!read_paused_for_quota_) return;
  read_paused_for_quota_ = false;
  bufferevent_enable(bev_, EV_READ);
  bufferevent_trigger(bev_, EV_READ, BEV_TRIG_IGNORE_WATERMARKS);
}

void Connection::ScheduleResume() {
  if (resume_scheduled_) return;
  if (!resume_event_) {
    auto *base = bufferevent_get_base(bev_);
    resume_event_.reset(evtimer_new(base, EventCallbackFunc<&Connection::OnResume>, this));
  }
  resume_scheduled_ = true;
  timeval tv = {0, 0};
  evtimer_add(resume_event_.get(), &tv);
}

void Connection::OnResume(int, int16_t) {
  resume_scheduled_ = false;
  if (IsFlagEnabled(kCloseAsync)) {
    Close();
    return;
  }

  is_running_ = true;
  MakeScopeExit([this] { is_running_ = false; });

  auto config = srv_->GetConfig()->GetSnapshot();
  size_t max_cmds = static_cast<size_t>(config->read_event_max_commands);
  int64_t max_time_us = config->read_event_max_time_us;
  auto result = ExecuteCommandsWithBudget(req_.GetCommands(), max_cmds, max_time_us, true);
  if (IsFlagEnabled(kCloseAsync)) {
    Close();
    return;
  }

  if (result == ExecuteResult::kYielded) {
    PauseReadForQuota();
    ScheduleResume();
  } else if (result == ExecuteResult::kBlocked) {
    read_paused_for_quota_ = false;
  } else {
    ResumeReadIfPaused();
  }
}

void Connection::OnWrite([[maybe_unused]] bufferevent *bev) {
  if (IsFlagEnabled(kCloseAfterReply) || IsFlagEnabled(kCloseAsync)) {
    Close();
  }
}

void Connection::OnEvent(bufferevent *bev, int16_t events) {
  if (events & BEV_EVENT_ERROR) {
#ifdef ENABLE_OPENSSL
    error("[connection] Removing client: {}, error: {}, SSL Error: {}", GetAddr(),
          evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()),
          fmt::streamed(SSLError(bufferevent_get_openssl_error(bev))));  // NOLINT
#else
    error("[connection] Removing client: {}, error: {}", GetAddr(),
          evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
#endif
    Close();
    return;
  }

  if (events & BEV_EVENT_EOF) {
    debug("[connection] Going to remove the client: {}, while closed by client", GetAddr());
    Close();
    return;
  }

  if (events & BEV_EVENT_TIMEOUT) {
    debug("[connection] The client: {} reached timeout", GetAddr());
    bufferevent_enable(bev, EV_READ | EV_WRITE);
  }
}

void Connection::Reply(const std::string &msg) {
  if (reply_mode_ == ReplyMode::SKIP) {
    reply_mode_ = ReplyMode::ON;
    return;
  }
  if (reply_mode_ == ReplyMode::OFF) {
    return;
  }

  owner_->srv->stats.IncrOutboundBytes(msg.size());
  const auto& ns = GetNamespace();
  if (!ns.empty()) {
    owner_->IncrOutboundBytesForNamespace(ns, msg.size());
  }
  if (in_exec_) {
    queued_replies_.push_back(msg);
  } else if (owner_->IsBatchReplyDeferralActive()) {
    owner_->EnqueueBatchReply(GetFD(), GetID(), msg);
  } else {
    redis::Reply(bufferevent_get_output(bev_), msg);
  }
}

const std::vector<std::string> &Connection::GetQueuedReplies() const { return queued_replies_; }

void Connection::EnqueueDeferredExecWatchUpdate(bool mark_all_keys, std::vector<std::string> keys) {
  if (!in_exec_) return;

  if (mark_all_keys) {
    deferred_exec_watch_all_keys_ = true;
    deferred_exec_watch_keys_.clear();
    return;
  }

  if (deferred_exec_watch_all_keys_ || keys.empty()) return;
  auto old_size = deferred_exec_watch_keys_.size();
  deferred_exec_watch_keys_.resize(old_size + keys.size());
  std::move(keys.begin(), keys.end(), deferred_exec_watch_keys_.begin() + static_cast<std::ptrdiff_t>(old_size));
}

void Connection::ApplyDeferredExecWatchUpdates() {
  if (!srv_->HasWatchedKeys()) {
    deferred_exec_watch_all_keys_ = false;
    deferred_exec_watch_keys_.clear();
    return;
  }

  if (deferred_exec_watch_all_keys_) {
    srv_->MarkAllWatchedKeysModified();
  } else if (!deferred_exec_watch_keys_.empty()) {
    srv_->UpdateWatchedKeysManually(GetNamespace(), deferred_exec_watch_keys_);
  }
  deferred_exec_watch_all_keys_ = false;
  deferred_exec_watch_keys_.clear();
}

void Connection::UpdateWatchedKeysManually(const std::vector<std::string> &keys) {
  if (keys.empty() || !srv_->HasWatchedKeys()) return;
  if (in_exec_) {
    EnqueueDeferredExecWatchUpdate(false, std::vector<std::string>(keys.begin(), keys.end()));
    return;
  }
  srv_->UpdateWatchedKeysManually(GetNamespace(), keys);
}

void Connection::SendFile(int fd) {
  // NOTE: we don't need to close the fd, the libevent will do that
  auto output = bufferevent_get_output(bev_);
  evbuffer_add_file(output, fd, 0, -1);
}

void Connection::SetAddr(std::string ip, uint32_t port) {
  std::lock_guard<std::mutex> lock(client_mu_);
  ip_ = std::move(ip);
  port_.store(port, std::memory_order_relaxed);
  addr_ = ip_ + ":" + std::to_string(port);
}

void Connection::SetNamespace(std::string ns) {
  bool should_count = false;
  std::string ns_for_stats;
  {
    std::lock_guard<std::mutex> lock(client_mu_);
    // Count only on first authentication (ns_ was empty, new ns is not empty)
    // Atomic compare_exchange prevents TOCTOU race on connection_counted_
    if (!ns.empty() && ns_.empty()) {
      bool expected = false;
      if (connection_counted_.compare_exchange_strong(expected, true)) {
        should_count = true;
        ns_for_stats = ns;
      }
    }
    ns_ = std::move(ns);
  }
  if (should_count) {
    owner_->IncrConnectionsForNamespace(ns_for_stats);
  }
}

uint64_t Connection::GetAge() const { return static_cast<uint64_t>(util::GetTimeStamp() - create_time_); }

void Connection::SetLastInteraction() { last_interaction_.store(util::GetTimeStamp(), std::memory_order_relaxed); }

uint64_t Connection::GetIdleTime() const {
  return static_cast<uint64_t>(util::GetTimeStamp() - last_interaction_.load(std::memory_order_relaxed));
}

std::string Connection::GetName() const {
  std::lock_guard<std::mutex> lock(client_mu_);
  return name_;
}

void Connection::SetName(std::string name) {
  std::lock_guard<std::mutex> lock(client_mu_);
  name_ = std::move(name);
}

std::string Connection::GetAddr() const {
  std::lock_guard<std::mutex> lock(client_mu_);
  return addr_;
}

void Connection::SetLastCmd(std::string cmd) {
  std::lock_guard<std::mutex> lock(client_mu_);
  last_cmd_ = std::move(cmd);
}

std::string Connection::GetIP() const {
  std::lock_guard<std::mutex> lock(client_mu_);
  return ip_;
}

void Connection::SetAnnounceIP(std::string ip) {
  std::lock_guard<std::mutex> lock(client_mu_);
  announce_ip_ = std::move(ip);
}

std::string Connection::GetAnnounceIP() const {
  std::lock_guard<std::mutex> lock(client_mu_);
  return !announce_ip_.empty() ? announce_ip_ : ip_;
}

std::string Connection::GetAnnounceAddr() const {
  auto announce_ip = GetAnnounceIP();
  return announce_ip + ":" + std::to_string(GetAnnouncePort());
}

// Currently, master connection is not handled in connection
// but in replication thread.
//
// The function will return one of the following:
//  kTypeSlave  -> Slave
//  kTypeNormal -> Normal client
//  kTypePubsub -> Client subscribed to Pub/Sub channels
uint64_t Connection::GetClientType() const {
  if (IsFlagEnabled(kSlave)) return kTypeSlave;

  if (subscribe_channels_count_.load(std::memory_order_relaxed) > 0 ||
      subscribe_patterns_count_.load(std::memory_order_relaxed) > 0 ||
      subscribe_shard_channels_count_.load(std::memory_order_relaxed) > 0) {
    return kTypePubsub;
  }

  return kTypeNormal;
}

std::string Connection::GetFlags() const {
  std::string flags;
  if (IsFlagEnabled(kSlave)) flags.append("S");
  if (IsFlagEnabled(kCloseAfterReply)) flags.append("c");
  if (IsFlagEnabled(kMonitor)) flags.append("M");
  if (IsFlagEnabled(kAsking)) flags.append("A");
  if (subscribe_channels_count_.load(std::memory_order_relaxed) > 0 ||
      subscribe_patterns_count_.load(std::memory_order_relaxed) > 0 ||
      subscribe_shard_channels_count_.load(std::memory_order_relaxed) > 0) {
    flags.append("P");
  }
  if (flags.empty()) flags = "N";
  return flags;
}

void Connection::EnableFlag(Flag flag) { flags_ |= flag; }

void Connection::DisableFlag(Flag flag) { flags_ &= (~flag); }

bool Connection::IsFlagEnabled(Flag flag) const { return (flags_ & flag) > 0; }

bool Connection::CanMigrate() const {
  return !is_running_                                                    // reading or writing
         && !has_pending_cmds_.load(std::memory_order_relaxed)           // pending commands
         && !IsFlagEnabled(redis::Connection::kCloseAfterReply)          // close after reply
         && !IsFlagEnabled(redis::Connection::kMonitor)                  // monitor connections have worker-specific registration
         && saved_current_command_ == nullptr                            // not executing blocking command like BLPOP
         && subscribe_channels_count_.load(std::memory_order_relaxed) == 0
         && subscribe_patterns_count_.load(std::memory_order_relaxed) == 0
         && subscribe_shard_channels_count_.load(std::memory_order_relaxed) == 0;  // not subscribing any channel
}

void Connection::SubscribeChannel(const std::string &channel) {
  for (const auto &chan : subscribe_channels_) {
    if (channel == chan) return;
  }

  subscribe_channels_.emplace_back(channel);
  subscribe_channels_count_.fetch_add(1, std::memory_order_relaxed);
  owner_->srv->SubscribeChannel(channel, this);
}

void Connection::UnsubscribeChannel(const std::string &channel) {
  for (auto iter = subscribe_channels_.begin(); iter != subscribe_channels_.end(); iter++) {
    if (*iter == channel) {
      subscribe_channels_.erase(iter);
      subscribe_channels_count_.fetch_sub(1, std::memory_order_relaxed);
      owner_->srv->UnsubscribeChannel(channel, this);
      return;
    }
  }
}

void Connection::UnsubscribeAll(const UnsubscribeCallback &reply) {
  if (subscribe_channels_.empty()) {
    if (reply) reply("", static_cast<int>(subscribe_patterns_.size()));
    return;
  }

  int removed = 0;
  for (const auto &chan : subscribe_channels_) {
    owner_->srv->UnsubscribeChannel(chan, this);
    removed++;
    if (reply) {
      reply(chan, static_cast<int>(subscribe_channels_.size() - removed + subscribe_patterns_.size()));
    }
  }
  subscribe_channels_.clear();
  subscribe_channels_count_.store(0, std::memory_order_relaxed);
}

int Connection::SubscriptionsCount() { return subscribe_channels_count_.load(std::memory_order_relaxed); }

void Connection::PSubscribeChannel(const std::string &pattern) {
  for (const auto &p : subscribe_patterns_) {
    if (pattern == p) return;
  }
  subscribe_patterns_.emplace_back(pattern);
  subscribe_patterns_count_.fetch_add(1, std::memory_order_relaxed);
  owner_->srv->PSubscribeChannel(pattern, this);
}

void Connection::PUnsubscribeChannel(const std::string &pattern) {
  for (auto iter = subscribe_patterns_.begin(); iter != subscribe_patterns_.end(); iter++) {
    if (*iter == pattern) {
      subscribe_patterns_.erase(iter);
      subscribe_patterns_count_.fetch_sub(1, std::memory_order_relaxed);
      owner_->srv->PUnsubscribeChannel(pattern, this);
      return;
    }
  }
}

void Connection::PUnsubscribeAll(const UnsubscribeCallback &reply) {
  if (subscribe_patterns_.empty()) {
    if (reply) reply("", static_cast<int>(subscribe_channels_.size()));
    return;
  }

  int removed = 0;
  for (const auto &pattern : subscribe_patterns_) {
    owner_->srv->PUnsubscribeChannel(pattern, this);
    removed++;
    if (reply) {
      reply(pattern, static_cast<int>(subscribe_patterns_.size() - removed + subscribe_channels_.size()));
    }
  }
  subscribe_patterns_.clear();
  subscribe_patterns_count_.store(0, std::memory_order_relaxed);
}

int Connection::PSubscriptionsCount() { return subscribe_patterns_count_.load(std::memory_order_relaxed); }

void Connection::SSubscribeChannel(const std::string &channel, uint16_t slot) {
  for (const auto &chan : subscribe_shard_channels_) {
    if (channel == chan) return;
  }

  subscribe_shard_channels_.emplace_back(channel);
  subscribe_shard_channels_count_.fetch_add(1, std::memory_order_relaxed);
  owner_->srv->SSubscribeChannel(channel, this, slot);
}

void Connection::SUnsubscribeChannel(const std::string &channel, uint16_t slot) {
  for (auto iter = subscribe_shard_channels_.begin(); iter != subscribe_shard_channels_.end(); iter++) {
    if (*iter == channel) {
      subscribe_shard_channels_.erase(iter);
      subscribe_shard_channels_count_.fetch_sub(1, std::memory_order_relaxed);
      owner_->srv->SUnsubscribeChannel(channel, this, slot);
      return;
    }
  }
}

void Connection::SUnsubscribeAll(const UnsubscribeCallback &reply) {
  if (subscribe_shard_channels_.empty()) {
    if (reply) reply("", 0);
    return;
  }

  int removed = 0;
  for (const auto &chan : subscribe_shard_channels_) {
    owner_->srv->SUnsubscribeChannel(chan, this,
                                     owner_->srv->GetConfig()->cluster_enabled ? GetSlotIdFromKey(chan) : 0);
    removed++;
    if (reply) {
      reply(chan, static_cast<int>(subscribe_shard_channels_.size() - removed));
    }
  }
  subscribe_shard_channels_.clear();
  subscribe_shard_channels_count_.store(0, std::memory_order_relaxed);
}

int Connection::SSubscriptionsCount() {
  return subscribe_shard_channels_count_.load(std::memory_order_relaxed);
}

bool Connection::IsSubscribed() const {
  return subscribe_channels_count_.load(std::memory_order_relaxed) > 0 ||
         subscribe_patterns_count_.load(std::memory_order_relaxed) > 0 ||
         subscribe_shard_channels_count_.load(std::memory_order_relaxed) > 0;
}

bool Connection::IsProfilingEnabled(const std::string &cmd) {
  auto config = srv_->GetConfig()->GetSnapshot();
  if (config->profiling_sample_ratio == 0) return false;

  if (!config->profiling_sample_all_commands &&
      config->profiling_sample_commands.find(cmd) == config->profiling_sample_commands.end()) {
    return false;
  }

  if (config->profiling_sample_ratio == 100 || std::rand() % 100 <= config->profiling_sample_ratio) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_iostats_context()->Reset();
    return true;
  }

  return false;
}

void Connection::RecordProfilingSampleIfNeed(const std::string &cmd, uint64_t duration) {
  int threshold = srv_->GetConfig()->GetSnapshot()->profiling_sample_record_threshold_ms;
  if (threshold > 0 && static_cast<int>(duration / 1000) < threshold) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    return;
  }

  std::string perf_context = rocksdb::get_perf_context()->ToString(true);
  std::string iostats_context = rocksdb::get_iostats_context()->ToString(true);
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  if (perf_context.empty()) return;  // request without db operation

  auto entry = std::make_unique<PerfEntry>();
  entry->cmd_name = cmd;
  entry->duration = duration;
  entry->iostats_context = std::move(iostats_context);
  entry->perf_context = std::move(perf_context);
  entry->ns = GetNamespace();
  srv_->GetPerfLog()->PushEntry(std::move(entry));
}

Status Connection::ExecuteCommand(engine::Context &ctx, const std::string &cmd_name,
                                  const std::vector<std::string> &cmd_tokens, Commander *current_cmd,
                                  std::string *reply) {
  srv_->stats.IncrCalls(cmd_name);
  const auto& ns = GetNamespace();
  if (!ns.empty()) {
    owner_->IncrCallsForNamespace(ns);
  }

  auto start = std::chrono::high_resolution_clock::now();
  bool is_profiling = IsProfilingEnabled(cmd_name);
  auto s = current_cmd->Execute(ctx, srv_, this, reply);
  auto end = std::chrono::high_resolution_clock::now();
  uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  if (is_profiling) RecordProfilingSampleIfNeed(cmd_name, duration);

  srv_->SlowlogPushEntryIfNeeded(&cmd_tokens, duration, this);
  srv_->stats.IncrLatency(static_cast<uint64_t>(duration), cmd_name);

  // Per-namespace command stats (after latency measurement)
  if (!ns.empty()) {
    owner_->IncrCommandStatForNamespace(ns, cmd_name, static_cast<uint64_t>(duration));
  }

  return s;
}

static bool IsCmdForIndexing(uint64_t cmd_flags, CommandCategory cmd_cat) {
  return (cmd_flags & redis::kCmdWrite) &&
         (cmd_cat == CommandCategory::Hash || cmd_cat == CommandCategory::JSON || cmd_cat == CommandCategory::Key ||
          cmd_cat == CommandCategory::Script || cmd_cat == CommandCategory::Function);
}

static bool IsCmdAllowedInStaleData(const std::string &cmd_name) {
  return cmd_name == "info" || cmd_name == "slaveof" || cmd_name == "config";
}

static bool IsAllowedInSubscribedMode(const std::string &cmd_name) {
  return cmd_name == "subscribe" || cmd_name == "unsubscribe" || cmd_name == "psubscribe" ||
         cmd_name == "punsubscribe" || cmd_name == "ssubscribe" || cmd_name == "sunsubscribe" ||
         cmd_name == "ping" || cmd_name == "quit" || cmd_name == "reset";
}

static const CommandAttributes *LookupCommandAttributesByName(const std::string &cmd_name) {
  if (cmd_name.empty()) return nullptr;
  auto commands = redis::CommandTable::Get();
  auto iter = commands->find(util::ToLower(cmd_name));
  if (iter == commands->end()) return nullptr;
  return iter->second;
}

static bool HelloHasAuthOption(const CommandTokens &cmd_tokens) {
  if (cmd_tokens.size() < 2) return false;

  size_t next_arg = 1;
  auto protocol = ParseInt<int>(cmd_tokens[next_arg], 10);
  if (!protocol) {
    return false;
  }
  ++next_arg;

  for (; next_arg < cmd_tokens.size(); ++next_arg) {
    size_t more_args = cmd_tokens.size() - next_arg - 1;
    const auto &opt = cmd_tokens[next_arg];
    if (util::EqualICase(opt, "auth") && more_args != 0) {
      return true;
    }
    if (util::EqualICase(opt, "setname") && more_args != 0) {
      ++next_arg;
      continue;
    }
    return false;
  }

  return false;
}

static bool IsBatchBarrierCommand(const std::string &cmd_name, const CommandAttributes *attributes,
                                  uint64_t cmd_flags, const CommandTokens &cmd_tokens) {
  if ((cmd_flags & kCmdBlocking) != 0) return true;
  if ((cmd_flags & kCmdExclusive) != 0) return true;
  if (cmd_name == "auth" || cmd_name == "reset") return true;
  if (cmd_name == "hello" && HelloHasAuthOption(cmd_tokens)) return true;
  if (cmd_name == "multi" || cmd_name == "exec" || cmd_name == "watch" || cmd_name == "unwatch" ||
      cmd_name == "applybatch") {
    return true;
  }
  if (attributes->category == CommandCategory::Script || attributes->category == CommandCategory::Function) return true;
  return false;
}

static size_t EstimateCommandBytes(const CommandTokens &cmd_tokens) {
  size_t bytes = 0;
  for (const auto &token : cmd_tokens) {
    bytes += token.size();
  }
  return bytes;
}

static void CollectWatchKeysFromCommand(const CommandAttributes &attr, const std::vector<std::string> &args,
                                        bool *mark_all_keys, std::vector<std::string> *keys) {
  *mark_all_keys = false;
  keys->clear();
  attr.ForEachKeyRange(
      [keys](const std::vector<std::string> &tokens, const CommandKeyRange &range) {
        for (size_t i = range.first_key;
             range.last_key > 0 ? i <= size_t(range.last_key) : i <= tokens.size() + range.last_key;
             i += range.key_step) {
          keys->emplace_back(tokens[i]);
        }
      },
      args, [mark_all_keys](const std::vector<std::string> &) { *mark_all_keys = true; });
  if (*mark_all_keys) {
    keys->clear();
  }
}

void Connection::ExecuteCommands(std::deque<CommandTokens> *to_process_cmds) {
  ExecuteCommandsWithBudget(to_process_cmds, 0, 0, false);
}

Connection::ExecuteResult Connection::ExecuteCommandsWithBudget(std::deque<CommandTokens> *to_process_cmds,
                                                                size_t max_cmds, int64_t max_time_us,
                                                                bool allow_yield) {
  std::string reply;
  bool yielded = false;
  bool blocked = false;
  size_t processed = 0;
  size_t heavy_used = 0;
  size_t max_heavy = static_cast<size_t>(srv_->GetConfig()->GetSnapshot()->read_event_max_heavy);
  constexpr size_t kTimeCheckInterval = 16;
  auto start = std::chrono::steady_clock::now();

  auto should_yield = [&]() {
    if (!allow_yield) return false;
    if (max_cmds > 0 && processed >= max_cmds) return true;
    if (max_time_us > 0 && processed > 0 && (processed % kTimeCheckInterval == 0)) {
      auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start)
                         .count();
      return elapsed >= max_time_us;
    }
    return false;
  };

  while (!to_process_cmds->empty()) {
    if (allow_yield && (max_cmds > 0 || max_time_us > 0 || max_heavy > 0)) {
      const CommandAttributes *next_attr = nullptr;
      bool next_is_heavy = false;
      bool next_is_exec = false;
      if (const auto &front = to_process_cmds->front(); !front.empty()) {
        next_attr = LookupCommandAttributesByName(front.front());
      }
      if (next_attr != nullptr) {
        next_is_heavy = (next_attr->InitialFlags() & kCmdHeavy) != 0;
        next_is_exec = next_attr->name == "exec";
      }

      if (max_heavy > 0 && heavy_used >= max_heavy && next_is_heavy) {
        yielded = true;
        break;
      }
      if (processed > 0 && next_is_exec) {
        yielded = true;
        break;
      }
      if (max_time_us > 0 && processed > 0 && next_is_exec) {
        auto elapsed =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
        if (elapsed >= max_time_us) {
          yielded = true;
          break;
        }
      }
      if (should_yield()) {
        yielded = true;
        break;
      }
    }

    CommandTokens cmd_tokens = std::move(to_process_cmds->front());
    to_process_cmds->pop_front();
    if (cmd_tokens.empty()) continue;
    processed++;
    auto config = srv_->GetConfig()->GetSnapshot();
    auto close_idle_batch = MakeScopeExit([this, &config] {
      if (config->batching_enabled) {
        owner_->CloseIdleBatchContext();
      }
    });
    bool cluster_enabled = srv_->GetConfig()->cluster_enabled;
    const std::string &password = config->requirepass;

    bool is_multi_exec = IsFlagEnabled(Connection::kMultiExec);
    if (IsFlagEnabled(redis::Connection::kCloseAfterReply) && !is_multi_exec) break;
    if (IsFlagEnabled(redis::Connection::kCloseAsync)) break;
    auto multi_error_exit = MakeScopeExit([&] {
      if (is_multi_exec) multi_error_ = true;
    });

    auto cmd_s = Server::LookupAndCreateCommand(cmd_tokens.front());
    if (!cmd_s.IsOK()) {
      auto cmd_name = cmd_tokens.front();
      if (util::EqualICase(cmd_name, "host:") || util::EqualICase(cmd_name, "post")) {
        warn(
            "[connection] A likely HTTP request is detected in the RESP connection, indicating a potential "
            "Cross-Protocol Scripting attack. Connection aborted.");
        EnableFlag(kCloseAsync);
        break;
      }
      Reply(redis::Error(
          {Status::NotOK,
           fmt::format("unknown command `{}`, with args beginning with: {}", cmd_name,
                       util::StringJoin(nonstd::span(cmd_tokens.begin() + 1, cmd_tokens.end()),
                                        [](const auto &v) -> decltype(auto) { return fmt::format("`{}`", v); }))}));
      continue;
    }
    auto current_cmd = std::move(*cmd_s);

    const auto &attributes = current_cmd->GetAttributes();
    auto cmd_name = attributes->name;

    if (GetProtocolVersion() != RESP::v3 && IsSubscribed() && !IsAllowedInSubscribedMode(cmd_name)) {
      Reply(redis::Error({Status::NotOK, errSubscribedModeOnlyPubSub}));
      continue;
    }

    int tokens = static_cast<int>(cmd_tokens.size());
    if (!attributes->CheckArity(tokens)) {
      Reply(redis::Error({Status::NotOK, "wrong number of arguments"}));
      continue;
    }

    auto cmd_flags = attributes->GenerateFlags(cmd_tokens, *config);
    if (GetNamespace().empty()) {
      if (!password.empty()) {
        if (!(cmd_flags & kCmdAuth)) {
          Reply(redis::Error({Status::RedisNoAuth, "Authentication required."}));
          continue;
        }
      } else {
        BecomeAdmin();
        SetNamespace(kDefaultNamespace);
      }
    }

    bool batch_barrier = IsBatchBarrierCommand(cmd_name, attributes, cmd_flags, cmd_tokens);
    if (config->batching_enabled && owner_->IsBatchReplyDeferralActive() && batch_barrier) {
      owner_->FlushBatchReplies();
    }
    bool batch_candidate = config->batching_enabled && (cmd_flags & kCmdWrite) && !in_exec_ && !batch_barrier;
    if (batch_candidate) {
      auto ensure_batch = owner_->EnsureBatchContext(ns_);
      if (!ensure_batch.IsOK()) {
        Reply(redis::Error(ensure_batch));
        continue;
      }
    }
    bool in_namespace_batch = config->batching_enabled && owner_->HasBatchContextForNamespace(ns_);

    std::shared_lock<std::shared_mutex> concurrency;  // Allow concurrency
    std::unique_lock<std::shared_mutex> exclusivity;  // Need exclusivity
    // If the command needs to process exclusively, we need to get 'ExclusivityGuard'
    // that can guarantee other threads can't come into critical zone, such as DEBUG,
    // CLUSTER subcommand, CONFIG SET, MULTI, LUA (in the immediate future).
    // Otherwise, we just use 'ConcurrencyGuard' to allow all workers to execute commands at the same time.
    //
    // For namespace-isolated commands (like EXEC), we use namespace-specific locks
    // so that different namespaces can execute exclusive commands in parallel.
    if (is_multi_exec && !(cmd_flags & kCmdBypassMulti)) {
      // No lock guard, because 'exec' command has acquired 'WorkExclusivityGuard'
    } else if (cmd_flags & kCmdNoLock) {
      // No lock needed - command only sets atomic flags (e.g., SCRIPT KILL)
    } else if (!in_namespace_batch && (cmd_flags & kCmdExclusive)) {
      // Use namespace-specific lock if namespace is set, otherwise use global lock
      if (!ns_.empty()) {
        exclusivity = srv_->WorkExclusivityGuard(ns_);
      } else {
        exclusivity = srv_->WorkExclusivityGuard();
      }
    } else if (!in_namespace_batch) {
      // Use namespace-specific concurrency guard if namespace is set
      if (!ns_.empty()) {
        concurrency = srv_->WorkConcurrencyGuard(ns_);
      } else {
        concurrency = srv_->WorkConcurrencyGuard();
      }
    }

    if (srv_->IsLoading() && !(cmd_flags & kCmdLoading)) {
      Reply(redis::Error({Status::RedisLoading, errRestoringBackup}));
      continue;
    }

    current_cmd->SetArgs(cmd_tokens);
    auto s = current_cmd->Parse();
    if (!s.IsOK()) {
      Reply(redis::Error(s));
      continue;
    }

    if (is_multi_exec && (cmd_flags & kCmdNoMulti)) {
      Reply(redis::Error({Status::NotOK, fmt::format("{} inside MULTI is not allowed", util::ToUpper(cmd_name))}));
      continue;
    }

    if ((cmd_flags & kCmdAdmin) && !IsAdmin()) {
      Reply(redis::Error({Status::RedisExecErr, errAdminPermissionRequired}));
      continue;
    }

    if (cluster_enabled) {
      s = srv_->cluster->CanExecByMySelf(attributes, cmd_tokens, this);
      if (!s.IsOK()) {
        Reply(redis::Error(s));
        continue;
      }
    }

    // reset the ASKING flag after executing the next query
    if (IsFlagEnabled(kAsking)) {
      DisableFlag(kAsking);
    }

    multi_error_exit.Disable();
    // We don't execute commands, but queue them, and then execute in EXEC command
    if (is_multi_exec && !in_exec_ && !(cmd_flags & kCmdBypassMulti)) {
      multi_cmds_.emplace_back(std::move(cmd_tokens));
      Reply(redis::SimpleString("QUEUED"));
      continue;
    }
    if (cmd_flags & kCmdHeavy) {
      heavy_used++;
    }

    if (config->slave_readonly && srv_->IsSlave() && (cmd_flags & kCmdWrite)) {
      Reply(redis::Error({Status::RedisReadOnly, "You can't write against a read only slave."}));
      continue;
    }

    if ((cmd_flags & kCmdWrite) && !(cmd_flags & kCmdNoDBSizeCheck) && srv_->storage->ReachedDBSizeLimit()) {
      Reply(redis::Error({Status::NotOK, "write command not allowed when reached max-db-size."}));
      continue;
    }

    if (!config->slave_serve_stale_data && srv_->IsSlave() && !IsCmdAllowedInStaleData(cmd_name) &&
        srv_->GetReplicationState() != kReplConnected) {
      Reply(redis::Error({Status::RedisMasterDown,
                          "Link with MASTER is down "
                          "and slave-serve-stale-data is set to 'no'."}));
      continue;
    }

    ScopeExit in_script_exit{[this] { in_script_ = false; }, false};
    if (attributes->category == CommandCategory::Script || attributes->category == CommandCategory::Function) {
      in_script_ = true;
      in_script_exit.Enable();
    }

    SetLastCmd(cmd_name);
    {
      std::optional<MultiLockGuard> guard;
      if (cmd_flags & kCmdWrite) {
        std::vector<std::string> lock_keys;
        attributes->ForEachKeyRange(
            [&lock_keys, this](const std::vector<std::string> &args, const CommandKeyRange &key_range) {
              key_range.ForEachKey(
                  [&, this](const std::string &key) {
                    auto ns_key = ComposeNamespaceKey(ns_, key, srv_->storage->IsSlotIdEncoded());
                    lock_keys.emplace_back(std::move(ns_key));
                  },
                  args);
            },
            cmd_tokens);

        guard.emplace(srv_->storage->GetLockManager(), lock_keys);
      }
      engine::Context ctx(srv_->storage, ns_);

      std::vector<GlobalIndexer::RecordResult> index_records;
      if (!srv_->index_mgr.index_map.empty() && IsCmdForIndexing(cmd_flags, attributes->category) &&
          !cluster_enabled) {
        attributes->ForEachKeyRange(
            [&, this](const std::vector<std::string> &args, const CommandKeyRange &key_range) {
              key_range.ForEachKey(
                  [&, this](const std::string &key) {
                    auto res = srv_->indexer.Record(ctx, key, ns_);
                    if (res.IsOK()) {
                      index_records.push_back(*res);
                    } else if (!res.Is<Status::NoPrefixMatched>() && !res.Is<Status::TypeMismatched>()) {
                      warn("[connection] index recording failed for key: {}", key);
                    }
                  },
                  args);
            },
            cmd_tokens);
      }

      s = ExecuteCommand(ctx, cmd_name, cmd_tokens, current_cmd.get(), &reply);
      for (const auto &record : index_records) {
        auto s = GlobalIndexer::Update(ctx, record);
        if (!s.IsOK() && !s.Is<Status::TypeMismatched>()) {
          warn("[connection] index updating failed for key: {}", record.key);
        }
      }
    }

    bool flush_batch_after_reply = false;
    if (s.IsOK() && config->batching_enabled && (cmd_flags & kCmdWrite) && !in_exec_ && !batch_barrier) {
      flush_batch_after_reply = owner_->OnBatchWrite(EstimateCommandBytes(cmd_tokens));
    }

    if (!(cmd_flags & redis::kCmdSkipMonitor)) {
      srv_->FeedMonitorConns(this, cmd_tokens);
    }

    // Break the execution loop when occurring the blocking command like BLPOP or BRPOP,
    // it will suspend the connection and wait for the wakeup signal.
    if (s.Is<Status::BlockingCmd>()) {
      // For the blocking command, it will use the command while resumed from the suspend state.
      // So we need to save the command for the next execution.
      // Migrate connection would also check the saved_current_command_ to determine whether
      // the connection can be migrated or not.
      saved_current_command_ = std::move(current_cmd);
      blocked = true;
      break;
    }

    // Reply for MULTI
    if (!s.IsOK()) {
      Reply(redis::Error(s));
      continue;
    }

    if (srv_->HasWatchedKeys() && (cmd_flags & kCmdWrite)) {
      bool deferred_watch_update = in_exec_ || (config->batching_enabled && !in_exec_ && !batch_barrier);
      if (deferred_watch_update) {
        bool mark_all_keys = false;
        std::vector<std::string> watched_keys;
        CollectWatchKeysFromCommand(*attributes, cmd_tokens, &mark_all_keys, &watched_keys);
        if (in_exec_) {
          EnqueueDeferredExecWatchUpdate(mark_all_keys, std::move(watched_keys));
        } else {
          owner_->EnqueueBatchWatchUpdate(mark_all_keys, std::move(watched_keys));
        }
      } else {
        srv_->UpdateWatchedKeysFromArgs(GetNamespace(), cmd_tokens, *attributes);
      }
    }

    if (!reply.empty()) Reply(reply);
    reply.clear();
    if (flush_batch_after_reply) {
      owner_->FlushBatchReplies();
    }
  }

  has_pending_cmds_.store(!to_process_cmds->empty(), std::memory_order_relaxed);
  if (blocked) return ExecuteResult::kBlocked;
  if (yielded) return ExecuteResult::kYielded;
  return ExecuteResult::kDrained;
}

void Connection::ResetMultiExec() {
  in_exec_ = false;
  deferred_exec_watch_all_keys_ = false;
  deferred_exec_watch_keys_.clear();
  multi_error_ = false;
  multi_cmds_.clear();
  DisableFlag(Connection::kMultiExec);
}

}  // namespace redis
