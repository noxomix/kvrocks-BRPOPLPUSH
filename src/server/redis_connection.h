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

#include <atomic>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "commands/commander.h"
#include "event_util.h"
#include "redis_request.h"
#include "server/redis_reply.h"
#include "server/watched_keys_update.h"

class Worker;

namespace redis {

class Connection : public EvbufCallbackBase<Connection> {
 public:
  struct ClientInfo {
    uint64_t id = 0;
    uint64_t type = 0;
    std::string addr;
    std::string announce_addr;
    std::string name;
    std::string ns;
    std::string last_cmd;
    std::string flags;
    uint64_t age = 0;
    uint64_t idle = 0;
    int fd = -1;
    size_t qbuf = 0;
    size_t obuf = 0;
    bool close_after_reply = false;
    bool is_slave = false;
    bool is_monitor = false;
    uint32_t worker = 0;
  };

  enum Flag {
    kSlave = 1 << 4,
    kMonitor = 1 << 5,
    kCloseAfterReply = 1 << 6,
    kCloseAsync = 1 << 7,
    kMultiExec = 1 << 8,
    kReadOnly = 1 << 9,
    kAsking = 1 << 10,
  };

  enum class ReplyMode {
    ON,   // Always reply to every command (default)
    OFF,  // Never reply to any command
    SKIP  // Skip reply for the next command, then automatically switch back to ON
  };

  explicit Connection(bufferevent *bev, Worker *owner);
  ~Connection();

  Connection(const Connection &) = delete;
  Connection &operator=(const Connection &) = delete;

  void Close();
  void Detach();
  void OnRead(bufferevent *bev);
  void OnWrite(bufferevent *bev);
  void OnEvent(bufferevent *bev, int16_t events);
  void SendFile(int fd);
  std::string ToString();
  ClientInfo GetClientInfo(bool include_buffers = false) const;
  static std::string FormatClientInfo(const ClientInfo &info);

  void Reply(const std::string &msg);
  const std::vector<std::string> &GetQueuedReplies() const;
  void ClearQueuedReplies() { queued_replies_.clear(); }
  RESP GetProtocolVersion() const { return protocol_version_; }
  void SetProtocolVersion(RESP version) { protocol_version_ = version; }
  std::string Bool(bool b) const { return redis::Bool(protocol_version_, b); }
  std::string BigNumber(const std::string &n) const { return redis::BigNumber(protocol_version_, n); }
  std::string Double(double d) const { return redis::Double(protocol_version_, d); }
  std::string VerbatimString(std::string ext, const std::string &data) const {
    return redis::VerbatimString(protocol_version_, std::move(ext), data);
  }
  std::string NilString() const { return redis::NilString(protocol_version_); }
  std::string NilArray() const { return redis::NilArray(protocol_version_); }
  std::string MultiBulkString(const std::vector<std::string> &values) const {
    return redis::MultiBulkString(protocol_version_, values);
  }
  std::string MultiBulkString(const std::vector<std::string> &values,
                              const std::vector<rocksdb::Status> &statuses) const {
    return redis::MultiBulkString(protocol_version_, values, statuses);
  }
  template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
  std::string HeaderOfSet(T len) const {
    return redis::HeaderOfSet(protocol_version_, len);
  }
  std::string SetOfBulkStrings(const std::vector<std::string> &elems) const {
    return redis::SetOfBulkStrings(protocol_version_, elems);
  }
  template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
  std::string HeaderOfMap(T len) const {
    return redis::HeaderOfMap(protocol_version_, len);
  }
  std::string MapOfBulkStrings(const std::vector<std::string> &elems) const {
    return redis::MapOfBulkStrings(protocol_version_, elems);
  }
  std::string Map(const std::map<std::string, std::string> &map) const { return redis::Map(protocol_version_, map); }
  template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
  std::string HeaderOfAttribute(T len) const {
    return redis::HeaderOfAttribute(len);
  }
  std::string HeaderOfPush(int64_t len) const { return redis::HeaderOfPush(protocol_version_, len); }

  using UnsubscribeCallback = std::function<void(std::string, int)>;
  void SubscribeChannel(const std::string &channel);
  void UnsubscribeChannel(const std::string &channel);
  void UnsubscribeAll(const UnsubscribeCallback &reply = nullptr);
  int SubscriptionsCount();
  void PSubscribeChannel(const std::string &pattern);
  void PUnsubscribeChannel(const std::string &pattern);
  void PUnsubscribeAll(const UnsubscribeCallback &reply = nullptr);
  int PSubscriptionsCount();
  void SSubscribeChannel(const std::string &channel, uint16_t slot);
  void SUnsubscribeChannel(const std::string &channel, uint16_t slot);
  void SUnsubscribeAll(const UnsubscribeCallback &reply = nullptr);
  int SSubscriptionsCount();
  bool IsSubscribed() const;

  uint64_t GetAge() const;
  uint64_t GetIdleTime() const;
  void SetLastInteraction();
  std::string GetFlags() const;
  void EnableFlag(Flag flag);
  void DisableFlag(Flag flag);
  bool IsFlagEnabled(Flag flag) const;

  uint64_t GetID() const { return id_.load(std::memory_order_relaxed); }
  void SetID(uint64_t id) { id_.store(id, std::memory_order_relaxed); }
  std::string GetName() const;
  void SetName(std::string name);
  std::string GetAddr() const;
  void SetAddr(std::string ip, uint32_t port);
  void SetLastCmd(std::string cmd);
  std::string GetIP() const;
  uint32_t GetPort() const { return port_.load(std::memory_order_relaxed); }
  void SetListeningPort(int port) { listening_port_.store(port, std::memory_order_relaxed); }
  int GetListeningPort() const { return listening_port_.load(std::memory_order_relaxed); }
  void SetAnnounceIP(std::string ip);
  std::string GetAnnounceIP() const;
  uint32_t GetAnnouncePort() const {
    auto listening_port = listening_port_.load(std::memory_order_relaxed);
    if (listening_port != 0) {
      return static_cast<uint32_t>(listening_port);
    }
    return port_.load(std::memory_order_relaxed);
  }
  std::string GetAnnounceAddr() const;
  uint64_t GetClientType() const;
  Server *GetServer() { return srv_; }

  bool IsAdmin() const { return is_admin_; }
  void BecomeAdmin() { is_admin_ = true; }
  void BecomeUser() { is_admin_ = false; }
  const std::string& GetNamespace() const { return ns_; }
  void SetNamespace(std::string ns);

  // SNI for fair scheduling (empty if not set)
  const std::string& GetSNI() const { return sni_; }
  void SetSNI(std::string sni) { sni_ = std::move(sni); }

  void NeedFreeBufferEvent(bool need_free = true) { need_free_bev_ = need_free; }
  void NeedNotFreeBufferEvent() { NeedFreeBufferEvent(false); }
  bool IsNeedFreeBufferEvent() const { return need_free_bev_; }

  Worker *Owner() { return owner_; }
  void SetOwner(Worker *new_owner) { owner_ = new_owner; };
  int GetFD() { return bufferevent_getfd(bev_); }
  evbuffer *Input() { return bufferevent_get_input(bev_); }
  evbuffer *Output() { return bufferevent_get_output(bev_); }
  bufferevent *GetBufferEvent() { return bev_; }
  enum class ExecuteResult { kDrained, kYielded, kBlocked };
  void ExecuteCommands(std::deque<CommandTokens> *to_process_cmds);
  ExecuteResult ExecuteCommandsWithBudget(std::deque<CommandTokens> *to_process_cmds, size_t max_cmds,
                                          int64_t max_time_us, bool allow_yield);
  Status ExecuteCommand(engine::Context &ctx, const std::string &cmd_name, const std::vector<std::string> &cmd_tokens,
                        Commander *current_cmd, std::string *reply);
  bool IsProfilingEnabled(const std::string &cmd);
  void RecordProfilingSampleIfNeed(const std::string &cmd, uint64_t duration);
  void SetImporting() { importing_ = true; }
  bool IsImporting() const { return importing_; }
  bool CanMigrate() const;

  // Multi exec
  void SetInExec() {
    in_exec_ = true;
    deferred_exec_watch_update_.Reset();
  }
  bool IsInExec() const { return in_exec_; }
  bool IsInScript() const { return in_script_; }
  bool IsMultiError() const { return multi_error_; }
  void ResetMultiExec();
  void ApplyDeferredExecWatchUpdates();
  void UpdateWatchedKeysManually(const std::vector<std::string> &keys);
  std::deque<redis::CommandTokens> *GetMultiExecCommands() { return &multi_cmds_; }

  std::function<void(int)> close_cb = nullptr;

  std::set<std::string> watched_keys;
  std::atomic<bool> watched_keys_modified = false;

  // Reply mode getter/setter
  void SetReplyMode(ReplyMode mode) { reply_mode_ = mode; }
  ReplyMode GetReplyMode() const { return reply_mode_; }

 private:
  void OnResume(int, int16_t);
  void ScheduleResume();
  void PauseReadForQuota();
  void ResumeReadIfPaused();
  void EnqueueDeferredExecWatchUpdate(bool mark_all_keys, std::vector<std::string> keys);

  std::atomic<uint64_t> id_{0};
  std::atomic<int> flags_ = 0;
  mutable std::mutex client_mu_;
  std::string ns_;  // Empty before AUTH, set via SetNamespace() after successful auth
  std::string sni_;  // SNI hostname or peer IP for fair scheduling
  std::atomic<bool> connection_counted_{false};  // Atomic to prevent TOCTOU race
  std::string name_;
  std::string ip_;
  std::string announce_ip_;
  std::atomic<uint32_t> port_{0};
  std::string addr_;
  std::atomic<int> listening_port_{0};
  bool is_admin_ = false;
  bool need_free_bev_ = true;
  std::string last_cmd_;
  int64_t create_time_;
  std::atomic<int64_t> last_interaction_{0};

  bufferevent *bev_;
  Request req_;
  Worker *owner_;
  std::unique_ptr<Commander> saved_current_command_;

  std::vector<std::string> subscribe_channels_;
  std::vector<std::string> subscribe_patterns_;
  std::vector<std::string> subscribe_shard_channels_;
  std::atomic<int> subscribe_channels_count_{0};
  std::atomic<int> subscribe_patterns_count_{0};
  std::atomic<int> subscribe_shard_channels_count_{0};

  Server *srv_;
  bool in_exec_ = false;
  bool multi_error_ = false;
  std::atomic<bool> is_running_ = false;
  std::atomic<bool> has_pending_cmds_ = false;
  std::deque<redis::CommandTokens> multi_cmds_;
  bool in_script_ = false;
  DeferredWatchKeysUpdate deferred_exec_watch_update_;

  bool importing_ = false;
  RESP protocol_version_ = RESP::v2;

  ReplyMode reply_mode_ = ReplyMode::ON;
  std::vector<std::string> queued_replies_;
  UniqueEvent resume_event_;
  bool resume_scheduled_ = false;
  bool read_paused_for_quota_ = false;
};

}  // namespace redis
