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

#include <string>
#include <string_view>
#include <vector>

#include "commands/commander.h"
#include "parse_util.h"
#include "string_util.h"

namespace redis {

enum class ScriptCommandPolicyViolation : uint8_t {
  kNone = 0,
  kReadOnlyScriptWrite,
  kNoScriptOrExclusive,
  kScriptAuthStateChange,
  kAtomicScriptPublish,
  kAtomicScriptApplyBatch,
};

inline bool IsAllowedInSubscribedMode(std::string_view cmd_name) {
  return cmd_name == "subscribe" || cmd_name == "unsubscribe" || cmd_name == "psubscribe" ||
         cmd_name == "punsubscribe" || cmd_name == "ssubscribe" || cmd_name == "sunsubscribe" || cmd_name == "ping" ||
         cmd_name == "quit" || cmd_name == "reset";
}

inline bool IsCommandAllowedInStaleData(std::string_view cmd_name) {
  return cmd_name == "info" || cmd_name == "slaveof" || cmd_name == "config";
}

inline bool IsCommandAllowedInMulti(uint64_t cmd_flags) { return (cmd_flags & kCmdNoMulti) == 0; }

inline bool HelloHasAuthOption(const std::vector<std::string> &cmd_tokens) {
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

inline bool IsBatchBarrierCommand(std::string_view cmd_name, const CommandAttributes *attributes, uint64_t cmd_flags,
                                  const std::vector<std::string> &cmd_tokens) {
  if ((cmd_flags & kCmdBlocking) != 0) return true;
  if ((cmd_flags & kCmdExclusive) != 0) return true;
  if (cmd_name == "auth" || cmd_name == "reset") return true;
  if (cmd_name == "hello" && HelloHasAuthOption(cmd_tokens)) return true;
  if (cmd_name == "multi" || cmd_name == "exec" || cmd_name == "watch" || cmd_name == "unwatch" ||
      cmd_name == "applybatch") {
    return true;
  }
  if (attributes != nullptr &&
      (attributes->category == CommandCategory::Script || attributes->category == CommandCategory::Function)) {
    return true;
  }
  return false;
}

inline bool IsScriptAuthStateChangeCommand(std::string_view cmd_name, const std::vector<std::string> &cmd_tokens) {
  if (cmd_name == "auth") return true;
  if (cmd_name == "hello" && HelloHasAuthOption(cmd_tokens)) return true;
  return false;
}

inline ScriptCommandPolicyViolation GetScriptCommandPolicyViolation(bool script_no_writes, bool atomic_tx,
                                                                    uint64_t cmd_flags, std::string_view cmd_name,
                                                                    const std::vector<std::string> &cmd_tokens) {
  if (script_no_writes && (cmd_flags & kCmdReadOnly) == 0) {
    return ScriptCommandPolicyViolation::kReadOnlyScriptWrite;
  }
  if ((cmd_flags & kCmdNoScript) != 0 || (cmd_flags & kCmdExclusive) != 0) {
    return ScriptCommandPolicyViolation::kNoScriptOrExclusive;
  }
  if (IsScriptAuthStateChangeCommand(cmd_name, cmd_tokens)) {
    return ScriptCommandPolicyViolation::kScriptAuthStateChange;
  }
  if (atomic_tx && (cmd_name == "publish" || cmd_name == "mpublish")) {
    return ScriptCommandPolicyViolation::kAtomicScriptPublish;
  }
  if (atomic_tx && cmd_name == "applybatch") {
    return ScriptCommandPolicyViolation::kAtomicScriptApplyBatch;
  }
  return ScriptCommandPolicyViolation::kNone;
}

}  // namespace redis
