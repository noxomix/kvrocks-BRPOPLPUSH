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

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace redis {

struct DeferredWatchKeysUpdate {
  bool mark_all_keys = false;
  std::vector<std::string> keys;

  bool Empty() const { return !mark_all_keys && keys.empty(); }

  void Reset() {
    mark_all_keys = false;
    keys.clear();
  }

  void Enqueue(bool mark_all, std::vector<std::string> updates) {
    if (mark_all) {
      mark_all_keys = true;
      keys.clear();
      return;
    }

    if (mark_all_keys || updates.empty()) return;
    auto old_size = keys.size();
    keys.resize(old_size + updates.size());
    std::move(updates.begin(), updates.end(), keys.begin() + static_cast<std::ptrdiff_t>(old_size));
  }
};

}  // namespace redis
