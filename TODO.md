# Namespace-Isolation

## Konstitution

### Grundregeln
- **Ziel:** 5-10 Tenants pro Instanz, je 10.000 writes = 50-100k Connections
- **Admin:** Nur default namespace (`__namespace`). Bei tenant-aware Functions abwägen: global oder eigener Tenant?
- **Worker:** Nicht für Tenants reserviert, eine Connection aktiv pro Worker (Eventloop)
- **Noisy-Neighbor:** Cross-Worker Aggregation darf andere Worker nicht blockieren, auch nicht bei seltenen befehlen, weil es gibt auch böse nachbarn.

### Auth & Namespace
- `ns_` bleibt leer bis AUTH (Sentinel-Wert, nicht mit Default initialisieren)
- Tracking erst nach Auth (`!GetNamespace().empty()`)
- `connection_counted_` verhindert Doppelzählung bei Re-AUTH/RESET

### Locking-Patterns
- **Per-NS Pattern:** `unordered_map<ns, unique_ptr<Struct>>` + `shared_mutex` (Lookup parallel)
- **Lock-Reihenfolge:** Outer Map Lock → Inner Struct Lock (nie umgekehrt)
- **Copy-then-Reply:** Empfänger unter Lock kopieren, Lock lösen, dann I/O
- **Nur `(Worker*, fd)` kopieren**, nie `Connection*` (Use-After-Free nach Lock-Release)

### Aggregation & Stats
- On-Demand bei INFO, 100ms cachen, nicht im Cron sampeln
- `GetNamespaceStats(ns)` statt `GetNamespaceStatsSnapshot()` (O(1) vs O(n))

### Blocking vs. Locking
- **Blocking:** Connection WARTET (BLPOP, XREAD BLOCK) - pausiert bis Event
- **Locking:** MULTI/EXEC - Connection AKTIV, Commands gequeued

### Konzepte (Akzeptiert)
- **Cluster vs. Namespaces:** Schließen sich gegenseitig aus
- **Replication:** Instanz-Ebene, repliziert ALLE Namespaces
- **Full-Sync Busy-Wait:** OK, Replica gibt sowieso "LOADING" zurück
- **SELECT:** No-Op bei Multi-Tenant, Tenants nutzen Key-Präfixe
- **WATCH Mutex:** Global akzeptabel (WATCH selten genutzt <1%)
- **Log-Entry IDs:** Globale Counter, ID-Lücken akzeptiert (wie Redis)

### Lua Scripts
- EVAL: `f_{ns}_{sha}` Storage-Key
- FUNCTION: `<ns>_<lib>`, LuaResetNamespace() für Cleanup
- `lua_pushvalue(lua, LUA_GLOBALSINDEX)` statt `lua_pushglobaltable` (Lua 5.1)

---

## Erledigt

### Commands
- [x] **PUB/SUB** - Vollständige Tenant-Isolation
- [x] **Shard PubSub** - Admin-only (`cmd_pubsub.cc:268-269`)
- [x] **MONITOR** - Per-NS Singleton, O(1) Lookup (`monitor_isolation_test.go`)
- [x] **CLIENT LIST/KILL** - Nur eigene Connections (`client_isolation_test.go`)
- [x] **SLOWLOG/PERFLOG** - Per-NS Filter (`slowlog_test.go`)
- [x] **DBSIZE, INFO keyspace** - Namespace-aware
- [x] **WATCH** - `MakeWatchedKey(ns, key)`
- [x] **FLUSHDB/FLUSHALL** - FLUSHDB eigener NS, FLUSHALL Admin-only
- [x] **SCRIPT FLUSH** - Namespace-Isolation (`script_isolation_test.go`)
- [x] **Blocking Commands** - Per-NS Mutex (`blocking_isolation_test.go`)
- [x] **DEBUG, FLUSHMEMTABLE, FLUSHBLOCKCACHE** - kCmdAdmin
- [x] **COMPACT** - Admin-only (`cmd_server.cc:1603`)
- [x] **STATS** - Admin-only (`cmd_server.cc:1610`)
- [x] **ROLE** - Admin-only (`cmd_server.cc:1597`)
- [x] **HELLO** - Kein Problem (Cluster/Master-Info erlaubt)

### INFO Stats
- [x] `used_percent` - `GetTotalSize(ns)`
- [x] `connected_clients`, `monitor_clients` - `GetClientCounts()`
- [x] `blocked_clients` - ConnContext.ns + `GetBlockedClientsCount()`
- [x] `total_commands_processed`, `total_net_input/output_bytes` - Per-Worker ns_stats_
- [x] `total_connections_received` - Per-NS nach Auth
- [x] `instantaneous_ops_per_sec` - On-Demand + 100ms Cache
- [x] `cmdstat_*` - Per-NS, `cmdstathist_*` Admin-only
- [x] `used_memory_lua` - Admin-only (Lua-VM shared)
- [x] **RocksDB, Replication, CPU, Persistence Sections** - Admin-only
- [x] **Sequence Number in Keyspace** - Admin-only

### Bugs & Security
- [x] **AUTH in MULTI** - `no-multi` Flag (`cmd_server.cc:1575`)
- [x] **FUNCTION FLUSH Segfault** - Async Reset (`function_namespace_test.go`)
- [x] **ns_locks_ Pointer-Invalidation** - C++ garantiert Stabilität
- [x] **Storage::Write() TOCTOU** - WorkExclusivityGuard schützt
- [x] **Namespace::List() Race** - Kopie statt Referenz (`namespace.h:41`)
- [x] **Connection Counting TOCTOU** - Atomic compare_exchange (`redis_connection.cc:169-177`)
- [x] **LogCollector Noisy-Neighbor** - Per-NS Pattern (`log_collector.cc`)
- [x] **Shard PubSub Destruktor** - SUnsubscribeAll() hinzugefügt (`redis_connection.cc:68`)

### Performance
- [x] `GetNamespace()` - `const std::string&` statt Kopie (`redis_connection.h:157`)
- [x] Doppelte `GetNamespace()`-Aufrufe eliminiert
- [x] `WakeupBlockingConns` - `std::move` statt Kopie (`server.cc:833`)
- [x] `PublishMessage` - `pair<Worker*, int>` statt ConnContext (`server.cc:449-472`)
- [x] `FeedMonitorConns` - Per-NS Singleton mit O(1) Lookup (`server.cc:433-475`)
- [x] **I/O unter Lock entfernen** - Copy-then-Reply Pattern überall

### Locking (Namespace-aware)
- [x] `WorkExclusivityGuard(ns)` - EXEC, SCRIPT (`redis_connection.cc:486-492`)
- [x] `ns_txn_states_` - Per-NS WriteBatch (`storage.cc:1025-1080`)
- [x] `pubsub_namespaces_mu_` - Per-NS PubSub (`server.h:480`)
- [x] `blocking_keys_ns_mu_` - Per-NS Blocking Keys (`server.h:488`)
- [x] `stream_consumers_ns_mu_` - Per-NS Stream Consumers (`server.h:492`)
- [x] `monitor_namespaces_mu_` - Per-NS Monitor (`server.h:496`)
- [x] `LogCollector` - Per-NS Deque + Mutex

---

## Offen

### Hoch - LockManager
- [ ] **LockManager ist GLOBAL** (`storage.cc:82`, `lock_manager.h`)
  - 65,536 Mutexes für ALLE Namespaces, Hash-Kollisionen zwischen Tenants
  - Betrifft: ALLE Write-Commands + Blocking Commands
  - Lösung: Per-Namespace LockManager oder Namespace-aware Hash

### Mittel - Result-Size-Limits
- [ ] **Config: `max_elements_in_response`** (0 = unlimited)
  - HGETALL/HKEYS/HVALS, SMEMBERS, LRANGE, ZRANGE, KEYS
  - Pattern: `if (limit > 0 && result.size() > limit) return Error;`

### Later
- [ ] **WATCH Mutex** - Per-NS Sharding (nur falls intensiv genutzt)

---

## Notizen

```
GEPRÜFT: Re-AUTH während Blocking ist kein Problem (Read-Callback nullptr)
GEPRÜFT: Re-AUTH während MULTI ist Bug → AUTH hat no-multi Flag
FAZIT: Transaktionen OK. LockManager ist NICHT OK - TODO erstellt. WATCH global aber akzeptabel.

TODO später:
- RESETSTAT existiert nicht in kvrocks (in Redis schon)
- Lua scripts in Transaktionen mappen
- LuaResetNamespace() iteriert über ALLE Globals - O(n_globals)
- Lua Scripts (EVAL) werden kCmdExclusive wenn lua_strict_key_accessing=false
  → Dann namespace-aware via WorkExclusivityGuard(ns)
```

### Ideen

**Response-Size pro Namespace begrenzen:**
Problem mit HGETALL/HSET mit Milliarden Elementen - am Ende iteriert man über große Datenstrukturen.
Wir sollten die Datenstrukturen selbst NICHT begrenzen (außer redis/kvrocks default max value size),
aber wir können pro Namespace generelle Limits wie `max_response_size` definieren um die Iterationen
zu begrenzen. So kann ein Tenant nicht unbegrenzt große Responses erzeugen die Memory/CPU fressen.
