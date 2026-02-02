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

### Work Guards (Command-Level Locking)
- **`kCmdExclusive`** → `WorkExclusivityGuard(ns)` = `unique_lock<shared_mutex>`
- **Normale Commands** → `WorkConcurrencyGuard(ns)` = `shared_lock<shared_mutex>`
- **`unique_lock` blockiert `shared_lock`!** Deshalb wartet non-exclusive auf exclusive
- **`kCmdNoLock`** → Kein Guard, für Commands die nur Atomics setzen (SCRIPT KILL)
- SCRIPT KILL muss `kCmdNoLock` haben, sonst Deadlock mit laufendem EVAL

### Worker-Blocking vs Lock-Blocking
- **Lock-Blocking:** Command wartet auf Lock, Worker kann andere Connections bedienen
- **Event-Loop-Blocking:** `lua_pcall` blockiert Worker komplett, keine anderen Connections
- Deshalb: SCRIPT KILL auf **anderem Worker** muss möglich sein (kCmdNoLock)
- Connections werden round-robin auf Workers verteilt (8 default)

### Accept-Dispatch Safety (Prod-Ready)
- **Worker-Snapshot (RCU):** Acceptors lesen nur Snapshot, kein direkter Zugriff auf `worker_threads_`
- **Resize-Safe:** `CONFIG SET workers` darf nie Acceptors/UAF triggern
- **Worker-State:** `Running/Stopping/Stopped`, Dispatch nur wenn `Running`
- **Pending-Queue bounded:** `acceptor-queue-limit` verhindert FD/RAM-Exhaustion
- **Stop/Resize:** `StopAccepting()` + Queue-Drain → keine geparkten FDs
- **Start-Reihenfolge:** Workers starten vor Acceptors

### Known Risks (WONTFIX / selten genutzt)
- **Connection-Migration:** TOCTOU/UAF-Risiko bei `MigrateConnection()` (nicht production-ready, wenig genutzt)
- **Resize während Last:** nutzt Migration → gleiches Risiko; daher nicht als stabiler Prod-Feature bewerben

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
- **lua_sethook:** `LUA_MASKCOUNT` alle 100k Instruktionen → prüft Timeout/Kill
- Hook feuert nur zwischen Lua-Instruktionen, nicht während `redis.call()`
- `luaL_error()` im Hook → longjmp, wird von `lua_pcall` gefangen
- **Timeout/Kill Limitierungen:** Hook greift nur in Lua-Bytecode, nicht während C++ in `redis.call()` läuft
- **SCRIPT KILL:** setzt nur Worker-Flag; funktioniert nur wenn Kill-Command auf anderem Worker läuft
- **SO_REUSEPORT Risiko:** beide Connections koennen auf demselben Worker landen → SCRIPT KILL haengt (Go-Test: i/o timeout)

### Transaktionen & Atomizität

**MULTI/EXEC:**
- Pro Namespace nur 1 EXEC gleichzeitig (`WorkExclusivityGuard(ns)`)
- Andere Tenants können parallel EXEC ausführen
- Worker blockiert während EXEC (kurz, nur Ausführungsdauer)
- Commands werden in `ns_txn_states_[ns].batch` (WriteBatch) gesammelt
- **Kein Rollback bei Fehlern** während EXEC (Redis-Design)

**EVAL/FCALL:**
- Default: `lua_strict_key_accessing=false` → EVAL ist `kCmdExclusive`
- Pro Namespace nur 1 EVAL gleichzeitig
- Worker blockiert während EVAL (synchrone Ausführung)
- **Standalone EVAL:** Kein WriteBatch, jeder `redis.call()` committed sofort
- **EVAL in MULTI:** Nutzt MULTI's WriteBatch, ist transaktional

**Verschachtelung:**
- EVAL in MULTI: ✅ Erlaubt, nutzt WriteBatch
- MULTI in EVAL: ❌ Nicht erlaubt (EXEC hat `exclusive` Flag)
- EVAL in EVAL: ❌ Nicht erlaubt (`no-script` Flag)
- FCALL in EVAL: ❌ Nicht erlaubt (`no-script` Flag)

**Kein Rollback (Redis-Verhalten):**
- Fehler während EXEC führen NICHT zu Rollback
- Bereits ausgeführte Commands + erfolgreiche Teile bleiben committed
- Nur DISCARD (vor EXEC) verwirft alles

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
- [x] **LockManager Hash-Kollisionen** (`storage.cc:82`)
  - Problem: 65,536 Buckets → 86% Kollision bei 512 Workers
  - Fix: `lock_mgr_(20)` = 1M Buckets → ~12% Kollision
  - Hinweis: Hash war bereits namespace-aware (ns_key), nur zu wenige Buckets

### Hoch - Concurrency
- [ ] **GetConnections() Data-Race** → Resize nutzt `Worker::GetConnections()` ohne Lock
- [ ] **Worker Destruktor UB** → iteriert `conns_` und löscht gleichzeitig (Shutdown/Resize)
- [ ] **Cross-Thread Conn Reads** → `GetClientsStr/GetClientCounts/KillClient` lesen `Connection`-Felder ohne Atomics/Lock

### Mittel - O(n) Noisy-Neighbor Commands (Audit 2026-02-01)

**Problem:** Diese Commands blockieren einen Worker während der gesamten Iteration.
Ein böser Tenant kann mit großen Datenstrukturen andere Tenants verlangsamen.

**Lösung:** Config `max_elements_in_response` (0 = unlimited, default)
**Pattern:** `if (limit > 0 && result.size() > limit) return Error;`

- [ ] **Hash O(n):** HGETALL, HKEYS, HVALS (`cmd_hash.cc`)
- [ ] **Set O(n):** SMEMBERS (`cmd_set.cc`)
- [ ] **Set O(n*m):** SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE (`cmd_set.cc`)
- [ ] **ZSet O(n*k):** ZUNION, ZINTER, ZDIFF, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE (`cmd_zset.cc`)
- [ ] **List O(n):** LRANGE, LINSERT, LREM (`cmd_list.cc`)
- [ ] **ZSet O(n):** ZRANGE, ZRANGEBYLEX, ZRANGEBYSCORE (`cmd_zset.cc`)
- [ ] **Keys O(n):** KEYS (`cmd_server.cc`)

**Bereits geschützt:**
- [x] SORT - Hat `SORT_LENGTH_LIMIT = 512` (`redis_db.h:39`)
- [x] XRANGE/XREVRANGE - Hat COUNT Option
- [x] GEORADIUS/GEOSEARCH - Hat COUNT Option
- [x] EVAL/FCALL - Namespace-aware via `WorkExclusivityGuard(ns)` (kein Cross-Tenant-Problem)

### Later
- [ ] **WATCH Mutex** - Per-NS Sharding (nur falls intensiv genutzt)
- [ ] **Lua Timeout** - `lua-time-limit` Config + `SCRIPT KILL` Command (namespace-aware)
- [x] **Accept-Dispatch Hardening (Race/FD-Leak)**
  - Worker-Snapshot (RCU) statt direktem `worker_threads_` Zugriff
  - StopAccepting + Queue-Drain (keine geparkten FDs)
  - `acceptor-queue-limit` (bounded pending queue)
  - Start-Reihenfolge: Workers vor Acceptors
- [x] **Lua Timeout/Kill - Accept-Dispatch Architecture (Phase 1 DONE)**
  - Ziel: SCRIPT KILL immer erreichbar, auch wenn Worker in lua_pcall blockiert
  - Konzept: 1..N Accept-Threads nehmen Verbindungen an und verteilen FD an Worker
  - Worker-Auswahl: round-robin, aber Worker mit `lua_script_running_` ueberspringen (fallback auf any)
  - **Phase 1 - Worker-Seite (DONE):**
    - [x] `PendingConnection` struct in worker.h
    - [x] Dispatch-Queue + eventfd in Worker
    - [x] `DispatchConnection()` - thread-safe, wakeup via eventfd
    - [x] `onDispatchEvent()` - batch processing
    - [x] `createConnectionFromDispatch()` - Connection aus dispatched FD
  - **Phase 1 - Acceptor-Seite (DONE):**
    - [x] `StartAcceptors()`, `AcceptorLoop()`, `SelectWorker()` in server.cc
    - [x] TCP-Listen aus Worker-Konstruktor entfernen (nur Unix-Socket Worker0)
    - [x] `acceptor-threads` Config (1-16, default 1)
    - [x] Graceful Shutdown: Threads erst joinen, dann FDs schließen
    - [x] systemd socket_fd: dup() für sauberes Shutdown
  - **Phase 2 (Later):** `IsLuaRunning()` Check, SCRIPT KILL, lua_sethook
  - **Phase 3 (Later):** SNI -> Tenant Mapping
- [ ] **Per-NS Heavy-Command Budget (verhindert Noisy-Neighbor durch O(n)/Lua)**
  - Idee: neuer Flag `kCmdHeavy` (oder reuse `kCmdSlow`) + Config `max-heavy-per-namespace`
  - Check in `Connection::ExecuteCommands` vor Ausfuehrung:
    - wenn heavy und counter(ns) >= limit → `BUSY/TRYAGAIN` (oder custom error)
    - sonst counter++ und per ScopeExit counter--
  - Heavy-Kandidaten: O(n)/O(n*m) Commands aus TODO (HGETALL, SMEMBERS, LRANGE, KEYS, Z*RANGE, SINTER/UNION/DIFF, Z*UNION/INTER/DIFF), plus EVAL/FCALL (Lua)
  - Multi/EXEC: Budget beim EXEC verbrauchen (nicht beim Queue)
  - Ziel: ein Tenant kann nicht alle Worker mit langen Commands blockieren
- [ ] **Lua Key-Level Locking** - Wie DragonflyDB: Nur deklarierte Keys locken statt ganzen Namespacep
- [ ] **EVAL_TX / FCALL_TX** - Transaktionale Lua Scripts mit Auto-Rollback
  - Neue Commands: `EVAL_TX`, `EVALSHA_TX`, `FCALL_TX` (analog zu `_RO` Suffix)
  - Standalone: Eigenes `BeginTxn`/`CommitTxn`, bei Fehler `DiscardTxn`
  - In MULTI: Nutzt MULTI's WriteBatch (kein eigenes Rollback)
  - Redis-kompatibel: EVAL/FCALL bleiben unverändert (kein Rollback)
  - Aufwand: ~20 Zeilen in `scripting.cc`, ~20 Zeilen in `cmd_function.cc`
  - Dateien: `cmd_script.cc`, `scripting.cc`, `cmd_function.cc`, `storage.cc`

---

## Notizen

```
GEPRÜFT: Re-AUTH während Blocking ist kein Problem (Read-Callback nullptr)
GEPRÜFT: Re-AUTH während MULTI ist Bug → AUTH hat no-multi Flag
FAZIT: Transaktionen OK. LockManager OK (1M Buckets). WATCH global aber akzeptabel.

TODO später:
- RESETSTAT existiert nicht in kvrocks (in Redis schon)
- LuaResetNamespace() iteriert über ALLE Globals - O(n_globals)
```

### Ideen

**Response-Size pro Namespace begrenzen:**
Problem mit HGETALL/HSET mit Milliarden Elementen - am Ende iteriert man über große Datenstrukturen.
Wir sollten die Datenstrukturen selbst NICHT begrenzen (außer redis/kvrocks default max value size),
aber wir können pro Namespace generelle Limits wie `max_response_size` definieren um die Iterationen
zu begrenzen. So kann ein Tenant nicht unbegrenzt große Responses erzeugen die Memory/CPU fressen.
