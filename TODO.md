# Namespace-Isolation

## Konstitution

### Grundregeln
- **Ziel:** 5-10 Tenants pro Instanz, je 10.000 writes = 50-100k Connections
- **Admin:** Nur default namespace (`__namespace`). Bei tenant-aware Functions abwägen: global oder eigener Tenant?
- **Worker:** Nicht für Tenants reserviert; pro Callback ist genau eine Connection aktiv, mit Yield rotiert der Worker fair zwischen Connections
- **Noisy-Neighbor:** Cross-Worker Aggregation darf andere Worker nicht blockieren, auch nicht bei seltenen befehlen, weil es gibt auch böse nachbarn.

### Auth & Namespace
- `ns_` bleibt leer bis AUTH (Sentinel-Wert, nicht mit Default initialisieren)
- Tracking erst nach Auth (`!GetNamespace().empty()`)
- `connection_counted_` verhindert Doppelzählung bei Re-AUTH/RESET

### Locking-Patterns
- **Per-NS Pattern:** `unordered_map<ns, unique_ptr<Struct>>` + `shared_mutex` (Lookup parallel)
- **Lock-Reihenfolge:** Outer Map Lock → Inner Struct Lock (nie umgekehrt)
- **Copy-then-Reply:** Empfänger unter Lock kopieren, Lock lösen, dann I/O
- **Nur stabile Handles `(Worker*, fd, conn_id)` kopieren**, nie `Connection*` (UAF/FD-Reuse nach Lock-Release)

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

### Event-Loop Slice & Batching
- `read-event-max-commands`, `read-event-max-time-us`, `read-event-max-heavy` sind Fairness/Latency-Knobs pro Connection-Slice
- Zu kleine Slices senken Write-Throughput (mehr Resume/Callback-Overhead, mehr timer-getriebene Commits)
- Zu große Slices verschlechtern Fairness/p99 bei vielen aktiven Connections auf einem Worker
- Batch-State ist **worker-lokal + namespace-gebunden**: Writes aus mehreren Connections im selben Namespace koennen im selben Batch landen
- Batch-Commit triggert durch Barrier oder `batching-max-ops` / `batching-max-bytes` / `batching-max-delay-us`
- Praxisregel: `read-event-max-commands` nicht deutlich kleiner als `batching-max-ops` setzen

### Fair Scheduler Defaults
- Default ist strikt fair: `sni-overdraft-percent = 0` (kein Burst/Overdraft)
- Burst-Verhalten nur explizit per Config aktivieren

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
- `EVAL_TX` / `FCALL_TX`: DB-Writes sind atomar (BeginTxn/CommitTxn, Rollback bei Runtime-Fehler)
- Konstitutions-Regel: In atomic scripts nur Commands erlauben, deren Side-Effects im NS-Txn liegen; alles andere explizit sperren
- Hard-Block-Kandidaten in Script-Context (fuer Atomik/Kontext-Sicherheit): `PUBLISH/MPUBLISH`, `APPLYBATCH`, `AUTH`, `HELLO AUTH`

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

### Concurrency
- [x] **LockManager Hash-Kollisionen** - `lock_mgr_(20)` = 1M Buckets (`storage.cc:82`)
- [x] **GetConnections() Data-Race** - `GetConnectionsSnapshot()` unter `conns_mu_`
- [x] **Worker Destruktor UB** - Zwei-Phasen-Cleanup
- [x] **Cross-Thread Conn Reads** - thread-sichere `Connection::ClientInfo` Snapshots
- [x] **PubSub Subscribe-State Race** - atomare Subscribe-Counter
- [x] **FD-Reuse Misrouting** - async Reply/Wakeup via `(fd + conn_id)` validiert
- [x] **DBScan Map Race** - reads unter `db_job_mu_`
- [x] **BGSAVE/Compact Flags Race** - INFO/Persistence reads unter `db_job_mu_`
- [x] **TLS Repl SSL Race** - Feed-Thread pausiert `EV_READ` unter `bufferevent`-Lock

### FairScheduler (SNI)
- [x] **UAF Risk** - `CleanupInactiveSNIs()` / `SelectWorker()` Race behoben
- [x] **Clamp-UB** - `active_snis > total_workers` korrigiert
- [x] **Counter-Leak** - `active_connections`/`active_sni_count_` Leak behoben
- [x] **Empty-SNI Leak** - `OnConnectionClosed()` dekrementiert jetzt korrekt
- [x] **Rebalance Drift** - Lazy `RefreshPreferredWorkers()` bei Select
- [x] **Per-SNI Mutex Contention** - lockfreier immutable Snapshot
- [x] **TLS-SNI Flaky Erkennung** - bounded Retry in `ExtractSNIFromClientHello()`
- [x] **Throughput Rework** - Concentration-Gating entfernt, Fair Pool strikt über Share+Overdraft
- [x] **Config-Bereinigung** - nur noch `sni-max-workers-percent` + `sni-overdraft-percent`
- [x] **Observability** - INFO `Scheduler` zeigt pro-SNI Stats

### PubSub/RESP
- [x] **Subscribed-Mode Guard** - RESP2-kompatibel implementiert
- [x] **RESET für PubSub** - ruft `SUnsubscribeAll()` auf
- [x] **Client-Type** - berücksichtigt `SSUBSCRIBE`
- [x] **TimeSeries RESP3** - RESP2-Clients korrekt bedient
- [x] **COMMAND INFO Null-Typ** - korrigiert

### Accept-Dispatch Architecture
- [x] **Phase 1** - Worker-Seite + Acceptor-Seite vollständig
- [x] **Phase 2** - Lua-Aware Routing (`SelectWorker()` überspringt blockierte Worker)
- [x] **Phase 3** - SNI-basiertes Fair Scheduling (FairScheduler Klasse)
- [x] **Accept-Dispatch Hardening** - Worker-Snapshot (RCU), StopAccepting, bounded queue

### Bereits geschützt (O(n) Commands)
- [x] SORT - `SORT_LENGTH_LIMIT = 512` (`redis_db.h:39`)
- [x] XRANGE/XREVRANGE - COUNT Option
- [x] GEORADIUS/GEOSEARCH - COUNT Option
- [x] EVAL/FCALL - `WorkExclusivityGuard(ns)`

---

## Offen

### Hoch
- [x] **CONFIG SET Race** - Config-Felder ohne globalen Lock, Background-Threads lesen parallel
- [x] **Lock-freier Accept-Hot-Path** - keine per-accept Mutex- oder Rebuild-Kosten
- [x] **Batching: Config & Defaults** (enabled/max_ops/max_bytes/max_delay_us)
- [x] **Batching: Worker Batch-State + Timer** (ops/bytes/timer + WriteBatchWithIndex)
- [x] **Batching: Reply-Deferral + Flush nach Commit**
- [x] **Batching: Storage::Write merge in ns-batch** (reuse `BeginTxn/CommitTxn`, kein db->Write im hot-path)
- [x] **Batching: Barriers** (MULTI/EXEC/WATCH, EVAL/FCALL, Blocking, CONFIG/DEBUG/CLUSTER)
- [x] **Batching: Hardening-Barriers** (`APPLYBATCH`, `AUTH`, `HELLO AUTH`, `RESET`)
- [x] **Batching: WATCH dirty erst nach erfolgreichem Commit** (batched write-path defered, apply nur bei erfolgreichem Commit)
- [LATER] **[LATER] Batching: Reply-Deferral auf Batch-Teilnehmer scopen** (aktuell nicht kritisch bei Betriebsannahme `1 Worker = 1 Namespace/Tenant`; relevant als Hardening fuer Multi-NS pro Worker)
- [LATER] **Batching: Commit-Fehler semantisch präzisieren** (nicht nur generisches `ERR batch commit failed` für alle deferred Replies)
- [LATER] **Batching: Tests erweitern** (mehr Barrier/Fehlerfälle)
- [x] **Batching: Annahme dokumentieren** (kein 1NS=1Worker nötig; aktiver Batch hält `WorkExclusivityGuard(ns)`)
- [x] **WATCH dirty in EXEC/MULTI commit-sensitiv machen** (EXEC deferred WATCH-Updates, apply nur nach erfolgreichem Commit; inkl. manueller WATCH-Updates aus List-Pfaden)

### Mittel - O(n) Noisy-Neighbor Commands
Config `max_elements_in_response` (0 = unlimited) mit Pattern `if (limit > 0 && result.size() > limit) return Error;`
- [ ] Hash: HGETALL, HKEYS, HVALS (`cmd_hash.cc`)
- [ ] Set: SMEMBERS (`cmd_set.cc`)
- [ ] Set O(n*m): SINTER, SUNION, SDIFF + Store-Varianten (`cmd_set.cc`)
- [ ] ZSet O(n*k): ZUNION, ZINTER, ZDIFF + Store-Varianten (`cmd_zset.cc`)
- [ ] List: LRANGE, LINSERT, LREM (`cmd_list.cc`)
- [ ] ZSet: ZRANGE, ZRANGEBYLEX, ZRANGEBYSCORE (`cmd_zset.cc`)
- [ ] Keys: KEYS (`cmd_server.cc`)

### Niedrig - Concurrency Follow-up
- [ ] **GetClientInfo Lock-Zeit** - `client_mu_` nicht während Buffer-Reads halten
- [ ] **KillClient Lock-Reacquire** - Kandidaten pro Worker gruppieren
- [ ] **GetNamespace API** - cross-thread-safe Snapshot/Kopie
- [x] **OnRead Yield/Quota** - Long Pipelines blockieren Worker: pro Tick max N Commands oder Zeitbudget, dann via event reschedulen; Re-Entry-Guard (`is_running_`), Backpressure (EV_READ off/on), kein Yield innerhalb EXEC

### Later
- [ ] **CONFIG RELOAD** - Config-Datei Hot-Reload ohne Restart (`Config::Load()` über neuen Subcommand; Validierung für nicht-änderbare Felder wie `bind`, `port`, `dir`)
- [ ] **WATCH Mutex** - Per-NS Sharding (nur falls intensiv genutzt)
- [ ] **Go-Tests für Fair Scheduling**
- [ ] **Per-NS Heavy-Command Budget** - `kCmdHeavy` Flag + `max-heavy-per-namespace` Config
- [ ] **Lua Key-Level Locking** - Nur deklarierte Keys locken (wie DragonflyDB)
- [x] **EVAL_TX / FCALL_TX** - Transaktionale Lua Scripts mit Auto-Rollback
- [x] **EVAL_TX/FCALL_TX Hardening:** `APPLYBATCH` aus Script-Context sperren (umgeht DB-Txn, kann Rollback aushebeln)

### Later - Reuse/Refactor
- [x] **Command-Policy zentralisieren** - eine gemeinsame Policy-Matrix fuer Barrier/Script/Subscribed/MULTI statt Checks an mehreren Stellen
- [x] **Gemeinsamer Script-Runner** - `EVAL/FCALL/EVAL_TX/FCALL_TX` ueber denselben Guard+Txn-Pfad fuehren (weniger Drift-Risiko)
- [ ] **[LATER] Einheitlicher Batch-Lifecycle Helper** - Begin/Defer/Commit/Fail-Reply in eine wiederverwendbare Komponente ziehen
- [x] **WATCH-dirty Pfad vereinheitlichen** - batched und EXEC nutzen denselben commit-sensitiven Apply-Mechanismus (`DeferredWatchKeysUpdate` + `Server::ApplyDeferredWatchKeysUpdate`)
- [x] **Script-Context Hardening:** `AUTH` + `HELLO AUTH` in Scripts sperren (Namespace/Auth-Wechsel mitten im Script)
- [x] **Tests:** `EVAL_TX`/`FCALL_TX` mit `lua-time-limit` und `SCRIPT KILL` auf Rollback verifizieren

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

**Worker-Fairness bei langen Pipelines:**
Implementiert: Yield/Quota in `Connection::OnRead()` (max N Commands oder Zeitbudget), Reschedule via Event; Backpressure (EV_READ off/on), kein Yield innerhalb EXEC.

**Response-Size pro Namespace begrenzen:**
Problem mit HGETALL/HSET mit Milliarden Elementen - am Ende iteriert man über große Datenstrukturen.
Wir sollten die Datenstrukturen selbst NICHT begrenzen (außer redis/kvrocks default max value size),
aber wir können pro Namespace generelle Limits wie `max_response_size` definieren um die Iterationen
zu begrenzen. So kann ein Tenant nicht unbegrenzt große Responses erzeugen die Memory/CPU fressen.

**Namespace-Batching (Valkey-Style, sync=true):**
Annahme: mehrere Worker sind ok; aktiver Batch hält **persistenten** `WorkExclusivityGuard(ns)` bis Commit.
Ziel: Group-Commit mit `sync=true`, Replies erst nach fsync, Semantik pro Namespace korrekt.
Plan:
- Config: `batching.enabled`, `batching.max_ops`, `batching.max_bytes`, `batching.max_delay_us`
- Worker hält Batch-State (ops/bytes/timer + `active_ns` + persistenter NS-Lock), Timer triggert Commit
- Reply-Deferral: Write-Replies sammeln, nach Commit flushen
- Reads bei aktivem Batch: aus Overlay; Antworten bleiben bei aktivem Batch deferred
- Storage::Write mergen: via bestehendem `BeginTxn/CommitTxn`-Pfad, kein db->Write im hot-path
- Barriers: vor MULTI/EXEC/WATCH, EVAL/FCALL, Blocking, CONFIG/DEBUG/CLUSTER immer Batch flush
- Tests: Basis vorhanden (`unit/connection`), weitere Barrier/Fehlerfälle offen

!! wichtig, ich bin mir nicht sicher aber akutell sollen reads nicht gebatched werden sondern sofort zurcgegbeen. hier müssen wir
sicherstellen das nichts zurckgegebenwird was noch nicht garantiert persisitiert ist (in rocksdb). !! 
