# Namespace-Isolation

Konstitution: {
Bitte auch architektur.md konsolidieren je nach szenario. Auch wenn nicht alles 100% aktuell ist, weil wir Schritt für Schritt ja namespace
awareness implementieren. Dennoch ist das grudnverösnis von workern und Parallelität wichtig. Aber immer logisch denken.

Das ziel ist KVrocks ist tenant aware, pro instanz etwa 5-10 tenants. Jeder Tenant soll 10.000 writes gleichezeitig können also im worstcase 50-100.000 connecitons parallel.
Worker sind naütlrich nicht für bestimmte tenant reserviert. Es ist wichtig dies zu verstehen, weil so manche arten von locks oder datenstrutkturen entpsechend auf diesen demand
angepasst werden müssen das wir später kein bottleneck haben.

In KVrocks ist ein ADMIN automatisch nur der defualt namespace (__namespace/_namespace). Bei tenant aware functions ist es wichtig abzuwägen,
ob im ADMIN fall global aggiert werden soll zB Flush scripts alle namespace scripts flusht oder nur im eignene tenant. Das haben wir bislang nicht zuverlässig
gemacht aber es ist nicht an allen stellen schlim, lediglich sollte es bedacht und überlegt werden.

Auf einem Worker können viele Tenant connections laufen. Aber nur eine davon ist quasi "active" also quasi eine eventloop (soweit ich das richtig verstand hab).
Wenn wir irgendwelche commands haben die daten von mehrere workern direkt aquirieren müssen zB statistiken über einen tenant (namespace) dann dürfen andere worker
nicht so lange blockieren, sonst könnte ein namespace andere namespaces absichlich verlangsamen (noisy oder evil neighbor).

LEARNING - Sentinel-Werte bei Auth-Flows nicht ändern:
State-Variablen die im Auth-Flow als "nicht authentifiziert" Marker dienen (z.B. leerer String, nullptr, 0)
dürfen NICHT mit Default-Werten initialisiert werden. Der Auth-Flow nutzt diese Sentinel-Werte um zu
entscheiden ob Authentifizierung nötig ist. Konkret: `ns_` muss leer bleiben bis AUTH erfolgt ist.

LEARNING - Per-Tenant Tracking nur nach Auth:
Wenn per-Namespace/Tenant Stats getrackt werden sollen, immer prüfen ob der Tenant bekannt ist
(z.B. `!GetNamespace().empty()`). Traffic VOR Authentifizierung kann keinem Tenant zugeordnet werden
und wird nur global gezählt. Das ist korrekt und kein Bug.

LEARNING - Aggregation optimieren:
Bei Cross-Worker Aggregation (z.B. INFO stats) nicht alle Daten von allen Workern holen und dann filtern.
Stattdessen: Gezielt nur die benötigten Daten anfragen
(z.B. `GetNamespaceStats(ns)` statt `GetNamespaceStatsSnapshot()` - O(1) vs O(n_namespaces)).

LEARNING - Connection-Zählung nach Auth:
`total_connections_received` wird pro Namespace gezählt, aber erst NACH erfolgreicher
Authentifizierung (in SetNamespace()). Grund: Vor AUTH ist der Namespace unbekannt.
Flag `connection_counted_` in Connection verhindert doppeltes Zählen bei Re-AUTH oder
RESET→AUTH. Jede Connection zählt genau einmal für den ersten authentifizierten Namespace.

LEARNING - On-Demand Rate mit Per-NS Mutex:
Für per-Namespace Rate-Metriken (instantaneous_ops/input_kbps/output_kbps):
1. NICHT im Cron sampeln → skaliert nicht mit O(n_namespaces)
2. On-Demand bei INFO berechnen, 100ms cachen
3. Pattern gemäß Konstitution: `unordered_map<ns, unique_ptr<NamespaceRateData>>`
   - shared_mutex für Map-Zugriff (Cache-Hits parallel)
   - Per-NS mutex für Rate-Berechnung (Tenants blockieren sich NICHT)
4. Fast Path: shared_lock + atomic read (keine per-NS Lock)
5. Slow Path: shared_lock + per-NS Lock + Aggregation

KONZEPT - Blocking vs. Locking:
- **Blocking** (`blocked_clients`): Connection WARTET auf externes Event (BLPOP wartet auf Daten, XREAD BLOCK, WAIT).
  Connection ist pausiert, verarbeitet keine Commands bis Event eintritt oder Timeout.
- **Locking** (MULTI/EXEC): Connection ist AKTIV, Commands werden gequeued. Bei EXEC kurzer Lock für Atomizität.
  Connection antwortet sofort auf jeden Command ("QUEUED"), ist nie "blocked".
`blocked_clients` zählt nur schlafende Connections, nicht Transaktionen.

LEARNING - Blocking-Structs brauchen Namespace:
Wenn Blocking-Datenstrukturen (ConnContext, StreamConsumer, WaitContext) für tenant-aware Zählung genutzt werden,
muss der Namespace zum Zeitpunkt des Blockings gespeichert werden. Blocking-Commands erfordern Auth,
daher ist GetNamespace() immer verfügbar. Zählung erfolgt on-demand bei INFO, nicht im Hot-Path.

LEARNING - Per-Namespace Sharding bei globalen Mutexen:
Globale Mutexe die von allen Tenants genutzt werden (blocking_keys_mu_, blocked_stream_consumers_mu_)
müssen durch per-Namespace Strukturen ersetzt werden: `unordered_map<ns, unique_ptr<NamespaceStruct>>`
mit shared_mutex für Lookup + mutex pro Namespace. Pattern: GetOrCreate() bei Block, Cleanup bei NS-Delete.

PATTERN - Nested Map für Per-Namespace Datenstrukturen:
Struktur: `unordered_map<ns, unique_ptr<NamespaceStruct>>` + `shared_mutex` für Map-Zugriff.
shared_lock für Lookup (parallel), unique_lock nur bei Create/Delete.
Eigener mutex pro Namespace-Struct → Tenants blockieren sich nicht gegenseitig.

LEARNING - Lock-Reihenfolge bei Nested Structures:
IMMER: 1. Outer Map Lock → 2. Inner Struct Lock. Niemals umgekehrt.
Subscribe/Create braucht unique_lock auf Outer, Query nur shared_lock.
Deadlock unmöglich wenn Reihenfolge konsistent eingehalten.

LEARNING - Copy-then-Reply bei Fan-Out:
Bei Broadcast (PUBLISH, MONITOR): Empfänger-Liste unter Lock kopieren,
Lock releasen, DANN Reply senden. Verhindert Lock-Hold während I/O.
WICHTIG: Nur `(Worker*, fd)` kopieren, NICHT `Connection*` - Connection kann nach
Lock-Release gelöscht werden (Use-After-Free). `Worker::Reply(fd, msg)` und
`Worker::EnableWriteEvent(fd)` validieren fd intern → sicher.

KONZEPT - Per-NS Singleton statt Per-Worker:
Wenn Daten per-Namespace gruppiert sind (MONITOR, PubSub), zentrale Struktur nutzen
statt auf alle Worker zu verteilen. Vorteile:
- O(1) statt O(n_workers) Lock-Akquisitionen pro Operation
- Namespace-Isolation by-design (kein nachträglicher Filter nötig)
- Cleanup bei Namespace-Delete trivial (eine Map löschen)
Pattern: `unordered_map<ns, unique_ptr<NamespaceStruct>>` mit shared_mutex.

LEARNING - CanMigrate für spezielle Connection-Typen:
Connections mit Worker-spezifischer Registrierung (MONITOR, PubSub, Blocking)
dürfen NICHT migriert werden. `CanMigrate()` muss alle relevanten Flags prüfen.
Nach Migration wäre die Registrierung beim alten Worker, Reply geht ins Leere.

LEARNING - Idempotenz bei Registrierung:
Commands die Connection-State ändern (MONITOR, SUBSCRIBE) können mehrfach
aufgerufen werden. Prüfe Flag BEVOR Registrierung: `if (kMonitor) return;`
Sonst: Doppelte Einträge, Counter-Drift, Memory-Leak.

LEARNING - GetOrCreate Race Condition:
FALSCH: `GetOrCreate()` → return raw pointer → caller nutzt pointer
RICHTIG: Lock halten während gesamter Operation (inline GetOrCreate)
Grund: Zwischen return und Nutzung könnte Cleanup den Pointer invalidieren.

LEARNING - Replication benötigt Namespace im Key:
RocksDB-Key: `ComposeNamespaceKey(ns, data)` → Format: `<1-byte ns_len><namespace><data>`
Replica extrahiert mit `ExtractNamespaceKey()` → korrekte Tenant-Zuordnung.
User-sichtbarer Name bleibt unverändert (kein Prefix sichtbar).

LEARNING - Destruktor muss Subscriptions/Registrierungen aufräumen:
Connection::~Connection() MUSS alle Registrierungen entfernen (UnsubscribeAll etc.)
Sonst: Stale Einträge in Maps → Zugriff auf geschlossene FDs/invalidierte Objekte.

KONZEPT - Admin-Verhalten differenzieren:
Maintenance-Commands (COMPACT, FLUSHALL, DEBUG): Admin = global
Daten-Commands (PUBSUB, normale Keys): Admin = normaler Tenant (default namespace)
Konsistenz wichtiger als Convenience.

KONZEPT - Cluster vs. Namespaces:
Cluster-Mode deaktiviert Namespaces by Design. Slot-Migration (slot_import.cc) arbeitet
nur mit kDefaultNamespace - kein Bug, da beide Features sich gegenseitig ausschließen.

KONZEPT - Replication ist Instanz-Ebene:
Replication (SLAVEOF/REPLICAOF) repliziert ALLE Daten der Instanz inkl. aller Namespaces.
Full-Sync kopiert alles - korrekt by Design. Tenant-Isolation auf Infra-Ebene = separate Instanzen.

KONZEPT - Full-Sync Busy-Wait ist OK:
`works_concurrency_rw_lock_` busy-wait (1ms polling) in `PrepareRestoreDB()` betrifft nur REPLICA-Seite.
Replica gibt während Full-Sync sowieso "LOADING" zurück → kein Noisy-Neighbor für andere Tenants.
Full-Sync ist selten (Initial-Sync, Connection-Loss). Optimierung (Condition Variable) nicht nötig.

KONZEPT - EVAL Scripts vs. FUNCTION Isolation:
EVAL-Scripts: Per-NS isoliert mit `f_{ns}_{sha}` Storage-Key und Lua-Global.
FUNCTION: Vollständig per-NS isoliert (`<ns>_<lib>`), LuaResetNamespace() für Cleanup.
SCRIPT FLUSH/EXISTS/LOAD: Namespace-aware, Admin = default namespace (normaler Tenant).
LuaResetNamespace() räumt BEIDE auf (FUNCTIONs UND EVAL-Scripts).

LEARNING - LuaResetNamespace Strukturierung:
Wenn mehrere Lua-Objekt-Typen aufgeräumt werden (FUNCTIONs, EVAL-Scripts), NIEMALS early-return
nach optionaler Prüfung. Stattdessen: Optionale Cleanups in if-Block wrappen, Pflicht-Cleanups danach.
Bug-Pattern: `if (!table_exists) return;` überspringt nachfolgende Cleanup-Sektionen.

LEARNING - Immediate Reset bei SCRIPT FLUSH:
Nach SCRIPT FLUSH muss `LuaResetNamespace()` SOFORT auf dem aktuellen Worker aufgerufen werden
(wie bei FUNCTION FLUSH), nicht nur lazy-mark. Sonst kann der gleiche Worker das Script noch ausführen.

LEARNING - LuaJIT/Lua 5.1 Kompatibilität:
`lua_pushglobaltable(lua)` existiert NICHT in Lua 5.1 (LuaJIT). Stattdessen:
`lua_pushvalue(lua, LUA_GLOBALSINDEX)` für Iteration über globale Variablen.

LEARNING - Replication Tests mit Auth:
Wenn Master `requirepass` hat, braucht Slave auch `masterauth` in der Config.
Sonst schlägt WaitForSync fehl weil Slave sich nicht authentifizieren kann.

LEARNING - Blocking-State ist server-lokal:
Blocking-State (welche Connections warten worauf) ist transient und nicht repliziert.
Nur Daten-Operationen (LPUSH, XADD) werden repliziert. Jeder Server (Primary/Replica)
verwaltet eigene Blocking-Clients. → Per-Namespace Blocking-Refactoring benötigt keine
Replication-Änderungen.

LEARNING - AUTH und Namespace-Wechsel:
AUTH während MULTI ist ein Bug - Commands nach AUTH in der Transaction laufen im neuen
Namespace. Fix: AUTH braucht `no-multi` Flag.
AUTH während Blocking (BLPOP etc.) ist KEIN Problem - Read-Callback ist nullptr während
Blocking, neue Commands werden erst NACH dem Blocking verarbeitet.

KONZEPT - SELECT No-Op bei Multi-Tenant:
Redis-SELECT ist für Single-Tenant DB-Isolation gedacht. Bei Multi-Tenant haben Tenants eh eigene Namespaces.
No-Op Stub ist ausreichend: SELECT OK, aber keine echte DB-Wechsel. Tenants können Key-Präfixe nutzen
wenn sie "DBs" brauchen (`db1:key`, `db5:key`). Komplexität der echten Implementation (2 Namespace-Getter,
Audit aller GetNamespace-Aufrufe) lohnt sich nicht für Multi-Tenant Use Case.

KONZEPT - WATCH Mutex niedrige Priorität:
`watched_key_mutex_` ist global, aber WATCH ist in der Praxis sehr selten (<1% der Connections).
Grund: Moderne Apps nutzen Lua Scripts oder atomare Commands (INCR, HINCRBY, etc.) statt WATCH.
Typische Workloads (Caching, Sessions, Queues wie Laravel Queue, Pub/Sub) nutzen kein WATCH.
Falls `watched_key_size_ == 0` → Early-Exit, kein Lock. Per-Namespace Sharding nur nötig
falls Tenants WATCH intensiv nutzen (unwahrscheinlich).

LEARNING - cmdstat vs cmdstathist Trennung:
`cmdstat_*` (calls, usec, usec_per_call) ist per-NS mit Noisy-Neighbor Prevention:
Worker-Level `ns_cmd_stats_` mit shared_mutex (Map-Lookup) + per-NS mutex (Command-Update).
`cmdstathist_*` (Latenz-Histogramme mit Buckets) bleibt global und admin-only:
Memory-Overhead wäre N_namespaces × M_commands × Buckets zu hoch.
Aggregation bei INFO über alle Worker für den anfragenden Namespace.

LEARNING - PERFLOG vs SLOWLOG Namespace-Handling:
SLOWLOG ist korrekt namespace-aware mit Filter-Lambda und `*WithFilter()` Methoden.
PERFLOG fehlt diese Implementierung. Pattern: Admin sieht global, Tenant mit ns_filter.
Jedes Log-Entry braucht `ns` Feld, Filter bei GET/LEN/RESET.
}

---

## Erledigt

**Commands tenant-aware:**
- [x] PUB/SUB Tenant-Isolation (Vollständig: Datenstruktur, Subscribe/Unsubscribe, Publish, Info-Commands, Cleanup, Persistence, Tests)
- [x] Shard PubSub Namespace-Isolation (SSUBSCRIBE, SUNSUBSCRIBE, SPUBLISH)
- [x] MONITOR - Per-NS Singleton, O(1) statt O(n_workers), Copy-then-Reply (Tests: `monitor_isolation_test.go`)
- [x] CLIENT LIST/KILL - Nur eigene Connections (Tests: `client_isolation_test.go`)
- [x] SLOWLOG - Nur eigene Queries (Tests: `slowlog_test.go`)
- [x] DBSIZE, INFO keyspace - Namespace-aware
- [x] WATCH - `MakeWatchedKey(ns, key)`
- [x] FLUSHDB/FLUSHALL - FLUSHDB nur eigener NS, FLUSHALL nur Admin
- [x] kCmdAdmin für DEBUG, FLUSHMEMTABLE, FLUSHBLOCKCACHE
- [x] Blocking Mutex Namespace-Isolation (BLPOP/BRPOP/BZPOP/XREAD BLOCK) - Tests: `blocking_isolation_test.go`

**INFO Stats:**
- [x] `used_percent` - `GetTotalSize(ns)`
- [x] `connected_clients`, `monitor_clients` - `GetClientCounts()`
- [x] `blocked_clients` - ConnContext.ns + `GetBlockedClientsCount()`
- [x] `total_commands_processed`, `total_net_input/output_bytes` - Per-Worker ns_stats_ Map

**Bugs gefixt:**
- [x] FUNCTION FLUSH Segfault - Async Reset statt Cross-Thread Zugriff (Tests: `function_namespace_test.go`)
- [x] ns_locks_ Pointer-Invalidation - C++ garantiert Stabilität
- [x] Storage::Write() TOCTOU - WorkExclusivityGuard schützt

**Performance-Fixes:**
- [x] `GetNamespace()` gibt `const std::string&` statt Kopie zurück (`redis_connection.h:157`)
- [x] Doppelte `GetNamespace()`-Aufrufe eliminiert durch Caching
- [x] `WakeupBlockingConns` - `std::move` statt Kopie (`server.cc:833`)
- [x] `PublishMessage` - `pair<Worker*, int>` statt ConnContext (`server.cc:449-472`)
- [x] `FeedMonitorConns` - Per-NS Singleton mit O(1) Lookup, Copy-then-Reply (`server.cc:433-475`)

---

## Offen

**Security-Bug:**
- [x] **AUTH in MULTI** - `no-multi` Flag hinzugefügt (`cmd_server.cc:1575`)

**Hohe Priorität - Noisy-Neighbor Locking:**
- [x] **I/O unter Lock entfernen** - Copy-then-Reply Pattern
  - [x] `WakeupBlockingConns()` - `vector<pair<Worker*, fd>>` sammeln, Lock lösen, EnableWriteEvent
  - [x] `OnEntryAddedToStream()` - `vector<pair<Worker*, fd>>` sammeln, Lock lösen, EnableWriteEvent
  - [x] `WakeupWaitConnections()` - `vector<tuple<Worker*, fd, replicas>>`, Worker::Reply statt Connection::Reply
- [x] **db_job_mu_ globaler Mutex** - COMPACT admin-only (`cmd_server.cc:1603`)
  - Analyse: COMPACT/BGSAVE/SCAN blockieren sich NICHT gegenseitig (verschiedene Flags)
  - Lösung: COMPACT zu admin-only (konsistent mit BGSAVE), kein Tenant kann andere blockieren
- [x] **works_concurrency_rw_lock_ Full-Sync** - Nicht nötig (siehe Konstitution)
  - Betrifft nur Replica während Full-Sync, gibt sowieso "LOADING" zurück, also voll unnötig.

**Mittlere Priorität - INFO Stats:**
- [x] `total_connections_received` - Kumulativer Counter (per-NS nach Auth)
- [x] `instantaneous_ops_per_sec` - On-Demand + 100ms Cache + Per-NS Mutex (alle 3 Metriken)
- [x] `cmdstat_*` - Per-Command Stats per-NS, `cmdstathist_*` admin-only (Tests: `client_isolation_test.go`)
- [x] `used_memory_lua` - Admin-only by design (Lua-VM pro Worker shared, keine per-NS Attribution möglich)

**Niedrige Priorität:**
- [x] SCRIPT FLUSH Namespace-Isolation - Scripts als `f_{ns}_{sha}` (Tests: `script_isolation_test.go`, Replication: `replication_test.go`)
- [x] SELECT (Logical Databases) - Bewusst No-Op gelassen, Tenants nutzen Key-Präfixe statt DBs
- [x] MONITOR Per-NS Singleton - O(1) statt O(n_workers), Copy-then-Reply, Tests: `monitor_isolation_test.go`
- [LATER] WATCH globaler Mutex - Per-NS Sharding (nur falls WATCH intensiv genutzt, unwahrscheinlich)

**Hohe Priorität - Security (Audit 2026-02-01):**
- [ ] **PERFLOG Namespace-Isolation** - Analog zu SLOWLOG (`cmd_server.cc:383-419`)
  - PerfEntry braucht `ns` Feld
  - Filter-Methoden: `SizeWithFilter()`, `GetLatestEntriesWithFilter()`, `ResetWithFilter()`
  - Admin sieht alles, Tenant nur eigene
- [x] **STATS kCmdAdmin** - Globale RocksDB-Statistiken nur für Admin (`cmd_server.cc:1610`)

**Mittlere Priorität - Information Disclosure (Audit 2026-02-01):**
- [ ] **INFO RocksDB Section** - Admin-only (`server.cc:1316-1441`)
- [ ] **INFO Replication Section** - Admin-only (`server.cc:1528-1542`)
- [ ] **INFO CPU Section** - Admin-only (`server.cc:1846-1857`)
- [ ] **INFO Persistence Section** - Admin-only (`server.cc:1831-1844`)
- [ ] **Sequence Number in Keyspace** - Admin-only oder entfernen (`server.cc:1870`)

**Niedrige Priorität - Concurrency (Audit 2026-02-01):**
- [ ] **Namespace::List() Race Condition** - Kopie statt Referenz zurückgeben (`namespace.h:41`)
- [ ] **Connection Counting TOCTOU** - Atomic compare_exchange (`redis_connection.cc:169-177`)

---

//für mich selber: {
    GEPRÜFT: Re-AUTH während Blocking ist kein Problem (Read-Callback ist nullptr).
    GEPRÜFT: Re-AUTH während MULTI ist ein Bug → Task oben angelegt.

    RESETSTAT exisitiert nicht in kvrocks, aber in redis - ggf können wir das irgnedwann mal erweitern. 

    Genau wie Transaktionen namespace aware machen das sie nicht global locken.

    Und Lua scripts in transaktio
nen mappen.
}
