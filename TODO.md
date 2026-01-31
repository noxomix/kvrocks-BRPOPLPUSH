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
Stattdessen: Gezielt nur die benötigten Daten anfragen (z.B. `GetNamespaceStats(ns)` statt `GetAllStats()`).

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

KONZEPT - EVAL Scripts vs. FUNCTION Isolation:
EVAL-Scripts: Global by SHA gecached (`f_{sha}`), aber Ausführung namespace-aware (redis.call nutzt conn->GetNamespace()).
FUNCTION: Vollständig per-NS isoliert (`<ns>_<lib>`), LuaResetNamespace() für Cleanup.
SCRIPT FLUSH: Global - löscht alle Scripts (Noisy Neighbor). Kein Auto-Eviction für ungenutzte Scripts.
Hybrid-Fix: Scripts als `f_{ns}_{sha}` speichern (Isolation + Kollisionsschutz) + SCRIPT FLUSH admin-only.

LEARNING - Blocking-State ist server-lokal:
Blocking-State (welche Connections warten worauf) ist transient und nicht repliziert.
Nur Daten-Operationen (LPUSH, XADD) werden repliziert. Jeder Server (Primary/Replica)
verwaltet eigene Blocking-Clients. → Per-Namespace Blocking-Refactoring benötigt keine
Replication-Änderungen.
}

---

## Tasks

**Commands tenant-aware:**
- [x] **PUB/SUB Tenant-Isolation** - Vollständige Isolation, Admin = normaler Tenant
  - [x] Phase 1: Datenstruktur (server.h)
    - [x] `NamespacePubSub` struct (mutex + channels + patterns Maps)
    - [x] `pubsub_namespaces_` Map mit shared_mutex
    - [x] `CleanupPubSubNamespace()` deklarieren
  - [x] Phase 2: Subscribe/Unsubscribe (server.cc)
    - [x] `SubscribeChannel()` - unique_lock + inline GetOrCreate
    - [x] `UnsubscribeChannel()` - NS-Lookup + NS-Lock
    - [x] `PSubscribeChannel()` - unique_lock + inline GetOrCreate
    - [x] `PUnsubscribeChannel()` - NS-Lookup + NS-Lock
  - [x] Phase 3: Publish (server.cc)
    - [x] `PublishMessage()` - NS-Lookup, Pattern-Match nur eigene, Reply außerhalb Lock
  - [x] Phase 4: Info-Commands
    - [x] `GetChannelsByPattern()` - Nur eigene NS (Admin = normaler Tenant)
    - [x] `ListChannelSubscribeNum()` - Nur eigene NS (Admin = normaler Tenant)
    - [x] `GetPubSubPatternSize()` - Namespace-Parameter (Admin = normaler Tenant)
    - [x] cmd_pubsub.cc: PUBSUB CHANNELS/NUMSUB/NUMPAT namespace-aware
    - [x] INFO pubsub_channels/patterns: Nur eigene NS
  - [x] Phase 5: Cleanup
    - [x] cmd_server.cc: `NAMESPACE DEL` ruft `CleanupPubSubNamespace()` auf
  - [x] Phase 6: Persistence (RocksDB)
    - [x] redis_pubsub.cc: `ComposeNamespaceKey()` für Replication
    - [x] replication.cc: `ExtractNamespaceKey()` beim Empfang
  - [x] Phase 7: Tests
    - [x] `pubsub_isolation_test.go` - Cross-Tenant, Same-NS, Pattern, PUBSUB CHANNELS/NUMSUB/NUMPAT
  - [ ] SPÄTER: Shard PubSub (SSUBSCRIBE etc.) - wird vorerst nicht genutzt
- [x] MONITOR - Bereits namespace-aware (Tenant sieht nur eigene, Admin alle)
- [x] CLIENT LIST/KILL - Nur eigene Connections (Tests: `client_isolation_test.go`)
- [x] SLOWLOG - Nur eigene Queries (Tests: `slowlog_test.go`)
- [x] DBSIZE, INFO keyspace - Bereits namespace-aware
- [x] WATCH - `MakeWatchedKey(ns, key)`
- [x] FLUSHDB/FLUSHALL - FLUSHDB nur eigener NS, FLUSHALL nur Admin
- [x] kCmdAdmin für DEBUG, FLUSHMEMTABLE, FLUSHBLOCKCACHE

**INFO Stats:**
- [ ] `total_connections_received` - Kumulativer Counter (einfach)
- [ ] `instantaneous_ops_per_sec` - Rate-Berechnung (mittel)
- [ ] `cmdstat_*` - Per-Command Stats (mittel-hoch, Memory-Overhead)
- [ ] `used_memory_lua` - Architektonisch schwierig (Lua-VM pro Worker shared)
- [x] `used_percent` - `GetTotalSize(ns)`
- [x] `connected_clients`, `monitor_clients` - `GetClientCounts()`
- [x] `blocked_clients` - ConnContext.ns + `GetBlockedClientsCount()`
- [x] `total_commands_processed`, `total_net_input/output_bytes` - Per-Worker ns_stats_ Map

**Bugs gefixt:**
- [x] FUNCTION FLUSH Segfault - Async Reset statt Cross-Thread Zugriff (Tests: `function_namespace_test.go`)
- [x] ns_locks_ Pointer-Invalidation - Kein Problem (C++ garantiert Stabilität)
- [x] Storage::Write() TOCTOU - WorkExclusivityGuard schützt

**Noisy-Neighbor Locking (P1):**
- [ ] **I/O unter Lock entfernen** - Copy-then-Reply Pattern (wie PublishMessage)
  - [ ] `WakeupBlockingConns()` (server.cc:819-840) - LPUSH/RPUSH/ZADD weckt BLPOP/BZPOP
    - [ ] Blocked clients unter Lock in Vector kopieren
    - [ ] Lock releasen
    - [ ] `EnableWriteEvent()` außerhalb Lock aufrufen
  - [ ] `OnEntryAddedToStream()` (server.cc:842-868) - XADD weckt XREAD BLOCK
    - [ ] Stream consumers unter Lock in Vector kopieren
    - [ ] Lock releasen
    - [ ] `EnableWriteEvent()` außerhalb Lock aufrufen
  - [ ] `WakeupWaitConnections()` (server.cc:888-912) - Replication WAIT
    - [ ] Wait contexts unter Lock in Vector kopieren
    - [ ] Lock releasen
    - [ ] `Reply()` + `EnableWriteEvent()` außerhalb Lock aufrufen
- [ ] **db_job_mu_ globaler Mutex** (server.h:433) - COMPACT/BGSAVE/DBSIZE blockieren sich gegenseitig
  - Problem: Ein Tenant's COMPACT (Minuten) blockiert alle anderen DB-Jobs
  - [ ] Option A: Aufteilen in `compaction_mu_`, `bgsave_mu_`, `scan_mu_`
  - [ ] Option B: Per-Namespace Job-Queues
- [ ] **works_concurrency_rw_lock_ Full-Sync** (server.h:476) - Blockiert ALLE Commands
  - Problem: Full-Sync nimmt exclusive Lock, busy-wait mit 1ms polling (server.cc:1801-1808)
  - [ ] Option A: Condition Variable statt busy-wait
  - [ ] Option B: Sync ohne globalen Command-Block

**Niedrige Priorität:**
- [ ] **SCRIPT FLUSH Namespace-Isolation** - Hybrid-Ansatz
  - [ ] Scripts als `f_{ns}_{sha}` statt `f_{sha}` speichern (scripting.cc)
  - [ ] SCRIPT FLUSH als kCmdAdmin markieren (cmd_script.cc)
  - [ ] Alternativ: Nur kCmdAdmin ohne Prefix-Änderung (minimal)
- [ ] SELECT (Logical Databases) - Ist No-Op, evtl. workround indem man "sub tnenats" implementiert (also namespace zB <ns> + "2" oder so und dann set logic und überprüfung)
- [ ] COMPACT kompaktiert Propagate CF nicht für Tenants (Background-Compaction macht's)
- [ ] COMPACT globales Lock - Noisy-Neighbor möglich, aber selten/manuell
- [ ] MONITOR globales Lock - O(n_workers) Locks pro Command wenn aktiv, Reply() unter Lock
- [x] **Blocking Mutex Namespace-Isolation** (~150-200 Zeilen) - Noisy-Neighbor bei BLPOP/XREAD BLOCK
  - [x] Phase 1: Datenstrukturen (server.h)
    - [x] `NamespaceBlockingKeys` struct (mutex + keys Map)
    - [x] `NamespaceStreamConsumers` struct (mutex + consumers Map)
    - [x] `blocking_keys_by_ns_` + `stream_consumers_by_ns_` Maps mit shared_mutex
  - [x] Phase 2: Key-Blocking (server.cc)
    - [x] `BlockOnKey()` - GetOrCreate + NS-Lock
    - [x] `UnblockOnKey()` - NS-Lookup + NS-Lock
    - [x] `WakeupBlockingConns()` - ns Parameter hinzugefügt
  - [x] Phase 3: Stream-Blocking (server.cc)
    - [x] `BlockOnStreams()` - GetOrCreate + NS-Lock
    - [x] `UnblockOnStreams()` - NS-Lookup + NS-Lock
    - [x] `OnEntryAddedToStream()` - NS-Lookup (ns bereits Parameter)
  - [x] Phase 4: Caller-Anpassungen
    - [x] cmd_list.cc: WakeupBlockingConns mit ns
    - [x] cmd_zset.cc: WakeupBlockingConns mit ns
  - [x] Phase 5: Cleanup
    - [x] cmd_server.cc: `NAMESPACE DEL` ruft `CleanupBlockingNamespace()` auf
  - [x] Phase 6: GetBlockedClientsCount per-NS
  - [x] Phase 7: Tests
    - [x] `blocking_isolation_test.go` - Cross-Tenant Isolation (Tests: `blocking_isolation_test.go`)

---

**Performance-Fixes (Throughput-Regression nach Isolation-Änderungen):**
- [x] **KRITISCH: `GetNamespace()` gibt Kopie statt Referenz zurück**
  - Datei: `src/server/redis_connection.h:157`
  - Problem: `std::string GetNamespace() const { return ns_; }` kopiert String bei JEDEM Aufruf
  - Impact: 7-11 String-Kopien (Heap-Allokationen) pro SET-Befehl
  - Fix: `const std::string& GetNamespace() const { return ns_; }`
- [x] Doppelte `GetNamespace()`-Aufrufe eliminieren (nach obigem Fix weniger kritisch)
  - `src/server/redis_connection.cc:144-145` - 2x Aufruf bei Reply
  - `src/server/redis_connection.cc:370-371` - 2x Aufruf bei Execute
  - `src/server/redis_request.cc:68-69, 109-110, 136-137` - 2x Aufrufe beim Parsen
  - Fix: `const auto& ns = GetNamespace();` einmal cachen
- [x] `WakeupBlockingConns` - `std::move` statt Kopie
  - Datei: `src/server/server.cc:833`
  - Problem: `auto conn_ctx = iter->second.front();` kopiert ConnContext inkl. String
  - Fix: `auto conn_ctx = std::move(iter->second.front());`
- [x] `PublishMessage` - nur Worker*/fd statt ConnContext kopieren
  - Datei: `src/server/server.cc:449-472`
  - Problem: `vector<ConnContext>` kopiert ns-String für jeden Subscriber
  - Fix: `vector<pair<Worker*, int>>` - ns wird für Reply nicht benötigt

---

//für mich selber, claude bitte hier erst ignorieren: {
    eine conneciton kann glaube ich den namespace wechseln indem man wieder auth schickt. Bin mir nicht sicher ob alle commands global das bedenken bzw bei block counter oder
    so könnte es sein, dass  es nur wenn nch kein namespace gestzt istder ns im Conn obj gespeichert/gestzt wird. Das dringend noch prüfen.
}achso
