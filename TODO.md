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
WICHTIG: Nur `(Worker*, fd)` kopieren, NICHT `Connection*` - Connection kann nach
Lock-Release gelöscht werden (Use-After-Free). `Worker::Reply(fd, msg)` und
`Worker::EnableWriteEvent(fd)` validieren fd intern → sicher.

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

LEARNING - AUTH und Namespace-Wechsel:
AUTH während MULTI ist ein Bug - Commands nach AUTH in der Transaction laufen im neuen
Namespace. Fix: AUTH braucht `no-multi` Flag.
AUTH während Blocking (BLPOP etc.) ist KEIN Problem - Read-Callback ist nullptr während
Blocking, neue Commands werden erst NACH dem Blocking verarbeitet.
}

---

## Erledigt

**Commands tenant-aware:**
- [x] PUB/SUB Tenant-Isolation (Vollständig: Datenstruktur, Subscribe/Unsubscribe, Publish, Info-Commands, Cleanup, Persistence, Tests)
- [x] Shard PubSub Namespace-Isolation (SSUBSCRIBE, SUNSUBSCRIBE, SPUBLISH)
- [x] MONITOR - Tenant sieht nur eigene, Admin alle
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

---

## Offen

**Security-Bug:**
- [x] **AUTH in MULTI** - `no-multi` Flag hinzugefügt (`cmd_server.cc:1575`)

**Hohe Priorität - Noisy-Neighbor Locking:**
- [x] **I/O unter Lock entfernen** - Copy-then-Reply Pattern
  - [x] `WakeupBlockingConns()` - `vector<pair<Worker*, fd>>` sammeln, Lock lösen, EnableWriteEvent
  - [x] `OnEntryAddedToStream()` - `vector<pair<Worker*, fd>>` sammeln, Lock lösen, EnableWriteEvent
  - [x] `WakeupWaitConnections()` - `vector<tuple<Worker*, fd, replicas>>`, Worker::Reply statt Connection::Reply
- [ ] **db_job_mu_ globaler Mutex** (server.h:433)
  - Problem: Ein Tenant's COMPACT (Minuten) blockiert alle anderen DB-Jobs
  - Option A: Aufteilen in `compaction_mu_`, `bgsave_mu_`, `scan_mu_`
  - Option B: Per-Namespace Job-Queues
- [ ] **works_concurrency_rw_lock_ Full-Sync** (server.h:476)
  - Problem: Full-Sync nimmt exclusive Lock, busy-wait mit 1ms polling
  - Option A: Condition Variable statt busy-wait
  - Option B: Sync ohne globalen Command-Block

**Mittlere Priorität - INFO Stats:**
- [ ] `total_connections_received` - Kumulativer Counter
- [ ] `instantaneous_ops_per_sec` - Rate-Berechnung
- [ ] `cmdstat_*` - Per-Command Stats (Memory-Overhead bedenken)
- [ ] `used_memory_lua` - Architektonisch schwierig (Lua-VM pro Worker shared)

**Niedrige Priorität:**
- [ ] SCRIPT FLUSH Namespace-Isolation - Scripts als `f_{ns}_{sha}` oder nur kCmdAdmin
- [ ] SELECT (Logical Databases) - Workaround mit Sub-Namespaces möglich
- [ ] COMPACT kompaktiert Propagate CF nicht für Tenants
- [ ] MONITOR globales Lock - O(n_workers) Locks pro Command, Reply() unter Lock

---

//für mich selber: {
    GEPRÜFT: Re-AUTH während Blocking ist kein Problem (Read-Callback ist nullptr).
    GEPRÜFT: Re-AUTH während MULTI ist ein Bug → Task oben angelegt.
}
