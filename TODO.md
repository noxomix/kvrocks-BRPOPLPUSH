# Namespace-Isolation: Offene Probleme

Konstitution: {
Das ziel ist KVrocks ist tenant aware, pro instanz etwa 5-10 tenants. Jeder Tenant soll 10.000 writes gleichezeitig können also im wortcase 10.000 connecitons oaralle.
Worker sind naütlrich nicht für bestimmte tenant reserviert. Es ist wichtig dies zu verstehen, weil so manche arten von locks oder datenstrutkturen entpsechend auf diesen demand
angepasst werden müssen das wir später kein bottleneck haben.


In KVrocks ist ein ADMIN automatisch nur der defualt namespace (__namespace/_namespace). Bei tenant aware functions ist es wichtig abzuwägen,
ob im ADMIN fall global aggiert werden soll zB Flush scripts alle namespace scripts flusht oder nur im eignene tenant. Das haben wir bislang nicht zuverlässig
gemacht aber es ist nicht an allen stellen schlim, lediglich sollte es bedacht und überlegt werden.

Auf einem Worker können viele Tenant connections laufen. Aber nur eine davon ist quasi "active" also quasi eine eventloop (soweit ich das richtig verstand hab).
Wenn wir irgendwelche commands haben die daten von mehrere workern direkt aquirieren müssen zB statistiken über einen tenant (namespace) dann dürfen andere worker
nicht so lange blockieren, sonst könnte ein namespace andere namespaces absichlich verlangsamen (noisy oder evil neighbor).

WICHTIG - ns_ Default-Initialisierung:
Die Connection::ns_ Variable MUSS leer ("") bleiben bis AUTH erfolgt ist!
Der Auth-Flow in ExecuteCommands() prüft `if (GetNamespace().empty())` um zu entscheiden ob AUTH nötig ist.
Wenn ns_ einen Default-Wert wie kDefaultNamespace hat, wird dieser Check übersprungen und:
- BecomeAdmin() wird nie aufgerufen wenn kein requirepass gesetzt ist
- Alle Admin-Commands (CLUSTER, CONFIG, etc.) schlagen fehl mit "admin permission required"
Für per-Namespace Stats: Prüfe `!GetNamespace().empty()` BEVOR Stats getrackt werden.
Bytes/Commands VOR AUTH werden nur global gezählt (korrekt, da Namespace noch unbekannt).
}

## Erledigt

- [x] ~~ns_locks_ Pointer-Invalidation~~ - KEIN PROBLEM (C++ Standard garantiert Stabilität)
- [x] ~~Storage::Write() TOCTOU~~ - WorkExclusivityGuard schützt
- [x] ~~WATCH nicht namespace-aware~~ - GEFIXT mit `MakeWatchedKey(ns, key)`
- [x] ~~FLUSHDB/FLUSHALL~~ - FLUSHDB löscht nur eigenen NS, FLUSHALL nur Admin
- [x] ~~FUNCTION FLUSH Segfault~~ - GEFIXT mit Async Reset (siehe unten)

---

## Gefixt: FUNCTION FLUSH Segfault

**Problem:** `ScriptResetNamespace()` griff auf Lua-States anderer Worker zu → Use-After-Free/Segfault

**Lösung:** Async Reset (Lazy Reset)
- Jeder Worker resettet nur seinen **eigenen** Lua-State
- `FUNCTION FLUSH` setzt nur ein Flag pro Worker
- Vor jeder Lua-Operation prüft Worker das Flag und resettet sich selbst
- Kein Cross-Thread Zugriff mehr → Thread-safe
- Perfekte Tenant-Isolation (Tenant A kann B nicht blockieren)

**Bug gefunden:** Self-Reset bei `FUNCTION LOAD REPLACE`
- `ScriptResetNamespace()` setzte Flag für ALLE Worker inkl. aktuellem
- Bei `FUNCTION LOAD REPLACE`: Delete → Load → nächste Lua-Op sieht eigenes Flag → Reset → Library weg!
- **Fix:** `ScriptResetNamespace(ns, Worker* exclude)` - aktueller Worker wird ausgeschlossen
- Aktueller Worker ruft `LuaResetNamespace(ns)` synchron auf, andere werden asynchron markiert

**Geänderte Dateien:**
- `src/server/server.h` - `script_reset_generation_` (Generation Counter)
- `src/server/server.cc` - `ScriptReset()`, `ScriptResetNamespace()`
- `src/server/worker.h` - `ns_reset_mutex_`, `namespaces_to_reset_`, `last_script_reset_generation_`
- `src/server/worker.cc` - `MarkNamespaceForReset()`, `CheckAndResetIfNeeded()`
- `src/storage/scripting.cc` - `CheckAndResetIfNeeded()` vor allen Lua-Einstiegspunkten
- `src/commands/cmd_script.cc` - `CheckAndResetIfNeeded()` vor SCRIPT LOAD

**Tests:** `tests/gocase/unit/scripting/function_namespace_test.go`

**Hinweis:** Gelegentlich schlägt `TestFullSyncReplication` fehl (Timeout bei WaitForOffsetSync).
Unklar ob durch diese Änderungen verursacht oder vorher existierendes Flaky-Test-Problem.
Der Test ist timing-sensitiv und hängt nicht von Lua/Scripting ab.

---

## Gefixt: SLOWLOG Namespace-Isolation

**Problem:** Globaler `slow_log_` - alle Tenants sehen alle Einträge (Informationsleakage)

**Lösung:** Namespace im SlowEntry speichern + bei Abfrage filtern
- `SlowEntry.ns` Feld hinzugefügt
- Generische Filter-Methoden in LogCollector: `SizeWithFilter()`, `ResetWithFilter()`, `GetLatestEntriesWithFilter()`
- CommandSlowlog prüft `conn->IsAdmin()` und filtert entsprechend

**Verhalten:**
| Command | Tenant | Admin |
|---------|--------|-------|
| SLOWLOG GET | Nur eigene | Alle |
| SLOWLOG LEN | Count eigene | Count alle |
| SLOWLOG RESET | Löscht eigene | Löscht alle |

**Geänderte Dateien:**
- `src/stats/log_collector.h` - `ns` Feld + Filter-Methodendeklarationen
- `src/stats/log_collector.cc` - Filter-Methoden implementiert
- `src/server/server.cc` - `entry->ns = conn->GetNamespace()`
- `src/commands/cmd_server.cc` - CommandSlowlog tenant-aware

**Tests:** `tests/gocase/unit/slowlog/slowlog_test.go:TestSlowlogNamespaceIsolation`

---

## Offen: Command-Klassifizierung für Tenant-Isolation

### Prinzip
**Global = Beeinflusst etwas außerhalb des eigenen Namespace**

### 1. Admin-only (nicht tenant-aware möglich)
Technisch nicht isolierbar - müssen Admin-only bleiben:

**kCmdAdmin fehlt - hinzufügen:**
- [ ] COMPACT - RocksDB-global
- [ ] FLUSHMEMTABLE - RocksDB-global
- [ ] FLUSHBLOCKCACHE - RocksDB-global
- [ ] DEBUG - Kann Server crashen

**kCmdAdmin bereits vorhanden:**
- [x] CONFIG SET, SHUTDOWN, BGSAVE/RDB/SST
- [x] SLAVEOF/REPLICAOF, CLUSTER *, FLUSHALL

### 2. Tenant-aware machen (sinnvoll)

- [x] CLIENT LIST - Nur eigene Connections zeigen ✅ GEFIXT
- [x] CLIENT KILL - Nur eigene Connections killen ✅ GEFIXT
- [x] SLOWLOG - Nur eigene Queries zeigen ✅ GEFIXT
- [ ] MONITOR - Nur eigene Commands zeigen (Mittel)
- [x] INFO keyspace - ✅ Bereits namespace-aware (keys, expires, avg_ttl, used_db_size)
  - [x] `used_percent` - GEFIXT (`GetTotalSize(ns)`)
- [x] INFO clients - `connected_clients`, `monitor_clients` ✅ per-NS, `blocked_clients` noch global (Batch 1.5)
- [x] DBSIZE - ✅ Bereits namespace-aware

### 3. Bereits korrekt (tenant-lokal)
- Alle Daten-Commands (GET, SET, HGET, ZADD, etc.)
- KEYS, SCAN, FLUSHDB
- MULTI/EXEC/WATCH

---

## Implementierungs-Reihenfolge

1. [ ] Quick Win: `kCmdAdmin` für COMPACT, DEBUG, FLUSHMEMTABLE, FLUSHBLOCKCACHE
2. [x] ~~Prüfen: DBSIZE, INFO~~ - Beide bereits namespace-aware
3. [x] ~~CLIENT LIST/KILL tenant-aware~~ - GEFIXT (Tests: `client_isolation_test.go`)
4. [x] ~~SLOWLOG tenant-aware~~ - GEFIXT (Tests: `slowlog_test.go:TestSlowlogNamespaceIsolation`)
5. [ ] MONITOR tenant-aware
6. [ ] INFO vollständig tenant-aware:
   - **Ansatz:** Per-Connection Stats → bei INFO aggregieren (kein Hot-Path Impact)
   - **Bleiben global:** Server, CPU, Persistence, Replication, RocksDB, Cluster

   **Batch 1 - Quick Wins (Trivial/Niedrig):** ✅ ERLEDIGT
   - [x] `used_percent` - `GetTotalSize(ns)` statt `GetTotalSize()`
   - [x] `connected_clients` - On-demand per-Namespace zählen via `GetClientCounts()`
   - [x] `monitor_clients` - On-demand per-Namespace zählen via `GetClientCounts()`

   **Batch 1.5 - blocked_clients (Mittel, verschoben):**
   - [ ] `blocked_clients` - Aktuell global, später tenant-aware
   > Blocked = Connection wartet auf BLPOP/BRPOP/XREAD BLOCK etc.
   > Wird über Server-weite Wait-Contexts getrackt, nicht per-Connection-Flag.
   > Tenant-aware = BlockingKey-Struktur um Namespace erweitern.

   **Batch 2 - Per-Connection Counter (Mittel):**
   - [ ] `total_commands_processed` - Counter zu Connection
   - [ ] `total_net_input_bytes` - Counter zu Connection
   - [ ] `total_net_output_bytes` - Counter zu Connection

   **Batch 3 - Komplexer (Mittel-Hoch):**
   - [ ] `instantaneous_ops_per_sec` - Rate-Berechnung
   - [ ] `total_connections_received` - Kumulativer Counter
   - [ ] `used_memory_lua` - Lua-States aggregieren

   **Batch 4 - Aufwendig (Hoch):**
   - [ ] `cmdstat_*` - Per-Connection Command-Map + Aggregation
