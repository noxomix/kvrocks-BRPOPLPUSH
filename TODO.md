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
}

---

## Tasks

**Commands tenant-aware:**
- [ ] MONITOR - Nur eigene Commands zeigen
- [ ] PUB/SUB - Namespace-Isolation (SUBSCRIBE, PUBLISH, PUBSUB CHANNELS etc.)
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

**Niedrige Priorität:**
- [ ] SELECT (Logical Databases) - Ist No-Op, evtl. Key-Prefix pro DB
- [ ] COMPACT kompaktiert Propagate CF nicht für Tenants (Background-Compaction macht's)
- [ ] COMPACT globales Lock - Noisy-Neighbor möglich, aber selten/manuell

---

//für mich selber, claude bitte hier erst ignorieren: {
    eine conneciton kann glaube ich den namespace wechseln indem man wieder auth schickt. Bin mir nicht sicher ob alle commands global das bedenken bzw bei block counter oder
    so könnte es sein, dass  es nur wenn nch kein namespace gestzt istder ns im Conn obj gespeichert/gestzt wird. Das dringend noch prüfen.
}
