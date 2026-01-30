# Namespace-Isolation: Offene Probleme

## Echte Probleme

- [ ] **WATCH nicht namespace-aware** (HOCH)
  - Datei: `src/server/server.h:455`, `src/server/server.cc:2097-2174`
  - Problem: watched_key_map_ speichert Keys ohne Namespace-Prefix

- [ ] **Globale Commands nutzen NS-Locks** (MEDIUM)
  - Datei: `src/server/redis_connection.cc:458-464`
  - Problem: CONFIG SET, SHUTDOWN, CLUSTER-Ops etc. sollten global locken
  - Lösung: kCmdGlobalExclusive Flag einführen

- [ ] **FLUSHALL Verhalten klären** (DESIGN)
  - Datei: `src/commands/cmd_server.cc:174`
  - Frage: Gewollt (Tenant-Isolation) oder Bug (sollte alle NS löschen)?

---

## Kein Problem (verifiziert)

- [x] ~~**ns_locks_ Pointer-Invalidation**~~ - **KEIN PROBLEM**
  - C++ Standard garantiert: Pointer/Referenzen auf `unordered_map` Elemente bleiben bei Insert/Rehash gültig
  - Nur Iteratoren werden invalidiert, aber wir verwenden Pointer (`&it->second`)
  - Elemente werden nie gelöscht (`ns_locks_.erase()` existiert nicht)
  - Quelle: https://en.cppreference.com/w/cpp/container/unordered_map/insert

- [x] ~~Storage::Write() TOCTOU~~ - EXEC hält WorkExclusivityGuard(ns) während gesamter Transaktion

- [x] ~~GetWriteBatchBase Observer-Pointer~~ - Gleicher Grund (Lock-Hierarchie schützt)

- [x] ~~Parallele NS-Transaktionen~~ - Jeder NS hat eigene isolierte Lock-Säule
