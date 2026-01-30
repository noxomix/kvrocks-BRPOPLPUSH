# Namespace-Isolation: Offene Probleme

## Echte Probleme

- [x] **WATCH nicht namespace-aware** (HOCH) - **GEFIXT**
  - Datei: `src/server/server.h:455`, `src/server/server.cc:2097-2174`
  - Problem: watched_key_map_ speichert Keys ohne Namespace-Prefix
  - Lösung: Keys werden jetzt mit `MakeWatchedKey(ns, key)` namespace-prefixed gespeichert

- [ ] **FUNCTION FLUSH Segfault** (KRITISCH) - ROOT CAUSE GEFUNDEN
  - Test: `TestFunctionFlushParallelIsolation` in `tests/gocase/unit/scripting/function_namespace_test.go`
  - Problem: Server crasht sporadisch mit Segfault bei parallelen FUNCTION FLUSH
  - **ROOT CAUSE:** `ScriptResetNamespace()` in `src/server/server.cc:1894-1898`
    - Worker 1 ruft `LuaResetNamespace()` auf Lua-States von Worker 2, 3, 4... auf
    - Lua-States sind NICHT thread-safe!
    - Wenn ein anderer Worker gerade Lua ausführt → Use-After-Free / Segfault
  - **Lösung:** `WorkExclusivityGuard` verwenden (wie bei `ScriptFlush`)
  - Nicht durch WATCH-Änderung verursacht

- [ ] **Globale Commands nutzen NS-Locks** (MEDIUM)
  - Datei: `src/server/redis_connection.cc:458-464`
  - Problem: CONFIG SET, SHUTDOWN, CLUSTER-Ops etc. sollten global locken
  - Lösung: kCmdGlobalExclusive Flag einführen
  - Betroffene Commands: SHUTDOWN, DEBUG, RDB, SST, SLAVEOF, etc.

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

- [x] ~~FLUSHDB/FLUSHALL~~ - FLUSHDB löscht nur eigenen Namespace, FLUSHALL löscht alles (nur Admin)

