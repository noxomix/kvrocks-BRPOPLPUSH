# kvrocks Atomicity & Konsistenz

## Grundprinzip

| Modus | Batching | fsyncs pro N Writes |
|-------|----------|---------------------|
| Einzelne Commands | Nein | N |
| MULTI/EXEC | Ja | 1 |
| Lua Script | **Nein** | N |

## MULTI/EXEC Flow

```
MULTI                    → BeginTxn(), is_txn_mode_=true
  SET key1 val1          → Write() returns OK (kein fsync)
  SET key2 val2          → Write() returns OK (kein fsync)
EXEC                     → CommitTxn() → db_->Write() → 1x fsync
```

- Commands werden in `multi_cmds_` (deque) gequeued
- Ein globales `txn_write_batch_` für alle Commands
- Code: `src/commands/cmd_txn.cc:83-90`

## Lua Script Flow

```
EVAL "redis.call('SET','k1','v1'); redis.call('SET','k2','v2')"
  ↓
redis.call('SET','k1','v1')  → ExecuteCommand() → db_->Write() → fsync #1
redis.call('SET','k2','v2')  → ExecuteCommand() → db_->Write() → fsync #2
```

- **Kein BeginTxn()** in scripting.cc
- `is_txn_mode_` bleibt false
- Jeder redis.call() schreibt sofort
- Code: `src/storage/scripting.cc:881`

## Storage::Write Logik

```cpp
// src/storage/storage.cc:711-717
rocksdb::Status Storage::Write(...) {
  if (is_txn_mode_) {
    return rocksdb::Status::OK();      // ← Batch, kein Write
  }
  return writeToDB(...);               // ← Sofort schreiben!
}
```

## Crash-Konsistenz

### MULTI/EXEC: Atomar (Alles oder Nichts)

```
MULTI
SET key1 val1  → nur RAM
SET key2 val2  → nur RAM
⚡ CRASH
EXEC           → nie erreicht

Nach Restart: key1 + key2 unverändert ✓
```

### Lua Script: NICHT atomar

```lua
redis.call('SET', 'key1', 'val1')  -- fsync ✓
-- ⚡ CRASH
redis.call('SET', 'key2', 'val2')  -- nie ausgeführt

Nach Restart: key1='val1', key2=alter Wert ✗
```

## BeginTxn() Aufrufe im Codebase

```
src/commands/cmd_txn.cc:83  ← NUR hier (EXEC Command)
```

Lua ruft BeginTxn() **nicht** auf.

## Relevante Code-Pfade

| Komponente | Datei |
|------------|-------|
| MULTI Command | `src/commands/cmd_txn.cc:30-43` |
| EXEC Command | `src/commands/cmd_txn.cc:61-105` |
| BeginTxn | `src/storage/storage.cc:981-994` |
| CommitTxn | `src/storage/storage.cc:996-1009` |
| txn_write_batch_ | `src/storage/storage.h:398` |
| Lua EvalGenericCommand | `src/storage/scripting.cc:638-719` |
| Lua redis.call() | `src/storage/scripting.cc:881` |
| Storage::Write | `src/storage/storage.cc:711-717` |
| writeToDB | `src/storage/storage.cc:720-745` |

## Warum EXEC global lockt

### Architektur

```
┌─────────────────────────────────────┐
│        Storage (EINE Instanz!)      │
│                                     │
│  is_txn_mode_ = false/true          │  ← GLOBAL
│  txn_write_batch_ = ...             │  ← GLOBAL, nur 1!
└─────────────────────────────────────┘
                │ shared by all
    ┌───────────┼───────────┐
    ▼           ▼           ▼
 Worker 0    Worker 1    Worker 2
 (Conn A,B)  (Conn C,D)  (Conn E,F)
```

### Problem: Nur EIN txn_write_batch_

```cpp
// src/storage/storage.h:391-398
class Storage {
  std::atomic<bool> is_txn_mode_ = false;
  std::unique_ptr<WriteBatchWithIndex> txn_write_batch_;  // NUR EINER!
};
```

Wenn 2 EXEC parallel laufen würden:
- Beide setzen `is_txn_mode_ = true`
- Beide schreiben in denselben `txn_write_batch_`
- Race Condition → Datenverlust

### Lösung: EXEC ist `exclusive`

```cpp
// src/commands/cmd_txn.cc:133
MakeCmdAttr<CommandExec>("exec", 1, "exclusive bypass-multi slow", NO_KEY);
```

```cpp
// src/server/redis_connection.cc:455-456
if (cmd_flags & kCmdExclusive) {
  exclusivity = srv_->WorkExclusivityGuard();  // unique_lock!
}
```

**Effekt:** EXEC blockiert ALLE Worker im gesamten Server.

### Locking-Übersicht

| Command | Lock | Andere blockiert? |
|---------|------|-------------------|
| GET, SET, ... | shared_lock | Nein |
| **EXEC** | unique_lock | **Ja, alle!** |
| **Lua (EVAL)** | shared_lock | Nein |
| FLUSHDB, CONFIG SET | unique_lock | Ja, alle |

## Warum Lua kein Batching hat

### Das Dilemma

| Feature | Benötigt | EXEC hat es | Lua hat es |
|---------|----------|-------------|------------|
| Batched Writes (1 fsync) | ✓ | ✓ | ✗ |
| Read-Your-Own-Writes | ✓ | ✓ | ✓ |
| Parallelität | ✓ | ✗ | ✓ |

**Alle drei gleichzeitig sind mit aktuellem Design unmöglich.**

### Option A: Lua wird exclusive (einfach, langsam)

```cpp
// Änderung: EVAL als exclusive markieren
MakeCmdAttr<CommandEval>("eval", ..., "exclusive", ...);
```

- Aufwand: ~10 Zeilen
- Problem: Nur 1 Lua-Script gleichzeitig → Performance-Killer

### Option B: Per-Connection Batches (mittel, Refactoring)

```cpp
// Hypothetisch: Jede Connection hat eigenen Batch
class Connection {
  bool is_in_txn_ = false;
  std::unique_ptr<WriteBatchWithIndex> txn_batch_;
};
```

- Aufwand: ~200-400 Zeilen, mehrere Dateien
- Ermöglicht parallele Transaktionen
- Erfordert Änderungen an Storage::Write, GetWriteBatchBase, etc.

### Option C: Lua-spezifisches Batching (komplex)

```cpp
// In EvalGenericCommand:
auto lua_batch = std::make_unique<WriteBatchWithIndex>();
// Alle redis.call() schreiben in lua_batch
// Am Ende: storage->Write(lua_batch)
```

- Aufwand: ~300-500 Zeilen
- Commands müssen wissen, ob sie in Lua laufen
- Saubere Trennung von MULTI/EXEC

## EVAL ist auch exclusive (by default!)

```cpp
// src/commands/cmd_script.cc:128-134
uint64_t GenerateEvalFlags(..., const Config &config) {
  if (!config.lua_strict_key_accessing) {  // Default: false!
    return flags | kCmdExclusive;
  }
  return flags;
}
```

**Mit Default-Config blockiert EVAL bereits ALLE Worker!**

Config `lua_strict_key_accessing=true` → EVAL wird parallel (aber immer noch N fsyncs).

## exclusive ≠ Batching

| Konzept | Was es tut | Kontrolliert durch |
|---------|-----------|-------------------|
| `exclusive` Flag | Blockiert andere Worker | `kCmdExclusive` |
| `is_txn_mode_` | Batcht Writes | `BeginTxn()`/`CommitTxn()` |

**Diese sind UNABHÄNGIG!** EVAL mit exclusive=true schreibt trotzdem sofort pro redis.call().

## Alle exclusive Commands

### Müssen global bleiben

| Command | Grund |
|---------|-------|
| FLUSHDB/FLUSHALL | Löscht alle Keys |
| SHUTDOWN | Stoppt Server |
| SLAVEOF/REPLICAOF | Ändert Replication |
| RDB | Lädt Daten |
| DEBUG | Debug-Operationen |
| FLUSHMEMTABLE | RocksDB Memtable |
| FLUSHBLOCKCACHE | RocksDB Cache |
| FT.CREATE/DROP | Search Index |
| SCRIPT FLUSH | Globaler Script-Cache |
| FUNCTION | Globaler Function-State |
| CLUSTER (manche) | Cluster-Topology |

### Könnten per-Worker/Connection werden

| Command | Blocker | Lösung | Aufwand |
|---------|---------|--------|---------|
| **EXEC** | Globales `txn_write_batch_` | Per-Connection Batch | ~200-300 Zeilen |
| **EVAL** | `lua_strict_key_accessing=false` | Config ändern ODER Batching | Config: 0 / Batching: ~300-400 Zeilen |

## Zusammenfassung

| Frage | Antwort |
|-------|---------|
| Lua Script = 1 fsync? | **Nein**, N fsyncs für N Writes |
| MULTI/EXEC = 1 fsync? | **Ja** |
| Lua crash-sicher? | **Nein**, partial writes möglich |
| MULTI/EXEC crash-sicher? | **Ja**, atomar |
| Warum EXEC global lockt? | Nur 1 globaler `txn_write_batch_` |
| EVAL auch exclusive? | **Ja**, wenn `lua_strict_key_accessing=false` (default) |
| exclusive = Batching? | **Nein**, unabhängige Konzepte |
| Lua + Batching möglich? | Ja, erfordert Code-Änderungen |
| Einfachste Parallelität? | `lua_strict_key_accessing=true` (aber N fsyncs) |
| Beste Lösung? | Per-Connection Batches (mehr Aufwand) |