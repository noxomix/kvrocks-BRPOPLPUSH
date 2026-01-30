# kvrocks Namespace-Isolation Implementation Notes

## Locking-Architektur

### Dual-Level Locking System

```
Server
├── works_concurrency_rw_lock_ (GLOBAL)
│   └── Für Cluster-Ops, SCRIPT FLUSH, etc.
│
└── ns_locks_ (MAP: namespace → shared_mutex)
    ├── "__namespace" → shared_mutex
    ├── "tenant_a"    → shared_mutex
    └── "tenant_b"    → shared_mutex
```

### Lock-Typen und Wann Sie Verwendet Werden

| Lock-Typ | Methode | Wann verwendet | Beispiel-Commands |
|----------|---------|----------------|-------------------|
| **Global Shared** | `WorkConcurrencyGuard()` | Normale Commands ohne Namespace | - |
| **Global Exclusive** | `WorkExclusivityGuard()` | Server-weite exklusive Ops | SCRIPT FLUSH, Cluster-Ops |
| **Namespace Shared** | `WorkConcurrencyGuard(ns)` | Normale Commands mit Namespace | GET, SET, FCALL, BLPOP |
| **Namespace Exclusive** | `WorkExclusivityGuard(ns)` | Namespace-exklusive Ops | EXEC, FUNCTION FLUSH/DELETE |

### Lock-Flow für verschiedene Szenarien

**Szenario 1: Normaler GET/SET Command**
```
Client (tenant_a) → GET key
  └── WorkConcurrencyGuard("tenant_a")  // shared_lock auf ns_locks_["tenant_a"]
      └── Command ausführen
      └── Lock released
```

**Szenario 2: FCALL (Lua Function Call)**
```
Client (tenant_a) → FCALL myfunc 0
  └── WorkConcurrencyGuard("tenant_a")  // shared_lock
      └── Worker::Lua() → Funktion ausführen
      └── Lock released
```

**Szenario 3: FUNCTION FLUSH**
```
Client (tenant_a) → FUNCTION FLUSH
  └── WorkExclusivityGuard("tenant_a")  // exclusive_lock auf ns_locks_["tenant_a"]
      └── RocksDB: DeleteRange für tenant_a's Funktionen
      └── ScriptResetNamespace("tenant_a")
          └── für jeden Worker: LuaResetNamespace("tenant_a")
              └── Lösche nur Lua-Globals mit Prefix "tenant_a_"
      └── Lock released
```

**Szenario 4: Parallele FUNCTION FLUSH in verschiedenen Tenants**
```
Client A (tenant_a) → FUNCTION FLUSH    Client B (tenant_b) → FUNCTION FLUSH
  │                                       │
  └── exclusive_lock(ns_locks_["tenant_a"])   └── exclusive_lock(ns_locks_["tenant_b"])
      │                                           │
      │  ← Verschiedene Locks! →                  │
      │  ← Laufen parallel! →                     │
      │                                           │
      └── LuaResetNamespace("tenant_a")           └── LuaResetNamespace("tenant_b")
```

**Szenario 5: FCALL während FUNCTION FLUSH (gleicher Tenant)**
```
Time →
Worker 1: FCALL myfunc (tenant_a)
  └── shared_lock(tenant_a) ─────────[hält]─────────────→ fertig
                                                           │
Worker 2: FUNCTION FLUSH (tenant_a)                        │
  └── exclusive_lock(tenant_a) ───[wartet]─────────────────┴→ acquired → ausführen
```

### Lock-Hierarchie und Sicherheit

```
┌────────────────────────────────────────────────────────────────┐
│ REGEL: Wer exclusive_lock(ns) hält, hat exklusiven Zugriff    │
│        auf ALLE Daten und Lua-State dieses Namespaces.        │
└────────────────────────────────────────────────────────────────┘

Konsequenzen:
1. Kein FCALL kann laufen während FUNCTION FLUSH/DELETE läuft
2. Kein EXEC kann parallel zu anderem EXEC im gleichen Namespace
3. Verschiedene Namespaces blockieren sich NICHT gegenseitig
```

### Lock-Acquisition (redis_connection.cc:456-472)

```cpp
if (cmd_flags & kCmdExclusive) {
  if (!ns_.empty()) {
    exclusivity = srv_->WorkExclusivityGuard(ns_);  // Namespace-spezifisch
  } else {
    exclusivity = srv_->WorkExclusivityGuard();      // Global (Fallback)
  }
}
```

### Default Namespace

- `kDefaultNamespace = "__namespace"` (config.h:57)
- Wird automatisch gesetzt wenn kein Auth/Namespace konfiguriert
- Gilt als "gesetzter" Namespace → nutzt namespace-spezifischen Lock

---

## Namespace-Isolation Status

### Namespace-Aware (isoliert)

| Komponente | Key-Format | Code |
|------------|-----------|------|
| **EXEC/Transactions** | Per-NS `WriteBatchWithIndex` | storage.h:438-448 |
| **FUNCTION LOAD/LIST/DELETE/FLUSH** | `<prefix><ns_len><ns><name>` | storage.h:90-96, scripting.cc:645 |
| **FT.CREATE/DROPINDEX** | Namespace-spezifisch | cmd_search.cc |
| **Worker Locks** | `ns_locks_[namespace]` | server.cc:870-885 |

### NICHT Namespace-Aware (global)

| Komponente | Key-Format | Problem |
|------------|-----------|---------|
| **SCRIPT LOAD/EXISTS** | `lua_func_sha_<sha>` | Alle Tenants teilen Scripts |
| **SCRIPT FLUSH** | Löscht alle `lua_func_sha_*` | Betrifft alle Tenants |
| **Lua-State** | Shared über alle Worker | Ein Reset betrifft alle |

---

## Globale Locks (absichtlich)

| Operation | Datei:Zeile | Grund |
|-----------|-------------|-------|
| SCRIPT FLUSH | server.cc:1899 | SCRIPT ist nicht namespace-aware |
| SCRIPT FLUSH (propagated) | server.cc:1924 | SCRIPT ist nicht namespace-aware |
| FUNCTION DELETE/FLUSH (propagated) | server.cc:1930 | Kein Namespace in Propagation-Tokens (TODO) |
| Slot Migration | slot_migrate.cc:1039 | Cluster-Routing ist global |
| Slot Migrated | cluster.cc:289 | `migrated_slots_` ist global |

**Hinweis:** Direkte FUNCTION DELETE/FLUSH Commands sind jetzt namespace-aware.
Nur die propagierten Commands (Replikation) sind noch global, da das Propagation-Protokoll
keinen Namespace-Kontext mitschickt.

---

## Gefixte Bugs

### Blocking Commands (blocking_commander.h:79)

**Vorher:**
```cpp
auto concurrency = conn_->GetServer()->WorkConcurrencyGuard();  // GLOBAL!
```

**Nachher:**
```cpp
auto concurrency = conn_->GetServer()->WorkConcurrencyGuard(conn_->GetNamespace());
```

**Betroffene Commands:** BLPOP, BRPOP, BLMPOP, BLMOVE, BZPOPMIN, BZPOPMAX, BZMPOP

### FUNCTION FLUSH/DELETE Namespace-Isolation (scripting.cc)

**Problem 1:** FUNCTION FLUSH löschte ALLE Funktionen aller Namespaces in RocksDB.

**Problem 2:** `ScriptReset()` setzte ALLE Lua-States zurück (alle Namespaces betroffen).

**Lösung - RocksDB (scripting.cc:643-661):**
```cpp
const std::string &ns = conn->GetNamespace();
std::string start = ComposeFunctionKey(kLuaLibCodePrefix, ns, "");
std::string end = util::StringNext(start);
DeleteRange(cf, start, end);  // Löscht nur aktuellen Namespace
```

**Lösung - Lua-State Reset (neu implementiert):**
```cpp
// Vorher:
conn->GetServer()->ScriptReset();  // Resettet ALLE Namespaces!

// Nachher:
conn->GetServer()->ScriptResetNamespace(ns);  // Nur dieser Namespace
```

**Neue Methoden:**
- `Server::ScriptResetNamespace(ns)` - server.cc:1894-1897
- `Worker::LuaResetNamespace(ns)` - worker.cc:557-610

**LuaResetNamespace Implementierung:**
```cpp
void Worker::LuaResetNamespace(const std::string &ns) {
  lua_State *lua = Lua();
  std::string ns_prefix = ns + "_";

  // 1. REDIS_FUNCTION_LIBRARIES durchsuchen
  // 2. Alle Libraries mit Prefix <ns>_ sammeln
  // 3. Für jede Funktion: __redis_registered_<ns>_<func> löschen
  // 4. Library-Einträge aus REDIS_FUNCTION_LIBRARIES entfernen
}
```

### StringNext() Bug (string_util.cc:549-557)

**Problem:** Bei Strings die auf `\xFF` enden wurde das Byte nicht entfernt.

**Vorher:**
```cpp
std::string StringNext(std::string s) {
  for (auto iter = s.rbegin(); iter != s.rend(); ++iter) {
    if (*iter != char(0xff)) {
      (*iter)++;
      break;  // \xFF Bytes bleiben im String!
    }
  }
  return s;
}
```

**Nachher:**
```cpp
std::string StringNext(std::string s) {
  // Remove trailing 0xFF bytes first
  while (!s.empty() && static_cast<unsigned char>(s.back()) == 0xFF) {
    s.pop_back();
  }
  if (!s.empty()) {
    s.back()++;
  }
  return s;
}
```

**Wichtig für:** DeleteRange mit namespace-aware Keys (die \xFF im Namespace enthalten könnten).

### ns_locks_mutex_ Bottleneck (server.cc:870-909)

**Problem:** Jeder Command musste `unique_lock` auf `ns_locks_mutex_` acquiren.

**Lösung:** Read-Write Lock Pattern - `shared_lock` für existierende Namespaces (Fast-Path), `unique_lock` nur beim Erstellen neuer Namespaces (Slow-Path).

```cpp
// Fast-path: shared_lock for existing namespaces (common case)
{
  std::shared_lock map_lock(ns_locks_mutex_);
  auto it = ns_locks_.find(ns);
  if (it != ns_locks_.end()) {
    ns_lock = &it->second;
  }
}

// Slow-path: unique_lock only when namespace is new (rare)
if (!ns_lock) {
  std::unique_lock map_lock(ns_locks_mutex_);
  ns_lock = &ns_locks_[ns];
}
```

---

## Analyse: GetWriteBatchBase Observer-Pointer (Fragil aber sicher)

Das Design sieht fragil aus:
```cpp
ObserverOrUniquePtr<...> Storage::GetWriteBatchBase(const std::string &ns) {
  std::shared_lock lock(ns_txn_mutex_);
  // ... return batch.get() ...
}  // Lock released! Pointer könnte danach ungültig werden...
```

**Warum es trotzdem sicher ist:**
- EXEC hält `WorkExclusivityGuard(ns)` während der gesamten Transaktion
- Nur EXEC ruft BeginTxn/CommitTxn auf
- CommitTxn kann nicht parallel zu ExecuteCommands laufen
- Die Lock-Hierarchie schützt, nicht der Code selbst

**Konsequenz:** Kein Fix nötig, aber zukünftige Änderungen könnten das brechen.

---

## Race-Condition Analyse

### Warum LuaResetNamespace sicher ist

**Frage:** Was passiert wenn mehrere Worker gleichzeitig LuaResetNamespace aufrufen?

**Antwort:** Kein Problem, weil:
1. Jeder Worker hat seinen **eigenen** Lua-State (`lua_` Member in Worker)
2. `LuaResetNamespace(ns)` modifiziert nur den Lua-State des aufrufenden Workers
3. Es gibt kein Sharing von Lua-States zwischen Workern

```
Worker 1: LuaResetNamespace("tenant_a") → modifiziert Worker 1's lua_
Worker 2: LuaResetNamespace("tenant_a") → modifiziert Worker 2's lua_
Worker 3: LuaResetNamespace("tenant_a") → modifiziert Worker 3's lua_
         ↑ Alle unabhängig, keine Race Condition
```

### Warum FCALL + FUNCTION FLUSH sicher ist

**Frage:** Was wenn ein Worker FCALL ausführt während ein anderer FUNCTION FLUSH macht?

**Antwort:** Unmöglich durch Locking:

```
FCALL:          WorkConcurrencyGuard(ns)  = shared_lock(ns_locks_[ns])
FUNCTION FLUSH: WorkExclusivityGuard(ns)  = exclusive_lock(ns_locks_[ns])

shared_lock und exclusive_lock auf dem GLEICHEN Mutex sind inkompatibel.
→ FUNCTION FLUSH muss warten bis alle FCAlls fertig sind.
```

### Warum paralleles FUNCTION FLUSH in verschiedenen Tenants sicher ist

```
Tenant A FLUSH: exclusive_lock(ns_locks_["tenant_a"])
Tenant B FLUSH: exclusive_lock(ns_locks_["tenant_b"])
                ↑ Verschiedene Mutexe!

LuaResetNamespace("tenant_a"):
  - Löscht nur Globals mit Prefix "tenant_a_"
  - Berührt keine "tenant_b_" Globals

→ Vollständig isoliert, keine Interference.
```

---

## Potentielle Probleme

### Server-weite Commands nutzen Namespace-Lock

Commands wie FLUSHALL, SHUTDOWN, DEBUG, RDB, etc. haben das `exclusive` Flag, aber wenn der Client einen Namespace hat, bekommen sie nur den Namespace-Lock statt den globalen.

**Betrifft:**
- FLUSHALL (sollte alle Namespaces blockieren)
- SHUTDOWN (sollte Server-weit sein)
- DEBUG, RDB, SST, SLAVEOF, FLUSHMEMTABLE, FLUSHBLOCKCACHE

**Mögliche Lösung:** Neues Flag `kCmdGlobalExclusive` oder explizite Prüfung in redis_connection.cc

---

## SCRIPT vs FUNCTION Vergleich

| Feature | SCRIPT | FUNCTION |
|---------|--------|----------|
| Storage Key | `lua_func_sha_<sha>` | `lua_lib_code_<ns_len><ns><name>` |
| Namespace-Aware | Nein | Ja |
| Tenant-Isolation | Nein | Ja |
| FLUSH Scope | Global | Namespace-spezifisch (nach Fix) |
| Lua-Globals | `lua_func_sha_*` | `__redis_registered_<ns>_<func>` |

---

## Relevante Code-Pfade

| Komponente | Datei |
|------------|-------|
| Lock Guards | src/server/server.cc:862-885 |
| Lock Acquisition | src/server/redis_connection.cc:456-472 |
| Namespace Transaction State | src/storage/storage.h:438-448 |
| Function Key Composition | src/storage/storage.h:90-96 |
| Script Storage (global) | src/server/server.cc:1815-1830 |
| Function Storage (ns-aware) | src/server/server.cc:1832-1864 |
| Blocking Commands | src/commands/blocking_commander.h:71-106 |
| Default Namespace | src/config/config.h:57 |
| ScriptResetNamespace | src/server/server.cc:1894-1897 |
| LuaResetNamespace | src/server/worker.cc:557-610 |
| StringNext (fixed) | src/common/string_util.cc:549-559 |
| FunctionFlush (ns-aware) | src/storage/scripting.cc:643-661 |
| FunctionDelete (ns-aware) | src/storage/scripting.cc:596-642 |