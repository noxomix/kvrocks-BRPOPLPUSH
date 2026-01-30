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
| **FUNCTION LOAD/LIST/DELETE** | `<prefix><ns_len><ns><name>` | storage.h:90-96 |
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
| SCRIPT FLUSH (propagated) | server.cc:1901 | Lua-State ist global |
| FUNCTION DELETE/FLUSH (propagated) | server.cc:1907 | Lua-State ist global |
| Slot Migration | slot_migrate.cc:1039 | Cluster-Routing ist global |
| Slot Migrated | cluster.cc:289 | `migrated_slots_` ist global |

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
| FLUSH Scope | Global | Global (Lua-State) |

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