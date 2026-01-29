# kvrocks Worker-Architektur

## IO-Framework

- **libevent** mit epoll (Linux) / kqueue (macOS)
- **KEIN io_uring**

## Verbindungsverteilung

```
Kernel (SO_REUSEPORT) → verteilt Connections auf Worker
```

- Jeder Worker bindet separat auf denselben Port mit `SO_REUSEPORT`
- Kernel macht Load-Balancing, keine Anwendungslogik
- Code: `src/server/worker.cc:268`

## Worker-Struktur

```
1 Worker = 1 OS-Thread = 1 event_base (libevent) = N Connections
```

- Eine Connection bleibt **permanent** bei ihrem Worker
- Ausnahme: Migration bei `CONFIG SET workers N` (Worker-Reduktion)
- Code: `src/server/worker.cc:337-355`

## Command-Abarbeitung pro Worker

```
Event Loop (epoll_wait)
    ↓
Connection hat Daten → OnRead()
    ↓
Parse Commands
    ↓
FOR EACH Command:
    Lock(key_mutex)
    Execute()
    db_->Write()  ← optional fsync hier
    Unlock(key_mutex)
    Reply()
    ↓
Nächste Connection
```

**Sequentiell** - ein Worker kann nur 1 Command zur Zeit ausführen.

Code: `src/server/redis_connection.cc:390-602`

## Lock-System (2 Ebenen)

### Ebene 1: WorkConcurrencyGuard

```cpp
std::shared_mutex works_concurrency_rw_lock_;
```

- Normale Commands: `shared_lock` (parallel erlaubt)
- Exklusive Commands (EXEC, CONFIG SET): `unique_lock` (blockiert alle)
- Code: `src/server/server.cc:862-868`

### Ebene 2: Per-Key Locks

```cpp
LockManager lock_mgr_(16);  // 2^16 = 65536 Mutex-Partitionen
```

- Hash(key) → Mutex-Index
- Gleicher Key = gleicher Mutex = Serialisierung
- Deadlock-Prevention: Locks werden sortiert akquiriert
- Code: `src/common/lock_manager.h:32-141`

## fsync-Verhalten

**Voraussetzung: `rocksdb.write_options.sync=true` ist Pflicht. Non-durable mode kommt nicht in Frage.**

```
Worker-Thread ruft db_->Write(sync=true)
    ↓
RocksDB fsync() ← BLOCKIERT im selben Thread
    ↓
ALLE Connections in diesem Worker warten
```

- Kein separater fsync-Thread
- fsync blockiert den gesamten Worker
- Code: `src/storage/storage.cc:114-120`, `src/storage/storage.cc:744`

## RocksDB Group Commit

```
Worker 0 → db_->Write() ──┐
Worker 1 → db_->Write() ──┼→ RocksDB gruppiert → 1x fsync
Worker 2 → db_->Write() ──┘
```

- Group Commit entsteht durch **mehrere Worker** die gleichzeitig schreiben
- 1 Worker kann nur 1 Write zur Zeit (single-threaded)
- Mehr Worker + mehr Last = größere Groups = weniger fsyncs/Write

## Parallelität

| Szenario | Verhalten |
|----------|-----------|
| 1 Client, viele Commands | Sequentiell (1 Worker) |
| N Clients, gleicher Key | Serialisiert (Per-Key Mutex) |
| N Clients, verschiedene Keys | Parallel (verschiedene Mutexe) |
| N Clients, verschiedene Worker | Parallel + Group Commit möglich |

## Relevante Code-Pfade

| Komponente | Datei |
|------------|-------|
| Worker-Erstellung | `src/server/server.cc:103-116` |
| Worker Event-Loop | `src/server/worker.cc:314-320` |
| SO_REUSEPORT | `src/server/worker.cc:268-270` |
| Connection-Handling | `src/server/worker.cc:114-194` |
| Command-Execution | `src/server/redis_connection.cc:390-602` |
| LockManager | `src/common/lock_manager.h` |
| WriteOptions/fsync | `src/storage/storage.cc:114-120` |
| RocksDB Write | `src/storage/storage.cc:744` |