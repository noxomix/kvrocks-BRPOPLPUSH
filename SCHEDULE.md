a# Phase 3: SNI-basiertes Fair Scheduling

## Konzept

Faires Worker-Scheduling basierend auf SNI (Server Name Indication). Jeder unique SNI bekommt einen fairen Anteil der Worker. Konfigurierbare Thresholds für Konzentration, Fair Share und Überziehung.

## Kernprinzipien

1. **SNI = Scheduling-Einheit** (nicht Namespace/Tenant)
   - SNI-Hostname direkt = Scheduling-Gruppe
   - Keine Verknüpfung mit AUTH/Namespace nötig
   - AUTH-basierte SNI-Validierung (Hopping Prevention) = separate Aufgabe

2. **Fair Share Algorithmus**
   ```
   fair_share = total_workers / active_snis
   ```
   - 1 SNI aktiv → alle Worker
   - 2 SNIs aktiv → je 50%
   - N SNIs aktiv → je 1/N (minimum konfigurierbar)

3. **Konzentration bei niedriger Last**
   - Wenige Connections → wenige Worker (reduziert Context Switches)
   - Viele Connections → mehr Worker (Parallelität)
   - Threshold konfigurierbar: `sni-connections-per-worker`

4. **Überziehung (Overdraft)**
   - Im Burst darf ein SNI über Fair Share hinaus Worker nutzen
   - Prozentual konfigurierbar: `sni-overdraft-percent`

5. **Lua-Blocking Isolation**
   - Blockierte Worker werden übersprungen
   - Neue Connections desselben SNI → andere eigene Worker
   - Fallback wenn alle blockiert (besser als reject)

---

## Konfiguration

```ini
# === Fair Scheduling (kvrocks.conf) ===

# Connections pro aktivem Worker bevor auf mehr Worker expandiert wird
# Höher = mehr Konzentration, weniger Context Switches
# Niedriger = mehr Parallelität
# Default: 10
sni-connections-per-worker 10

# Minimum Worker pro SNI (auch bei vielen Tenants)
# Default: 2
sni-min-workers 2

# Maximum Worker pro SNI in Prozent (0 = auto = workers/active_snis)
# Default: 0
sni-max-workers-percent 0

# Überziehung: Wie viel darf ein Tenant über Fair Share im Burst (Prozent)
# 30 = Tenant kann bis 130% von fair_share nutzen
# 0 = Strikt, keine Überziehung
# Default: 30
sni-overdraft-percent 30
```

### Beispiel-Szenarien

**Kleine Installation (8 Worker, 2 Tenants):**
```ini
sni-connections-per-worker 20
sni-min-workers 2
sni-max-workers-percent 0      # Auto: 4 pro Tenant
sni-overdraft-percent 50       # Kann bis 6 Worker nutzen
```

**Große Installation (256 Worker, 10 Tenants):**
```ini
sni-connections-per-worker 10
sni-min-workers 4
sni-max-workers-percent 0      # Auto: 25 pro Tenant
sni-overdraft-percent 30       # Kann bis 32 Worker nutzen
```

---

## Architektur

```
┌──────────────────────────────────────────────────────────────┐
│                      Acceptor Thread                          │
│                                                               │
│  accept() ──► ExtractSNI() ──► FairScheduler.SelectWorker()  │
│                                        │                      │
│              ┌─────────────────────────┼──────────────────┐  │
│              ▼                         ▼                  ▼  │
│         [Worker 0]              [Worker 1]          [Worker N]│
│          SNI: a.io               SNI: b.io           SNI: ...│
└──────────────────────────────────────────────────────────────┘
```

---

## Datenstrukturen

```cpp
struct FairSchedulerConfig {
  uint32_t connections_per_worker = 10;   // Konzentration
  uint32_t min_workers = 2;               // Minimum pro SNI
  uint32_t max_workers_percent = 0;       // 0 = auto
  uint32_t overdraft_percent = 30;        // Überziehung
};

struct SNIState {
  std::string sni;
  std::atomic<uint32_t> active_connections{0};
  std::atomic<uint32_t> next_worker_idx{0};     // Lock-free round-robin
  std::vector<uint32_t> preferred_workers;       // Worker-Affinität
  std::atomic<uint64_t> last_activity_ms{0};
  mutable std::mutex mu;                         // Nur für preferred_workers
};
```

---

## Algorithmen

### GetTargetWorkerCount() - Wie viele Worker soll SNI nutzen?

```cpp
uint32_t SNIState::GetTargetWorkerCount(const FairSchedulerConfig& cfg) {
  uint32_t conns = active_connections.load();
  uint32_t total_workers = scheduler->GetTotalWorkers();
  uint32_t active_snis = scheduler->GetActiveSNICount();

  // 1. Fair Share Basis
  uint32_t fair_share = total_workers / std::max(1u, active_snis);

  // 2. Max Workers (Config oder Fair Share)
  uint32_t max_workers = (cfg.max_workers_percent > 0)
    ? (total_workers * cfg.max_workers_percent / 100)
    : fair_share;

  // 3. Overdraft erlauben
  uint32_t max_with_overdraft = fair_share + (fair_share * cfg.overdraft_percent / 100);
  max_workers = std::min(max_workers, max_with_overdraft);

  // 4. Konzentration: Wie viele Worker brauchen wir für aktuelle Last?
  uint32_t needed_for_load = (conns + cfg.connections_per_worker - 1)
                            / cfg.connections_per_worker;

  // 5. Clamp zwischen min und max
  return std::clamp(needed_for_load, cfg.min_workers, max_workers);
}
```

### SelectWorker() - Worker für neue Connection wählen

```cpp
std::shared_ptr<Worker> FairScheduler::SelectWorker(const std::string& sni) {
  SNIState* state = GetOrCreateState(sni);  // shared_lock für lookup
  state->active_connections.fetch_add(1, std::memory_order_relaxed);

  // Wie viele Worker soll dieser SNI nutzen?
  uint32_t target_count = state->GetTargetWorkerCount(config_);

  // Round-Robin nur über target_count Worker (nicht alle preferred)
  uint32_t idx = state->next_worker_idx.fetch_add(1, std::memory_order_relaxed);

  // Versuche aus preferred_workers (ohne Lua-Block)
  for (uint32_t i = 0; i < target_count; i++) {
    uint32_t worker_idx = (idx + i) % target_count;
    if (worker_idx < state->preferred_workers.size()) {
      uint32_t worker_id = state->preferred_workers[worker_idx];
      auto worker = GetWorker(worker_id);
      if (worker->IsAccepting() && !worker->IsLuaScriptRunning()) {
        return worker;
      }
    }
  }

  // Alle target Worker Lua-blockiert → erweitere auf mehr Worker
  return SelectWithOverdraft(state, target_count);
}

std::shared_ptr<Worker> FairScheduler::SelectWithOverdraft(SNIState* state, uint32_t current) {
  // Versuche zusätzliche Worker aus preferred_workers
  for (uint32_t i = current; i < state->preferred_workers.size(); i++) {
    auto worker = GetWorker(state->preferred_workers[i]);
    if (worker->IsAccepting() && !worker->IsLuaScriptRunning()) {
      return worker;
    }
  }

  // Letzter Fallback: irgendein akzeptierender Worker
  return SelectAnyAccepting();
}
```

### OnConnectionClosed() - Counter decrementieren

```cpp
void FairScheduler::OnConnectionClosed(const std::string& sni) {
  std::shared_lock lock(sni_states_mu_);
  auto it = sni_states_.find(sni);
  if (it != sni_states_.end()) {
    it->second->active_connections.fetch_sub(1, std::memory_order_relaxed);
  }
}
```

---

## Thread-Safety

| Operation | Lock-Art | Häufigkeit | Latenz |
|-----------|----------|------------|--------|
| SNI Lookup | `shared_lock` | Jede Connection | ~50ns |
| SNI Insert | `unique_lock` | Erster Connect pro SNI | ~100ns |
| active_connections++ | Atomic | Jede Connection | ~10ns |
| active_connections-- | Atomic | Jede Connection | ~10ns |
| preferred_workers Update | Per-SNI Mutex | Selten | ~100ns |

**Kein Bottleneck:**
- `shared_lock` = parallele Reads (N Acceptors gleichzeitig)
- `unique_lock` nur bei neuem SNI (einmalig pro Tenant)
- Counter-Updates sind lock-free Atomics
- Tenants blockieren sich nicht gegenseitig (per-SNI Mutex)

---

## Beispiel: 256 Worker, 4 SNIs

```
Config:
  connections_per_worker = 10
  min_workers = 2
  overdraft_percent = 30

Fair Share = 256 / 4 = 64 Worker pro SNI
Max mit Overdraft = 64 + 30% = 83 Worker

SNI "a.io" hat 50 Connections:
  → needed = ceil(50/10) = 5 Worker
  → clamp(5, 2, 83) = 5 Worker aktiv
  → Round-Robin auf Worker [0..4]

SNI "b.io" hat 800 Connections (Burst):
  → needed = ceil(800/10) = 80 Worker
  → clamp(80, 2, 83) = 80 Worker (nutzt Overdraft)
  → Round-Robin auf Worker [64..143]

SNI "c.io" hat 5 Connections:
  → needed = ceil(5/10) = 1 Worker
  → clamp(1, 2, 83) = 2 Worker (Minimum)
  → Round-Robin auf Worker [144..145]

SNI "d.io" hat 3 Connections, Worker 192 hat Lua-Block:
  → target = 2 Worker [192, 193]
  → Worker 192 übersprungen (Lua)
  → Connection landet auf Worker 193
```

---

## Neue Dateien

| Datei | Zweck |
|-------|-------|
| `src/server/fair_scheduler.h` | FairScheduler, SNIState, Config |
| `src/server/fair_scheduler.cc` | Implementierung |
| `src/server/tls_util.h` | ExtractSNI() Deklaration |
| `src/server/tls_util.cc` | TLS ClientHello Parser |

## Geänderte Dateien

| Datei | Änderung |
|-------|----------|
| `src/config/config.h` | `FairSchedulerConfig` Felder |
| `src/config/config.cc` | Config Parsing |
| `src/server/server.h` | `unique_ptr<FairScheduler>` Member |
| `src/server/server.cc` | FairScheduler init, SelectWorker(sni) |
| `src/server/worker.h` | `PendingConnection.sni` aktivieren |
| `src/server/worker.cc` | SNI an Connection durchreichen |
| `src/server/redis_connection.h` | `sni_` Member |
| `src/server/redis_connection.cc` | `OnClose()` → FairScheduler benachrichtigen |

---

## SNI-Extraktion

```cpp
// MSG_PEEK liest TLS ClientHello ohne Buffer zu leeren
std::string ExtractSNI(int fd) {
  char buf[1024];
  ssize_t n = recv(fd, buf, sizeof(buf), MSG_PEEK);
  // Parse TLS Record Header + Handshake + Extensions
  return ParseSNIFromClientHello(buf, n);
}

// Non-TLS Fallback: Peer IP als Scheduling-Key
std::string GetSchedulingKey(int fd, bool is_tls) {
  if (is_tls) {
    std::string sni = ExtractSNI(fd);
    if (!sni.empty()) return sni;
  }
  return GetPeerIP(fd);
}
```

---

## Wichtige Implementierungsdetails

### SelectAnyAccepting() modifiziert NICHT preferred_workers

```cpp
// KORREKT: Fallback ohne Seiteneffekt
std::shared_ptr<Worker> SelectAnyAccepting() {
  for (auto& worker : GetAllWorkers()) {
    if (worker->IsAccepting() && !worker->IsLuaScriptRunning()) {
      return worker;  // Einmalige Nutzung, KEIN Hinzufügen zu preferred_workers
    }
  }
  return nullptr;
}

// FALSCH wäre:
// state->preferred_workers.push_back(worker->GetID());  // ← NIE im Fallback!
```

**Warum wichtig:** Verhindert "Worker-Leak" zu fremden SNIs. Geliehene Worker werden nur für eine Connection genutzt, nicht dauerhaft übernommen.

### GetActiveSNICount() zählt nur SNIs mit Connections

```cpp
uint32_t GetActiveSNICount() {
  uint32_t count = 0;
  std::shared_lock lock(sni_states_mu_);
  for (const auto& [_, state] : sni_states_) {
    if (state->active_connections.load() > 0) {  // ← Wichtig!
      count++;
    }
  }
  return count;
}
```

**Warum wichtig:** SNIs ohne aktive Connections zählen nicht zum Fair Share Divisor. Alte/tote SNIs beeinflussen das Scheduling nicht.

### Optional: Cleanup-Cron für tote SNIs

```cpp
// Alle 60 Sekunden im Server-Cron
void FairScheduler::CleanupInactiveSNIs() {
  uint64_t now = GetCurrentTimeMs();
  std::unique_lock lock(sni_states_mu_);

  for (auto it = sni_states_.begin(); it != sni_states_.end();) {
    auto& state = it->second;
    bool is_dead = state->active_connections == 0 &&
                   (now - state->last_activity_ms) > 300000;  // 5 Min
    if (is_dead) {
      it = sni_states_.erase(it);
    } else {
      ++it;
    }
  }
}
```

**Warum:** Verhindert unbegrenztes Wachstum der SNI-Map bei vielen einmaligen SNIs.

---

## Known Limitations (Akzeptiert)

### 1. Nur neue Connections werden fair verteilt

Bestehende lang-lebige Connections bleiben auf ihren Workern. Das ist ein fundamentaler Trade-off:
- Connection Migration ist zu komplex (bufferevent tied to event_base)
- In der Praxis: Connection Pooling hat natürlichen Turnover
- Reconnects balancieren das System über Zeit

### 2. Fairness basiert auf Connection-Zahl, nicht auf Last

Wenige aber sehr aktive/pipelined Clients können mehr CPU nutzen als ihr Fair Share.

**Warum nicht gefixt:**
- SNI ist nur im Acceptor bekannt, Commands werden nach AUTH im Worker ausgeführt
- Verknüpfung SNI → Command-Rate erfordert doppeltes Tracking
- YAGNI: Pipelined Heavy-Clients sind selten

**Mögliche Erweiterung (Phase 4+):** Wenn SNI = Namespace validiert wird, können Namespace-Stats für Last-basiertes Scheduling genutzt werden.

---

## Nicht im Scope (Phase 4+)

- AUTH-basierte SNI-Validierung (SNI-Hopping Prevention)
- Last-basiertes Scheduling (commands/sec statt connections)
- Per-SNI Rate Limiting
- SNI→Namespace Config-Mapping
- Dynamisches Worker Start/Stop