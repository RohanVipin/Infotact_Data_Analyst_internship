# AtmoSync Task A: Operational Runbook and Administrator Guide
**Document Version:** 1.0.0  
**Status:** Approved Operational Guide  
**Scope:** Task A — IoT Sensor Simulator, Event Engineering, Unified Runner, and Kafka Producer  
**System:** AtmoSync — Micro-Climate Arbitrage Analytics  
**Target Repository:** `Infotact_Data_Analyst_internship`  
**Author:** Siddarth (Lead Data Engineer — Task A)  

---

## 1. Purpose and Scope

### 1.1 Operational Scope
This document provides complete, practical instructions for engineers, evaluators, and system operators to prepare, configure, execute, monitor, test, and troubleshoot **Task A** within the AtmoSync platform.

**Task A** encapsulates the upstream edge simulation and streaming layer:
- Multi-container IoT sensor telemetry generation (temperature, humidity, vibration).
- Version-1 canonical schema validation and deterministic JSON serialization.
- Local JSON Lines (JSONL) persistence.
- Unified pipeline orchestration via `SimulationRunner`.
- High-throughput streaming via an idempotent Apache Kafka producer.
- Command-line interface with cross-platform OS signal management (`SIGINT`, `SIGTERM`, Windows `SIGBREAK`).

### 1.2 Boundary Notice
- **Covered:** Local execution of the Python simulation engine, file logging, Docker-based KRaft single-node Kafka infrastructure, CLI operational commands, live verification suites, and broker troubleshooting.
- **Explicitly Excluded:** Downstream Snowflake data warehouse provisioning, Snowpipe streaming setup (Task B), dbt transformation models (Task C), and Apache Superset BI dashboards (Task D).

---

## 2. System Prerequisites

### 2.1 Hardware and Operating System
- **Supported Operating Systems:**
  - Windows 10 / 11 (64-bit AMD64) — Primary tested environment (PowerShell 5.1 / 7+).
  - Linux (Ubuntu 20.04+, Debian 11+, RHEL 8+, or similar 64-bit distributions).
  - macOS (macOS 12 Monterey or newer).
- **CPU & Memory:** Minimum 2 CPU cores, 4 GB available system RAM (8 GB recommended when running Docker Kafka alongside the simulation).
- **Disk Storage:**
  - ~1.5 GB for Docker container image (`confluentinc/cp-kafka:7.6.0` is ~1.1 GB uncompressed).
  - ~150 MB for the Python virtual environment and dependencies.
  - Variable storage for telemetry output: ~200 bytes per event (at default 1.0s interval for 3 containers, dual-sink mode generates ~1.8 KB/sec or ~6.5 MB/hour).

### 2.2 Software Requirements
- **Python Version:** **Python 3.10+** is required.
  - Verified and tested on **Python 3.13.14** (64-bit).
  - Confirm Python installation:
    ```powershell
    python --version
    # or on Windows with py launcher:
    py -3.13 --version
    ```
- **Container Runtime:** Docker Desktop (Windows / macOS) or Docker Engine with Docker Compose v2 (Linux).
  - Docker Compose version: `v2.20.0+` recommended (`docker compose` syntax).
  - Confirm Docker installation:
    ```powershell
    docker --version
    docker compose version
    ```

### 2.3 Required Network Ports

| Port | Protocol | Scope | Direction | Purpose |
| :---: | :---: | :---: | :---: | :--- |
| **9092** | TCP | Localhost | Inbound/Outbound | Kafka External Host Listener (`PLAINTEXT_HOST://localhost:9092`). Required for the Python producer and external consumers. |
| **29092**| TCP | Docker Internal | Internal Container | Kafka Internal Broker Listener (`PLAINTEXT://127.0.0.1:29092`). |
| **29093**| TCP | Docker Internal | Internal Container | KRaft Controller Quorum Voter Listener (`CONTROLLER://127.0.0.1:29093`). |

> [!IMPORTANT]
> Port `9092` must be available on the host machine before starting Kafka. If another service (such as an existing Kafka, ZooKeeper, or local application) is bound to `9092`, the container will fail to start.

---

## 3. Repository Setup

### 3.1 Clone and Branch Checkout
Clone the official internship repository and check out the verified Task A feature branch:

```powershell
# Clone the repository
git clone https://github.com/RohanVipin/Infotact_Data_Analyst_internship.git

# Navigate into the project root directory
cd Infotact_Data_Analyst_internship

# Checkout the active feature branch
git checkout feature/siddarth-simulation-event-engineering

# Verify current branch and latest commit
git branch --show-current
git log -1 --oneline
```

### 3.2 Directory Structure Overview
```text
Infotact_Data_Analyst_internship/
├── docker-compose.kafka.yml      # Root-level KRaft Kafka Docker Compose file
├── requirements.txt              # Core production runtime dependencies
├── requirements-dev.txt          # Development, testing, and coverage dependencies
├── pytest.ini                    # Test runner configuration and custom markers
├── data/                         # Local persistence output directory
│   ├── .gitkeep                  # Preserves directory in version control
│   └── verification/             # Preserved live integration test artifacts
├── docs/                         # Authoritative documentation suite
│   ├── DATA_CONTRACT.md          # Telemetry event contract specification
│   ├── ARCHITECTURE.md           # System architecture & lifecycle manual
│   └── OPERATIONS_GUIDE.md       # This operational runbook
├── simulation/                   # Core Python simulation package
└── tests/                        # Automated unit and integration test suite
```

---

## 4. Python Environment Setup

### 4.1 Create Virtual Environment
Isolate project dependencies by creating a dedicated Python virtual environment:

**On Windows (PowerShell):**
```powershell
# Using the Python 3.13 launcher (recommended)
py -3.13 -m venv .venv

# Or using the standard python executable
python -m venv .venv
```

**On Linux / macOS (Bash/Zsh):**
```bash
python3 -m venv .venv
```

### 4.2 Activate Virtual Environment

**On Windows (PowerShell):**
```powershell
# Allow script execution if restricted
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process

# Activate environment
.\.venv\Scripts\Activate.ps1
```

**On Windows (Command Prompt):**
```cmd
.\.venv\Scripts\activate.bat
```

**On Linux / macOS:**
```bash
source .venv/bin/activate
```

### 4.3 Install Dependencies
Install production runtime and testing dependencies using `pip`:

```powershell
# Upgrade pip to latest version
python -m pip install --upgrade pip

# Install production dependencies (PyYAML, confluent-kafka)
pip install -r requirements.txt

# Install development and testing dependencies (pytest, pytest-cov)
pip install -r requirements-dev.txt
```

### 4.4 Verify Installation
Confirm that the core packages load cleanly without circular import or C-extension issues:

```powershell
python -c "import yaml, confluent_kafka, pytest; print('Environment initialized successfully! confluent-kafka version:', confluent_kafka.__version__)"
```

---

## 5. Kafka Infrastructure Setup

When running in dual-sink (`both`) or streaming-only (`kafka`) mode, a local Apache Kafka broker must be running and accessible on `localhost:9092`.

### 5.1 Architecture: Single-Node KRaft Broker
Task A utilizes a single-node Apache Kafka cluster operating in **KRaft mode** (Kafka Raft Metadata mode, eliminating Apache ZooKeeper). The configuration is maintained in `docker-compose.kafka.yml` at the project root.

#### The KRaft Loopback DNS Remediation
On Docker's default bridge network, container names (`kafka`) do not resolve reliably via internal DNS. To prevent controller heartbeat timeouts, cluster partition freezes, and broker fencing, the configuration sets `KAFKA_CONTROLLER_QUORUM_VOTERS: '1@127.0.0.1:29093'` and binds `extra_hosts: ["kafka:127.0.0.1"]`. This guarantees stable, instantaneous controller quorum election.

### 5.2 Starting Kafka
Ensure Docker Desktop or the Docker daemon is running, then launch the Kafka container:

```powershell
# Launch the container in detached background mode
docker compose -f docker-compose.kafka.yml up -d
```

### 5.3 Verifying Broker Health
Verify that the container is up and that the Kafka broker has registered with the KRaft controller:

```powershell
# 1. Check container status
docker compose -f docker-compose.kafka.yml ps

# Expected output:
# NAME             IMAGE                          COMMAND                  SERVICE   CREATED          STATUS          PORTS
# atmosync-kafka   confluentinc/cp-kafka:7.6.0   "/etc/confluent/dock…"   kafka     10 seconds ago   Up 10 seconds   0.0.0.0:9092->9092/tcp

# 2. Inspect broker startup logs for the operational ready signal
docker compose -f docker-compose.kafka.yml logs kafka | Select-String "Kafka Server started"

# 3. Test TCP port accessibility from the host machine
Test-NetConnection -ComputerName localhost -Port 9092
# Expected: TcpTestSucceeded : True
```

### 5.4 Stopping Kafka
To stop the broker during normal teardown without deleting message data:

```powershell
# Gracefully stop the Kafka container
docker compose -f docker-compose.kafka.yml stop
```

To stop and remove the container:
```powershell
docker compose -f docker-compose.kafka.yml down
```

---

## 6. Configuration Management

### 6.1 YAML Configuration (`simulation/config/simulation_config.yaml`)
The simulation engine is configured via a human-readable YAML file:

```yaml
simulation:
  # Interval in seconds between consecutive simulation cycles (must be > 0.0)
  interval_seconds: 1.0

  # Master PRNG seed for reproducible test runs (integer or null)
  random_seed: null

containers:
  # List of unique shipping container identifiers
  - "CONTAINER_001"
  - "CONTAINER_002"
  - "CONTAINER_003"

sensors:
  temperature:
    enabled: true
    unit: "celsius"          # Provisional unit
    min_value: null          # Unconstrained baseline
    max_value: null
  humidity:
    enabled: true
    unit: "percent"          # Provisional unit (% RH)
    min_value: null
    max_value: null
  vibration:
    enabled: true
    unit: "m/s^2"            # Provisional unit
    min_value: null
    max_value: null
```

### 6.2 Environment Variable Overrides
The Kafka client respects standard environment variables, which can be set in the shell to override defaults:

| Setting | Environment Variable | Default Value | Description |
| :--- | :--- | :--- | :--- |
| **Broker Address** | `ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated list of broker `host:port` pairs. |
| **Topic Name** | `ATMOSYNC_KAFKA_TOPIC` | `atmosync.telemetry.v1` | Target Kafka topic for published events. |
| **Client ID** | `ATMOSYNC_KAFKA_CLIENT_ID` | `atmosync-sensor-producer` | Client identifier string sent to Kafka. |
| **Acks** | `ATMOSYNC_KAFKA_ACKS` | `all` | Acknowledgement durability level (`all`, `-1`, `1`, `0`). |
| **Security Protocol**| `ATMOSYNC_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Transport protocol (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`). |
| **SASL Username** | `ATMOSYNC_KAFKA_SASL_USERNAME` | `None` | Authentication username for SASL mechanisms. |
| **SASL Password** | `ATMOSYNC_KAFKA_SASL_PASSWORD` | `None` | Authentication password (masked in string representations). |

CLI arguments (e.g. `--bootstrap-servers`, `--topic`) strictly take precedence over environment variables.

---

## 7. Running the Simulator

The entrypoint for running the simulator is `python -m simulation` (or `python simulation/cli.py`).

### 7.1 CLI Synopsis & Arguments
```text
python -m simulation [-h] [-v] [-c CONFIG] [-n CYCLES] [-m {both,jsonl,kafka}]
                     [--jsonl-path JSONL_PATH] [--bootstrap-servers BOOTSTRAP_SERVERS]
                     [--topic TOPIC] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
```

| Flag | Long Argument | Type | Default | Operational Description |
| :--- | :--- | :---: | :--- | :--- |
| `-h` | `--help` | flag | — | Displays the command-line usage manual and exits. |
| `-v` | `--version` | flag | — | Displays the application version number (`0.1.0`) and exits. |
| `-c` | `--config` | `str` | `simulation/config/simulation_config.yaml` | Filesystem path to the YAML configuration file. |
| `-n` | `--cycles` | `int` | `None` | Number of simulation cycles to execute ($N \ge 1$). If omitted, runs continuously. |
| `-m` | `--output-mode` | `str` | `both` | Output destination mode: `both` (JSONL + Kafka), `jsonl`, or `kafka`. |
| — | `--jsonl-path` | `str` | `data/telemetry.jsonl` | Filesystem destination path for line-delimited JSON output. |
| — | `--bootstrap-servers`| `str`| `None` | Kafka broker list (overrides `ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS`). |
| — | `--topic` | `str` | `None` | Kafka topic name (overrides `ATMOSYNC_KAFKA_TOPIC`). |
| `-l` | `--log-level` | `str` | `INFO` | Console logging threshold (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`). |

---

### 7.2 Verified Execution Scenarios

#### Scenario A: Quick Local Smoke Test (JSONL Only, Zero Docker Required)
Executes 5 simulation cycles persisting exclusively to local disk. Does not require Docker, Kafka, or network connectivity.
```powershell
python -m simulation -n 5 -m jsonl
```
- **Infrastructure Needed:** None.
- **Output:** Writes 15 validated events ($5 \times 3$ containers) to `data/telemetry.jsonl`.
- **Expected Exit Code:** `0`.

#### Scenario B: Custom Output File Path
Runs 10 cycles and writes to a user-specified file:
```powershell
python -m simulation -n 10 -m jsonl --jsonl-path data/custom_run.jsonl
```
- **Infrastructure Needed:** None.
- **Output:** Parent directories are created automatically; writes 30 events to `data/custom_run.jsonl`.

#### Scenario C: Default Dual-Sink Verification (Local File + Live Kafka)
Produces 2 cycles simultaneously to both `data/telemetry.jsonl` and local Kafka broker:
```powershell
python -m simulation -n 2 -m both
```
- **Infrastructure Needed:** Local Kafka broker running on `localhost:9092` (`docker compose -f docker-compose.kafka.yml up -d`).
- **Output:**
  - 18 events appended to `data/telemetry.jsonl`.
  - 18 messages published to topic `atmosync.telemetry.v1` keyed by `container_id`.
- **Expected Exit Code:** `0`.

#### Scenario D: Kafka Streaming Only with Custom Topic
Publishes 50 cycles directly to a specific topic on a remote or custom broker:
```powershell
python -m simulation -n 50 -m kafka --bootstrap-servers localhost:9092 --topic custom.telemetry.v1 -l DEBUG
```
- **Infrastructure Needed:** Kafka broker reachable at specified address.
- **Output:** Telemetry streamed directly to Kafka. No local JSONL file is created.

#### Scenario E: Continuous Streaming Mode (Long-Running Simulation)
Runs indefinitely at the interval configured in YAML (e.g. 1 cycle/second) until interrupted:
```powershell
python -m simulation -m both
```
- **Infrastructure Needed:** Local Kafka broker.
- **Halt Execution:** Press `Ctrl+C` in the terminal to trigger graceful shutdown.

---

## 8. Runtime Monitoring and Metrics

### 8.1 Structured Execution Summary
Upon normal completion, error abort, or signal interruption, the CLI prints an aligned execution summary to standard output:

```text
============================================================
          AtmoSync Simulation Pipeline Summary
============================================================
Status:                      COMPLETED
Wall-Clock Duration:         2.05s
Simulation Cycles Completed: 2
Simulation Cycles Failed:    0
Events Generated:            18
Sink Dispatches Attempted:   18
Sink Dispatches Succeeded:   18
Sink Dispatches Failed:      0
Kafka Confirmed Receipts:    18
Kafka Failed Deliveries:     0
Kafka In-Flight Pending:     0
============================================================
```

### 8.2 Understanding Metric Semantics
- **Events Generated:** Total sensor readings produced by virtual sensors.
- **Sink Dispatches Attempted:** Events passed to `output_sink.write_event()`.
- **Sink Dispatches Succeeded:** Events accepted synchronously (written to file buffer and accepted into local librdkafka client memory).
- **Sink Dispatches Failed:** Events rejected during local write (e.g. disk full, schema validation failure).
- **Kafka Confirmed Receipts:** Messages confirmed committed by the Kafka broker partition leader via delivery callbacks.
- **Kafka Failed Deliveries:** Messages rejected or timed out by the broker.
- **Kafka In-Flight Pending:** Messages buffered in local client memory awaiting broker acknowledgement (`attempted - (delivered + failed)`). Must reach `0` upon normal shutdown.

### 8.3 Inspecting Output Files Safely
To inspect the generated line-delimited JSON output without locking the file:

**On Windows (PowerShell):**
```powershell
# View the last 5 records
Get-Content data/telemetry.jsonl -Tail 5

# Count total records written
(Get-Content data/telemetry.jsonl).Count
```

**On Linux / macOS:**
```bash
tail -n 5 data/telemetry.jsonl
wc -l data/telemetry.jsonl
```

---

## 9. Graceful Shutdown & Signal Management

### 9.1 Supported OS Signals
The CLI wraps the simulation inside a cross-platform `SignalContext`:
- **`SIGINT`:** Triggered interactively by pressing `Ctrl+C`.
- **`SIGTERM`:** Triggered by process supervisors, systemd, or Docker (`kill -15 <pid>`).
- **`SIGBREAK`:** Supported on Windows (`CTRL_BREAK_EVENT`).

### 9.2 The Graceful Shutdown Protocol
1. **Signal Interception:** The signal handler marks the interruption flag and calls `runner.stop()` non-blockingly. No blocking I/O or flush locks occur inside the signal handler callback.
2. **Cycle Completion:** The active simulation cycle in the main thread finishes generating and dispatching its container readings.
3. **Queue Draining:** Sinks execute `flush()`. The Kafka producer blocks up to 30 seconds (`delivery_timeout_ms`), ensuring all pending messages in librdkafka memory buffers are transmitted and acknowledged by the broker.
4. **Resource Teardown:** Open file handles and Kafka client handles are closed cleanly.
5. **Exit Code Mapping:** The process returns standard POSIX exit codes:
   - `0` (`EXIT_SUCCESS`): Normal completion (`status == "COMPLETED"`).
   - `1` (`EXIT_FAILURE`): Unhandled error, configuration failure, or broker delivery rejection.
   - `2` (`EXIT_USAGE_ERROR`): Invalid CLI syntax or non-positive `--cycles`.
   - `130` (`EXIT_INTERRUPTED`): Gracefully halted by `SIGINT` / `Ctrl+C` ($128 + 2$).
   - `143` (`EXIT_TERMINATED`): Gracefully terminated by `SIGTERM` ($128 + 15$).

---

## 10. Verification and Testing

### 10.1 Running the Automated Test Suite (Unit & Local Tests)
Execute the complete test suite without requiring Docker or a running Kafka broker:

```powershell
# Run all tests using py launcher
py -3.13 -m pytest -v

# Or using active venv
pytest -v
```
- **Expected Results:** **373 passed, 4 skipped** in ~11–13 seconds.
- *Why 4 skipped?* The 4 live integration tests gracefully skip when no live Kafka broker is detected on `localhost:9092`.

### 10.2 Checking Test Coverage
Verify test coverage across the entire `simulation` codebase:

```powershell
pytest --cov=simulation --cov-report=term-missing
```
- **Expected Result:** **$\ge 91\%$ statement coverage** across all modules.

### 10.3 Running Live Kafka Integration Tests
To execute the live broker tests against the local KRaft cluster:

```powershell
# 1. Start the Kafka container
docker compose -f docker-compose.kafka.yml up -d

# 2. Wait 5 seconds for broker initialization, then run integration tests
pytest -v -m integration

# Expected Result: 4 passed in ~4-6 seconds.
```

### 10.4 Verifying JSONL File Validity
Validate that every line in `data/telemetry.jsonl` conforms strictly to the Version-1 contract:

```powershell
python -c "import simulation.events; from simulation.validation.event_validator import EventValidator; import json; lines = open('data/telemetry.jsonl', encoding='utf-8').readlines(); [EventValidator.validate(json.loads(l)) for l in lines]; print(f'Verification passed: all {len(lines)} records adhere strictly to Schema v1!')"
```

### 10.5 Verifying Kafka Streaming with an Independent Consumer
Consume and print records directly from Kafka to verify topic ingestion:

```powershell
python -c "
import confluent_kafka, json

conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'verifier-group', 'auto.offset.reset': 'earliest'}
c = confluent_kafka.Consumer(conf)
c.subscribe(['atmosync.telemetry.v1'])

print('Polling messages from atmosync.telemetry.v1 (press Ctrl+C to stop)...')
try:
    for _ in range(10):
        msg = c.poll(3.0)
        if msg is None: break
        if msg.error(): print('Error:', msg.error()); continue
        key = msg.key().decode('utf-8')
        val = json.loads(msg.value().decode('utf-8'))
        print(f'Received message | Partition: {msg.partition()} | Key: {key} | Sensor: {val[\"sensor_type\"]} | Value: {val[\"value\"]} {val[\"unit\"]}')
finally:
    c.close()
"
```

---

## 11. Troubleshooting

| Symptom / Error Message | Probable Cause | Diagnostic Command | Corrective Action |
| :--- | :--- | :--- | :--- |
| **`error during connect: ... dockerDesktopLinuxEngine ... The system cannot find the file specified`** | Docker Desktop is not running or the Windows Docker pipe is closed. | `docker ps` | Launch Docker Desktop from the Windows Start menu and wait for the status indicator to show "Engine running". |
| **`Kafka Server started ... Local: Message timed out (_MSG_TIMED_OUT)`** | KRaft controller quorum failed to resolve internal container hostname on Docker bridge network. | `docker compose -f docker-compose.kafka.yml logs kafka` | Ensure `docker-compose.kafka.yml` uses `1@127.0.0.1:29093` for `KAFKA_CONTROLLER_QUORUM_VOTERS` and includes `extra_hosts: ["kafka:127.0.0.1"]`. Restart with `docker compose -f docker-compose.kafka.yml up -d`. |
| **`Failed to initialize output destination: ... Failed to resolve 'localhost:9092'` or Connection Refused (10061)** | Kafka broker is stopped or port 9092 is not bound. | `Test-NetConnection -Port 9092 -ComputerName localhost` | Start the Kafka container: `docker compose -f docker-compose.kafka.yml up -d`. If testing without Kafka, use `-m jsonl`. |
| **`Configuration error: Configuration file not found`** | Incorrect path supplied to `-c` / `--config`. | `Test-Path <path>` | Verify path. Default config is located at `simulation/config/simulation_config.yaml`. |
| **`Configuration error: ... duplicate key ...`** | YAML configuration contains duplicate mapping keys. | Inspect YAML file around indicated line. | Remove duplicate key definitions. |
| **`Maximum simulation cycles must be a positive integer`** | User passed `--cycles 0` or negative number. | Review CLI invocation. | Pass an integer $\ge 1$ or omit `--cycles` for continuous streaming. |
| **`Cannot write event: JsonlSink is closed` or File Permission Error** | Output path is not writable or target directory is locked by another process. | `Test-Path (Split-Path <jsonl-path>)` | Verify directory write permissions or specify another path via `--jsonl-path data/other.jsonl`. |
| **`Simulation finished with failure status: PARTIAL_FAILURE`** | Local JSONL writes succeeded, but Kafka broker deliveries failed or timed out during flush. | Check CLI summary: `Kafka Failed Deliveries > 0`. | Verify Kafka broker connectivity and broker logs. |
| **`No module named confluent_kafka`** | Active Python environment is missing required dependencies. | `pip list` | Activate the virtual environment (`.\.venv\Scripts\Activate.ps1`) and run `pip install -r requirements.txt`. |

---

## 12. Cleanup and Reset

### 12.1 Normal Teardown
To cleanly stop background simulation processes and Kafka infrastructure:

```powershell
# 1. Stop any background Python simulator processes
Get-Process python* | Where-Object { $_.CommandLine -like "*simulation*" } | Stop-Process

# 2. Stop the Kafka container
docker compose -f docker-compose.kafka.yml stop
```

### 12.2 Purging Telemetry Output Files
To remove locally generated telemetry files before running a fresh simulation:

```powershell
# Remove generated runtime telemetry in data/
Remove-Item data/*.jsonl -Force -ErrorAction SilentlyContinue
```

> [!CAUTION]
> **PRESERVE AUDIT ARTIFACTS:** Never delete or alter `data/verification/`. That directory contains official verification artifacts (`task_a_both.jsonl`, `task_a_jsonl.jsonl`, etc.) required for project audits.

### 12.3 Destructive Kafka Reset (Wiping Broker State)
If the Kafka cluster becomes corrupted or topic offsets need to be completely reset:

```powershell
# Stop and remove containers, networks, and internal anonymous volumes
docker compose -f docker-compose.kafka.yml down -v
```
*Note: This wipes all stored Kafka topic logs and partition offsets. Next time `up -d` is executed, a clean KRaft cluster will be formatted.*

---

## 13. Operational Limitations

1. **Development-Grade Broker:** The single-node KRaft container is intended strictly for development and integration verification. It does not provide multi-broker replication, high availability, or cross-datacenter failover.
2. **Dual-Sink Delivery Asynchrony:** Dual-sink (`both`) mode writes to local JSONL first and enqueues to Kafka second. It does not implement a distributed two-phase commit. In the event of an unrecoverable broker crash, records may exist on disk that were not committed to Kafka.
3. **Partition-Local Ordering Only:** Kafka guarantees in-order delivery **only within individual topic partitions**. Because messages are partitioned by `container_id`, readings across different containers land on different partitions and will interleave. Downstream consumers must sort using the `observed_at` UTC timestamp.
4. **No Schema Registry:** Event payloads are serialized as JSON and validated client-side. The broker does not enforce schema registry checks.

---

## 14. Quick-Start Checklist

### Fast Track 1: Local JSONL Simulation (No Docker / 30-Second Setup)
- [ ] 1. Clone repository and check out branch:
  ```powershell
  git checkout feature/siddarth-simulation-event-engineering
  ```
- [ ] 2. Activate Python virtual environment:
  ```powershell
  .\.venv\Scripts\Activate.ps1
  ```
- [ ] 3. Run 5 simulation cycles:
  ```powershell
  python -m simulation -n 5 -m jsonl
  ```
- [ ] 4. Confirm output:
  ```powershell
  Get-Content data/telemetry.jsonl -Tail 5
  ```

### Fast Track 2: End-to-End Streaming Simulation (With Docker Kafka)
- [ ] 1. Start Docker Desktop and launch Kafka:
  ```powershell
  docker compose -f docker-compose.kafka.yml up -d
  ```
- [ ] 2. Confirm Kafka is reachable on port 9092:
  ```powershell
  Test-NetConnection -ComputerName localhost -Port 9092
  ```
- [ ] 3. Run dual-sink simulation (10 cycles):
  ```powershell
  python -m simulation -n 10 -m both
  ```
- [ ] 4. Check summary: Confirm `Status: COMPLETED`, `Kafka Confirmed Receipts: 90`, `Kafka In-Flight Pending: 0`.
- [ ] 5. Stop Kafka when finished:
  ```powershell
  docker compose -f docker-compose.kafka.yml down
  ```

---

## 15. References and Authoritative Documentation

For further architectural and technical specifications, refer to the companion documents:
- [DATA_CONTRACT.md](file:///e:/AtmoSync/Infotact_Data_Analyst_internship/docs/DATA_CONTRACT.md): Canonical Version-1 Event Schema, Field Specifications, and Validation Rules.
- [ARCHITECTURE.md](file:///e:/AtmoSync/Infotact_Data_Analyst_internship/docs/ARCHITECTURE.md): Complete System Architecture, Package Hierarchy, and Lifecycle Design.
- [STAGE_5_KAFKA_DESIGN.md](file:///e:/AtmoSync/Infotact_Data_Analyst_internship/docs/STAGE_5_KAFKA_DESIGN.md): Historical Kafka Producer Integration Architecture.
- [STAGE_6_2_RUNNER_DESIGN.md](file:///e:/AtmoSync/Infotact_Data_Analyst_internship/docs/STAGE_6_2_RUNNER_DESIGN.md): Simulation Runner and Metrics Specification.
- [STAGE_6_3_CLI_GUIDE.md](file:///e:/AtmoSync/Infotact_Data_Analyst_internship/docs/STAGE_6_3_CLI_GUIDE.md): Historical CLI Operational Guide.
