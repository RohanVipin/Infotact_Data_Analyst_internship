# ATMOSYNC STAGE 6.3 CLI USER & OPERATIONAL GUIDE

## 1. Overview & Architecture

The AtmoSync command-line interface (`python -m simulation` or `python simulation/cli.py`) is the operational entry point for the Task A IoT Sensor Telemetry Simulator.

It provides terminal controls to execute the unified simulation pipeline, configure destinations (JSONL, Kafka, or both), govern execution duration (finite cycle limit or continuous streaming), control logging verbosity, and perform graceful, non-blocking shutdowns via OS signals (`SIGINT` / `SIGTERM`).

```
Terminal User / Process Supervisor
             |
             v (CLI invocation: python -m simulation [OPTIONS])
   +---------+---------+
   |   simulation.cli  |
   | - Argument Parser |
   | - SignalContext   |
   +---------+---------+
             |
             +---- ConfigLoader.load() --------> AppConfig
             |
             +---- create_output_sink() -------> OutputSink (JsonlSink / KafkaSink / Composite)
             |
             v
   +---------+---------+
   |  SimulationRunner |
   | - Lifecycle       |
   | - Flushes & Acks  |
   +---------+---------+
             |
             +--------------------> [data/telemetry.jsonl]
             |
             +--------------------> [Kafka Broker: localhost:9092]
```

---

## 2. Installation & Prerequisites

### Environment Setup
- Python 3.10+ (tested on Python 3.13)
- Required packages:
  ```bash
  pip install -r requirements.txt
  ```
  *(Installs `PyYAML>=6.0.1` and `confluent-kafka>=2.0.0`)*

### Kafka Broker (Optional for JSONL mode; Required for Kafka/Both modes)
When running in `kafka` or `both` mode, ensure a Kafka cluster or the project's local KRaft Docker container is running:
```bash
docker-compose -f docker/docker-compose.kafka.yml up -d
```
Verify broker accessibility on `localhost:9092`.

---

## 3. CLI Synopsis & Options

```bash
python -m simulation [-h] [-c CONFIG] [-n CYCLES] [-m {both,jsonl,kafka}]
                     [--jsonl-path JSONL_PATH] [--bootstrap-servers BOOTSTRAP_SERVERS]
                     [--topic TOPIC] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [-v]
```

### Supported Arguments

| Flag | Long Option | Type | Default | Description |
| :--- | :--- | :---: | :--- | :--- |
| `-h` | `--help` | flag | — | Display help message and command-line usage. |
| `-v` | `--version` | flag | — | Show application version number and exit. |
| `-c` | `--config` | `str` | `simulation/config/simulation_config.yaml` | Path to simulation YAML configuration file. |
| `-n` | `--cycles` | `int` | `None` | Number of simulation cycles to execute (positive integer; default is continuous). |
| `-m` | `--output-mode` | `str` | `both` | Output telemetry destination: `both`, `jsonl`, or `kafka`. |
| — | `--jsonl-path` | `str` | `data/telemetry.jsonl` | Target filesystem path for line-delimited JSON output. |
| — | `--bootstrap-servers`| `str` | `None` | Kafka broker list (overrides `ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS`). |
| — | `--topic` | `str` | `None` | Kafka topic name (overrides `ATMOSYNC_KAFKA_TOPIC`). |
| `-l` | `--log-level` | `str` | `INFO` | Logging threshold (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`). |

---

## 4. Execution Examples

### 4.1 Finite Execution (Smoke Testing & Verification)

#### Example 1: Default Dual-Sink Run (10 Cycles)
Produces 10 simulation cycles simultaneously to both `data/telemetry.jsonl` and local Kafka broker:
```bash
python -m simulation -n 10
```

#### Example 2: Local JSONL Persistence Only (No Kafka Required)
Runs 5 cycles writing exclusively to a custom JSONL file:
```bash
python -m simulation -n 5 -m jsonl --jsonl-path data/custom_telemetry.jsonl
```

#### Example 3: Kafka Streaming Only with Custom Topic & Broker
Publishes 100 cycles exclusively to a remote Kafka cluster:
```bash
python -m simulation -n 100 -m kafka --bootstrap-servers kafka.internal:9092 --topic prod.telemetry.v1 -l DEBUG
```

### 4.2 Continuous Execution & Graceful Shutdown

#### Example 4: Continuous Streaming Run
Runs indefinitely at the interval specified in `simulation_config.yaml`:
```bash
python -m simulation -m both
```
To halt continuous execution:
- Press `Ctrl+C` in the terminal (sends `SIGINT`).
- Or send `SIGTERM` from process supervisor (`kill -15 <pid>`).

---

## 5. Graceful Shutdown & Signal Handling

The CLI wraps the pipeline inside a non-blocking `SignalContext`:
1. **Signal Reception:** When `SIGINT` (Ctrl+C) or `SIGTERM` is intercepted by the main thread, the callback marks the interruption flag and immediately calls `runner.stop()`.
2. **Cycle Completion:** The signal handler does not execute blocking flushes or locks directly inside the signal callback. The active simulation cycle in `runner.run()` completes generating and dispatching its container events.
3. **Queue Draining:** Sinks are flushed (`output_sink.flush()`). Kafka delivery callbacks are drained, ensuring all messages in librdkafka buffers are acknowledged by the broker before exit.
4. **Resource Release:** Owned file handles and Kafka client handles are closed cleanly.
5. **Restoration:** Previous OS signal handlers are restored upon exit from `SignalContext`.

---

## 6. Process Exit Codes

The CLI returns standard POSIX-compliant exit codes:

| Exit Code | Constant | Meaning |
| :---: | :--- | :--- |
| `0` | `EXIT_SUCCESS` | Execution completed normally (`status == "COMPLETED"`). |
| `1` | `EXIT_FAILURE` | General runtime error, configuration error, or simulation/dispatch failure. |
| `2` | `EXIT_USAGE_ERROR` | Command-line argument syntax error or semantic validation failure (e.g. `--cycles 0`). |
| `130`| `EXIT_INTERRUPTED` | Execution gracefully halted by `SIGINT` / Ctrl+C ($128 + 2$). |
| `143`| `EXIT_TERMINATED` | Execution gracefully terminated by `SIGTERM` ($128 + 15$). |

---

## 7. Environment Variables & Kafka Configuration

Command-line flags override environment variables. When flags are omitted, settings fall back to environment variables:

| Setting | CLI Flag | Environment Variable | Default |
| :--- | :--- | :--- | :--- |
| Broker List | `--bootstrap-servers` | `ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| Topic Name | `--topic` | `ATMOSYNC_KAFKA_TOPIC` | `atmosync.telemetry.v1` |
| Client ID | — | `ATMOSYNC_KAFKA_CLIENT_ID` | `atmosync-sensor-producer` |
| Security Protocol | — | `ATMOSYNC_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` |
| SASL Mechanism | — | `ATMOSYNC_KAFKA_SASL_MECHANISM` | `None` |
| SASL Username | — | `ATMOSYNC_KAFKA_SASL_USERNAME` | `None` |
| SASL Password | — | `ATMOSYNC_KAFKA_SASL_PASSWORD` | `None` *(Masked in logs)* |

---

## 8. Output Summary Format

Upon pipeline completion or interruption, the CLI prints a structured execution summary to standard output:

```
============================================================
          AtmoSync Simulation Pipeline Summary
============================================================
Status:                      COMPLETED
Wall-Clock Duration:         1.84s
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

---

## 9. Troubleshooting

| Error Message / Symptom | Probable Cause | Recommended Action |
| :--- | :--- | :--- |
| `Configuration error: Configuration file not found` | Invalid `-c` or `--config` path. | Verify configuration file exists at specified path. Default: `simulation/config/simulation_config.yaml`. |
| `Maximum simulation cycles must be a positive integer` | Passed `--cycles 0` or negative number. | Specify an integer $\ge 1$ or omit `--cycles` for continuous streaming. |
| `Failed to initialize output destination: ... Failed to resolve 'localhost:9092'` | Kafka broker unreachable in `both` or `kafka` mode. | Start broker via Docker Compose (`docker-compose -f docker/docker-compose.kafka.yml up -d`) or use `-m jsonl`. |
| `Cannot write event: JsonlSink is closed` | File permissions or I/O failure. | Verify write permissions for target `--jsonl-path`. |
