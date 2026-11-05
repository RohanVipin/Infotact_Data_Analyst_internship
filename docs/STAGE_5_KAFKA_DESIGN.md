# AtmoSync — Stage 5 Architecture and Design: Kafka Producer Integration

## 1. Stage 5 Objectives

Stage 5 implements **Task A — Part 2: Kafka Streaming and Event Publishing** within the AtmoSync Micro-Climate Arbitrage Analytics pipeline.

Key objectives:
1. Establish a high-throughput, non-blocking Kafka producer for streaming simulated IoT container sensor telemetry.
2. Integrate seamlessly with the existing Stages 0–4 architecture without altering the canonical 7-field `SensorEvent` v1 contract or Option A multiplicity.
3. Partition events strictly by `container_id` to guarantee causal partition affinity per shipping container with verified idempotent ordering.
4. Provide robust error handling, delivery callbacks, operational metrics, queue backpressure management with bounded polling relief, and graceful resource termination.
5. Ensure 100% testability with unit tests decoupled from live broker infrastructure alongside dedicated integration test capabilities.

---

## 2. Official Requirements vs. Project Decisions

| Dimension | Official Infotact Requirements | Existing Project Decisions (Stages 0–4) | Stage 5 Engineering Decisions |
|:---|:---|:---|:---|
| **Telemetry Modalities** | Temperature, humidity, vibration | Isolated models with SHA-256 PRNG streams | Multi-modal events independently streamed to Kafka |
| **Event Contract** | Mock IoT container telemetry | Strict 7-field JSON schema version 1 | Validated by `EventValidator`, serialized by `EventSerializer` |
| **Streaming Multiplicity** | Continuous container streaming | Option A: 1 reading = 1 independent event | 1 event = 1 independent Kafka message |
| **Broker Client** | Apache Kafka | Unassigned in Part 1 | `confluent-kafka` (v2.16.1, `librdkafka` C-engine) |
| **Message Key** | Unspecified | Containers `CONTAINER_001..003` | UTF-8 `container_id` bytes (e.g. `b"CONTAINER_001"`) |
| **Topic Strategy** | Kafka topics feeding Snowflake | Unspecified | Single unified telemetry topic `atmosync.telemetry.v1` |
| **Ordering & Deduplication** | Unspecified | UTC timestamps (`observed_at`) | `enable.idempotence=True`, `max.in.flight <= 5`, key affinity |

---

## 3. Kafka Producer Architecture

```text
SimulationEngine
       |
       v
  SensorEvent (v1 Immutable Dataclass)
       |
       v
 EventSerializer (Canonical 7-Field JSON)
       |
       +-------------------------------+
       |                               |
       v                               v
  JsonlWriter              SensorEventKafkaProducer
 (Local data/*.jsonl)       (confluent_kafka.Producer)
                                       |
                         Key: UTF-8 container_id
                         Val: UTF-8 Canonical JSON
                                       |
                                       v
                             [Topic Partition]
                                       |
                            Murmur2(container_id)
                                       |
                        +--------------+--------------+
                        |                             |
                        v                             v
                   Partition 0                   Partition 1
             [CONTAINER_001 Events]        [CONTAINER_002 Events]
             (Idempotent In-Order)         (Idempotent In-Order)
```

---

## 4. Selected Kafka Client and Justification

**Selected Client:** `confluent-kafka` (version 2.16.1)

### Technical Justification
1. **Engineered for High Throughput:** `confluent-kafka` wraps `librdkafka`, the battle-tested C-library developed and maintained by Confluent. It provides asynchronous, memory-efficient I/O capable of handling hundreds of thousands of messages per second with minimal CPU overhead.
2. **True Asynchronous Non-Blocking Delivery:** Implements native background threads for socket I/O and batching, delegating delivery receipts via high-efficiency `poll()` callbacks rather than blocking the simulation thread.
3. **Python 3.13 & Windows Support:** Offers official, pre-compiled CPython 3.13 AMD64 wheels for Windows (`cp313-cp313-win_amd64.whl`), avoiding C-compiler toolchain issues and deprecation incompatibilities present in pure-Python alternatives (e.g. legacy `kafka-python`).
4. **Enterprise Alignment:** Confluent's client is the industry standard for production Kafka-to-Snowflake streaming architectures.

---

## 5. Kafka Topic Configuration

- **Default Topic Name:** `atmosync.telemetry.v1`
- **Naming Constraints:** Adheres to Kafka topic regex `^[a-zA-Z0-9._-]+$`, max 249 characters.
- **Partitioning Strategy:**
  - Recommended topic partition count: 3 or 6 partitions.
  - With 3 shipping containers (`CONTAINER_001`, `CONTAINER_002`, `CONTAINER_003`), 3+ partitions allow equal parallelism across downstream consumers (Snowflake Snowpipe Streaming / Kafka Connect) while preserving per-container serialization.

---

## 6. Event Serialization and Message Key

### Canonical 7-Field Payload (Value)
Each message value is a UTF-8 encoded JSON string produced directly by `EventSerializer.serialize()`:
```json
{
  "schema_version": 1,
  "event_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
  "container_id": "CONTAINER_001",
  "sensor_type": "temperature",
  "observed_at": "2026-11-05T08:00:00.123Z",
  "value": 4.25,
  "unit": "celsius"
}
```

### Partition Key Strategy and Precise Ordering Guarantees
- **Key Formula:** `event.container_id.encode("utf-8")`
- **Exact Ordering Boundaries and Scope:**
  1. **Same-Key Partition Affinity:** In Apache Kafka, all records with identical keys are deterministically routed to the same partition via the default partitioner (Murmur2 hash). All events for a specific `container_id` are co-located in the same partition log.
  2. **Intra-Partition Sequential Ordering (Single Idempotent Producer):** By configuring `enable.idempotence=True` and `max.in.flight.requests.per.connection <= 5`, `librdkafka` guarantees strict sequential message ordering on that partition for a single producer instance even across network retries, without duplicating records or reordering in-flight batches.
  3. **Multi-Producer & Cross-Partition Boundaries (No Universal FIFO):**
     - **Cross-Partition:** Kafka does not provide total chronological ordering across different partitions. Events in Partition 0 and Partition 1 have independent offset timelines.
     - **Multiple Producers:** Across multiple uncoordinated producer processes or independent application nodes writing concurrently to the same topic or partition, broker arrival order may differ from client event generation order due to network latency and socket concurrency.
     - **Analytical Temporal Sequencing:** Downstream analytical workloads (e.g. Snowflake window functions, micro-climate anomaly detection) must use the event's UTC `observed_at` timestamp for absolute temporal sequencing rather than relying on broker arrival order.

---

## 7. Delivery Semantics

- **Default Acks:** `acks="all"` (or `-1`). The leader broker waits for all in-sync replicas (ISR) to acknowledge receipt before confirming delivery.
- **Idempotence:** Enabled by default (`enable_idempotence=True`), preventing message duplication on broker retries.
- **Delivery Guarantee:** **At-Least-Once Delivery** with idempotent per-producer partition deduplication.
- **No False Claims:** We do not claim transactional "Exactly-Once Semantics" (EOS) across independent batch publishes. In the event of catastrophic producer process crashes before receipt acknowledgement, unconfirmed messages may be re-sent upon restart. Downstream consumers leverage unique `event_id` (UUIDv4) primary keys to deduplicate events.

---

## 8. Error Handling and Retry Strategy

1. **Serialization / Validation Errors:**
   - Any event failing strict Version-1 validation triggers `KafkaPublishError` and increments `stats.serialization_errors`.
   - In batch mode, validation occurs **before touching the Kafka buffer**; an invalid record causes the entire batch to reject with zero submitted messages.
2. **Transient Broker Errors:**
   - Handled automatically by `librdkafka` retries (default `retries=5`, `retry_backoff_ms=100`, `delivery_timeout_ms=30000`).
3. **Hard Delivery Failures:**
   - If broker acknowledgement fails after all retries, the internal delivery callback marks `DeliveryReport.is_success = False`, populates `DeliveryReport.error`, and increments `stats.failed`.

---

## 9. Backpressure and Flushing

- **Queue Limits:**
  - `queue_buffering_max_messages = 100000`
  - `queue_buffering_max_kbytes = 1048576` (1 GB)
- **Bounded Backpressure Relief Policy:**
  - When the local buffer fills faster than brokers can acknowledge, `produce()` raises a `BufferError`.
  - Instead of immediately failing or blocking forever, `SensorEventKafkaProducer` applies a bounded retry loop (configurable via `queue_full_retries`, default 3; and `queue_full_backoff_seconds`, default 0.1s).
  - During backoff, `poll(queue_full_backoff_seconds)` is invoked to actively drain completed delivery callbacks and free buffer capacity.
  - If the buffer remains full after all bounded retries are exhausted, `KafkaBackpressureError` is raised.
- **Flushing & Shutdown:**
  - `flush(timeout, raise_on_timeout)` synchronously waits for outstanding messages to be delivered.
  - If messages remain in the queue after `timeout` and `raise_on_timeout=True`, `KafkaPublishError` is raised.
  - Exiting the context manager (`with SensorEventKafkaProducer(...) as p:`) automatically closes and flushes pending messages while preserving any in-flight application exception.

---

## 10. Operational Metrics & Statistics

The thread-safe `PublishingStats` maintains strict separation between local queue acceptance and broker delivery:

| Metric | Meaning |
|:---|:---|
| `attempted` | Total messages successfully accepted into local `librdkafka` memory buffer. |
| `delivered` | Total messages positively acknowledged by broker receipt callback. |
| `failed` | Total messages whose delivery callback reported a broker error or timeout. |
| `pending` | Messages currently buffered and in-flight awaiting broker acknowledgement (`attempted - (delivered + failed)`). |
| `serialization_errors` | Events rejected locally before entering the producer buffer. |
| `flushes` | Number of explicit `flush()` calls invoked. |

---

## 11. Environment Variables

| Variable Name | Default Value | Description |
|:---|:---:|:---|
| `ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated `host:port` broker list (supports IPv4, hostnames, IPv6 `[::1]:9092`) |
| `ATMOSYNC_KAFKA_TOPIC` | `atmosync.telemetry.v1` | Target Kafka topic |
| `ATMOSYNC_KAFKA_CLIENT_ID` | `atmosync-sensor-producer` | Producer client identifier |
| `ATMOSYNC_KAFKA_ACKS` | `all` | Acknowledgment level (`all`, `1`, `0`) |
| `ATMOSYNC_KAFKA_ENABLE_IDEMPOTENCE` | `true` | Enable idempotent producer delivery (`true` / `false`) |
| `ATMOSYNC_KAFKA_MAX_IN_FLIGHT` | `5` | Maximum unacknowledged requests per connection (<= 5 if idempotent) |
| `ATMOSYNC_KAFKA_DELIVERY_TIMEOUT_MS` | `30000` | End-to-end delivery timeout in ms |
| `ATMOSYNC_KAFKA_REQUEST_TIMEOUT_MS` | `5000` | Broker network request timeout in ms |
| `ATMOSYNC_KAFKA_RETRIES` | `5` | Retry attempts for transient errors |
| `ATMOSYNC_KAFKA_RETRY_BACKOFF_MS` | `100` | Backoff delay between retries in ms |
| `ATMOSYNC_KAFKA_QUEUE_MAX_MESSAGES` | `100000` | Maximum messages in local memory buffer |
| `ATMOSYNC_KAFKA_QUEUE_MAX_KBYTES` | `1048576` | Maximum memory buffer size in kilobytes (1 GB) |
| `ATMOSYNC_KAFKA_QUEUE_FULL_RETRIES` | `3` | Bounded retries to attempt when buffer is full |
| `ATMOSYNC_KAFKA_QUEUE_FULL_BACKOFF_SECONDS` | `0.1` | Polling wait duration during queue backpressure |
| `ATMOSYNC_KAFKA_COMPRESSION_TYPE` | `none` | Compression codec (`none`, `gzip`, `snappy`, `lz4`, `zstd`) |
| `ATMOSYNC_KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Protocol (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`) |
| `ATMOSYNC_KAFKA_SASL_MECHANISM` | `None` | SASL mechanism (`PLAIN`, `SCRAM-SHA-256`, etc.) |
| `ATMOSYNC_KAFKA_SASL_USERNAME` | `None` | SASL username |
| `ATMOSYNC_KAFKA_SASL_PASSWORD` | `None` | SASL password (masked in string representations) |

---

## 12. Installation Instructions

Ensure Python 3.10+ (tested on Python 3.13) is active:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

---

## 13. Local Kafka Broker Setup

To run a local Apache Kafka broker using Docker Compose (KRaft mode, no Zookeeper required):

Create a `docker-compose.kafka.yml` file:
```yaml
version: '3.8'
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: atmosync-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
```

Start the broker:
```bash
docker compose -f docker-compose.kafka.yml up -d
```

Verify broker is healthy:
```bash
docker exec -it atmosync-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Create telemetry topic:
```bash
docker exec -it atmosync-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic atmosync.telemetry.v1 --partitions 3 --replication-factor 1
```

---

## 14. How to Execute the Producer

### Programmatic Usage

```python
from simulation.config.config_loader import ConfigLoader
from simulation.kafka.kafka_config import KafkaConfig
from simulation.kafka.kafka_producer import SensorEventKafkaProducer
from simulation.simulator.simulation_engine import SimulationEngine

# 1. Load simulation and Kafka configurations
app_config = ConfigLoader.load_yaml("config/simulation_config.yaml")
kafka_config = KafkaConfig.from_env()

# 2. Instantiate simulation engine
engine = SimulationEngine(config=app_config)

# 3. Publish streamed cycles using Kafka producer context manager
with SensorEventKafkaProducer(config=kafka_config) as producer:
    for event in engine.stream(max_cycles=10):
        producer.publish_event(event)

    # Flush remaining messages before exiting
    producer.flush(raise_on_timeout=True)

print("Publishing stats:", producer.stats.to_dict())
```

---

## 15. How to Run Unit Tests

All unit tests use mockable producer interfaces and execute instantly without external dependencies:

```bash
python -m pytest -v tests/test_kafka_config.py tests/test_kafka_producer.py
```

---

## 16. How to Run Real Broker Integration Tests

When a live Kafka broker is running:

```bash
# Ensure broker is reachable on localhost:9092 or set custom address
export ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export ATMOSYNC_KAFKA_TOPIC="atmosync.telemetry.v1"

# Run integration tests
python -m pytest -v -m integration
```

All 4 live broker integration tests execute and pass when a broker is present:
- `test_live_broker_publish_and_ack`: Single event publish and broker acknowledgement.
- `test_live_multi_container_and_consumer_validation`: End-to-end multi-container, multi-sensor publishing and consumer schema validation.
- `test_live_consumer_timeout_graceful`: Consumer poll timeout non-blocking safety.
- `test_live_unreachable_broker_failure`: Unreachable broker error handling and delivery timeout reporting.

*Note: If no broker is detected on port 9092, the live integration suite automatically and safely skips.*

---

## 17. Real Kafka Broker Integration and Verification Evidence

### 17.1 Live Broker Environment
- **Container Name:** `atmosync-kafka`
- **Docker Image:** `confluentinc/cp-kafka:7.6.0`
- **Mode:** KRaft (ZooKeeper-less)
- **Cluster ID:** `4L62EnnTQKywh7ad0gFixg`
- **Listeners:** `PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092`
- **Advertised Listeners:** `PLAINTEXT://localhost:29092,PLAINTEXT_HOST://localhost:9092`
- **Connectivity:** TCP connection to `localhost:9092` verified (`TcpTestSucceeded: True`).

### 17.2 Topic Metadata
Topic `atmosync.telemetry.v1` verified with 3 partitions and replication factor 1:
```text
Topic: atmosync.telemetry.v1   TopicId: wodXdKRYRTizRwfl0Gwk5A   PartitionCount: 3   ReplicationFactor: 1
    Partition: 0   Leader: 1   Replicas: 1   Isr: 1
    Partition: 1   Leader: 1   Replicas: 1   Isr: 1
    Partition: 2   Leader: 1   Replicas: 1   Isr: 1
```

### 17.3 Real Producer Execution Evidence
- **Telemetry Modalities:** Temperature, humidity, vibration across 3 simulation cycles.
- **Containers:** `CONTAINER_001`, `CONTAINER_002`, `CONTAINER_003`.
- **Total Published Events:** 27 independent events (Option A multiplicity: 1 reading = 1 event = 1 Kafka message).
- **Delivery Acks:** 27 of 27 successfully acknowledged (`is_success=True`, `error=None`).
- **Flush Duration:** 0.604s for 27 events with 0 remaining unsent.
- **Partition Key Affinity:**
  - `CONTAINER_001` (Key `b"CONTAINER_001"`) -> **Partition 2** (9 events, offsets 0..8)
  - `CONTAINER_002` (Key `b"CONTAINER_002"`) -> **Partition 1** (9 events, offsets 0..8)
  - `CONTAINER_003` (Key `b"CONTAINER_003"`) -> **Partition 0** (9 events, offsets 0..8)
  - Causal partition affinity verified: 100% of messages for each container routed strictly to that container's assigned partition.

### 17.4 Real Consumer Validation Evidence
A live consumer (`confluent_kafka.Consumer`) subscribed to `atmosync.telemetry.v1` with `auto.offset.reset='earliest'`:
- **Total Records Consumed:** 27 records matching published batch.
- **Message Key Verification:** 100% of consumed keys match the event's UTF-8 `container_id`.
- **Schema & Field Verification:**
  - Exact 7 canonical fields present in exact sequence: `["schema_version", "event_id", "container_id", "sensor_type", "observed_at", "value", "unit"]`.
  - `schema_version`: Exactly `1`.
  - `event_id`: Valid UUIDv4 strings.
  - `container_id`: Matches message key.
  - `sensor_type`: In `['temperature', 'humidity', 'vibration']`.
  - `observed_at`: Valid ISO 8601 UTC timestamps ending in `Z`.
  - `value`: Finite float/int, no `NaN`/`inf` or boolean types.
  - `unit`: Non-empty strings matching configured units (`celsius`, `percent`, `m/s^2`).
- **Data Integrity:** 100% `event_id` set equality between published and consumed events (`published_ids == consumed_ids`).
- **Intra-Partition Sequential Ordering (Single Producer):** Monotonically ascending offsets and timestamps verified across all 3 partitions for this single idempotent producer instance. Distinguishes from multi-producer scenarios or cross-partition ordering where universal chronological FIFO ordering is not guaranteed by Kafka.

### 17.5 Edge-Case Verification Evidence
- **Consumer Poll Timeout:** Non-blocking timeout poll safely returns `None` after 0.535s when no new messages are present.
- **Unreachable Broker Policy:** Producer targeting `localhost:9999` with 1000ms timeout caught failure via delivery callback (`KafkaError{code=_MSG_TIMED_OUT,str="Local: Message timed out"}`).

### 17.6 Test Suite Execution
Full test suite executed with live Kafka broker active:
```text
============================= test session starts =============================
platform win32 -- Python 3.13.14, pytest-9.1.1
collected 286 items

tests/integration/test_kafka_integration.py ....                         [  1%]
tests/test_configuration.py ............................................ [ 19%]
tests/test_container_simulator.py ........                               [ 22%]
tests/test_event_builder.py ...............                              [ 27%]
tests/test_event_serializer.py ............................              [ 37%]
tests/test_event_validator.py .......................................... [ 53%]
tests/test_humidity_sensor.py ..........                                 [ 57%]
tests/test_jsonl_writer.py .............                                 [ 61%]
tests/test_kafka_config.py ............................................ [ 82%]
tests/test_kafka_producer.py ...................                         [ 89%]
tests/test_simulation_engine.py ........                                 [ 92%]
tests/test_temperature_sensor.py ...........                             [ 96%]
tests/test_vibration_sensor.py ...........                               [100%]

============================= 286 passed in 5.94s =============================
```

#### Test Suite Breakdown by Module
| Test Module | Test Coverage / Scope | Status | Count |
|:---|:---|:---:|:---:|
| `tests/test_configuration.py` | ConfigLoader YAML validation & schema enforcement | PASS | 51 |
| `tests/test_temperature_sensor.py` | Temperature sensor model, limits & determinism | PASS | 11 |
| `tests/test_humidity_sensor.py` | Humidity sensor model, limits & determinism | PASS | 10 |
| `tests/test_vibration_sensor.py` | Vibration sensor Rayleigh model, non-negative bounds | PASS | 11 |
| `tests/test_event_builder.py` | Version-1 event builder & UUIDv4 generation | PASS | 15 |
| `tests/test_event_validator.py` | Canonical 7-field validation & type safety | PASS | 48 |
| `tests/test_container_simulator.py` | Container sensor grouping & error policy | PASS | 8 |
| `tests/test_simulation_engine.py` | Multi-container simulation cycle orchestration | PASS | 8 |
| `tests/test_event_serializer.py` | Canonical 7-field JSON serializer / deserializer | PASS | 28 |
| `tests/test_jsonl_writer.py` | Local append-only line-delimited JSON writer & atomic batch validation | PASS | 13 |
| `tests/test_kafka_config.py` | Kafka configuration loader, env vars & credentials | PASS | 60 |
| `tests/test_kafka_producer.py` | Unit tests with mock broker, backpressure & flush | PASS | 19 |
| `tests/integration/test_kafka_integration.py` | Live Kafka broker publish, ack, consumer & edge cases | PASS | 4 |
| **Total** | **All Stages 0–5 Automated Regression Tests** | **PASS** | **286** |

*Note on Stage 4 Output Sink: `JsonlWriter` strictly implements append-only line-delimited JSON writing with in-memory atomic validation and automatic directory creation. File rotation is not implemented in Stage 4 and is documented as an out-of-scope future enhancement.*

---

## 18. Known Limitations

1. **Schema Registry:** Avro/Protobuf Schema Registry integration is not included in Stage 5, adhering strictly to the JSON contract specified for Part 1 & Part 2.
2. **No Consumer Pipeline:** Stage 5 implements the event publishing producer. Downstream consumption (Snowpipe Streaming / Kafka Connect) will be implemented in subsequent stages.

---

## 19. Integration Requirements for Stage 6

1. **CLI / Runner Integration:** Connect the `SimulationEngine` and `SensorEventKafkaProducer` to a unified simulation CLI runner supporting simultaneous JSONL file output and live Kafka streaming.
2. **Throughput Profiling & Benchmarking:** Measure publisher throughput (events/sec) and latency under sustained continuous generation.
3. **Snowflake Ingestion Handover:** Ensure Kafka topic message format and partitioning are aligned with Snowflake ingestion landing tables.
