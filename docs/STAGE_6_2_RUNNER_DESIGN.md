# ATMOSYNC STAGE 6.2 DESIGN SPECIFICATION: UNIFIED SIMULATION RUNNER

## 1. Executive Summary & Architecture

Stage 6.2 introduces `SimulationRunner`, the core execution orchestrator uniting the Stage 4 multi-container `SimulationEngine` and the Stage 6.1 `OutputSink` architecture (`JsonlSink`, `KafkaSink`, and `CompositeOutputSink`).

The unified pipeline connects virtual container sensors with multi-destination persistence and streaming while preserving single event identity, strict immutability, fail-fast error propagation, and transparent distinction between synchronous local dispatch and asynchronous broker acknowledgements.

```
                              +---------------------------+
                              |      SimulationEngine     |
                              |  - Multi-Container PRNG   |
                              |  - Interval Scheduling    |
                              +-------------+-------------+
                                            |
                                            v (Yields canonical SensorEvent)
                              +-------------+-------------+
                              |      SimulationRunner     |
                              |  - Lifecycle Coordination |
                              |  - Metrics Collection     |
                              |  - Fail-Fast Enforcement  |
                              |  - Resource Ownership     |
                              +-------------+-------------+
                                            |
                                            v (write_event by reference)
                              +-------------+-------------+
                              |    CompositeOutputSink    |
                              |  - Deterministic Order    |
                              |  - Zero Mutation/Re-emit  |
                              +------+-------------+------+
                                     |             |
                     +---------------+             +---------------+
                     |                                             |
                     v                                             v
          +----------+----------+                       +----------+----------+
          |      JsonlSink      |                       |      KafkaSink      |
          +----------+----------+                       +----------+----------+
                     |                                             |
                     v                                             v
          +----------+----------+                       +----------+----------+
          |     JsonlWriter     |                       |    Kafka Producer   |
          |  (Atomic Line I/O)  |                       |  (confluent-kafka)  |
          +----------+----------+                       +----------+----------+
                     |                                             |
                     v                                             v
          [data/telemetry.jsonl]                       [Kafka Broker / Topic]
```

---

## 2. Component Responsibilities & Contract

| Component | Stage | Primary Responsibility | Interface Guarantees |
| :--- | :---: | :--- | :--- |
| `SimulationEngine` | Stage 4 | Multi-container simulation loop, virtual sensor sampling, isolated PRNG stream management | Emits immutable `SensorEvent` instances per cycle or stream. `stop()` gracefully halts iteration. |
| `OutputSink` | Stage 6.1 | Abstract contract for all telemetry destinations | Abstract `write_event(event)`, batch `write_events(events)`, `flush()`, `close()`. |
| `CompositeOutputSink` | Stage 6.1 | Deterministic multi-destination fan-out adapter | Forwards the exact same event reference in configured order. No two-phase commit rollback. |
| `SimulationRunner` | Stage 6.2 | Coordinated pipeline runner, lifecycle manager, metrics collector | Coordinates engine cycles and sink dispatch. Tracks synchronous attempts and asynchronous Kafka acks. |
| `RunnerMetrics` | Stage 6.2 | Immutable execution snapshot dataclass | Reports counters for generated, attempted, succeeded, failed events, cycle counts, Kafka acks/pending, and wall-clock duration. |

---

## 3. Public Classes and Signatures

### 3.1 `RunnerMetrics` (Dataclass)
```python
@dataclass(frozen=True)
class RunnerMetrics:
    events_generated: int = 0
    sink_dispatch_attempted: int = 0
    sink_dispatch_succeeded: int = 0
    sink_dispatch_failed: int = 0
    simulation_cycles_completed: int = 0
    simulation_cycles_failed: int = 0
    kafka_delivery_acknowledged: Optional[int] = None
    kafka_delivery_failed: Optional[int] = None
    kafka_pending: Optional[int] = None
    status: str = "INITIALIZED"  # 'INITIALIZED', 'RUNNING', 'COMPLETED', 'FAILED', 'STOPPED'
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]: ...
```
*Note: `SimulationResult` is defined as a direct alias for `RunnerMetrics`.*

### 3.2 `SimulationRunner`
```python
class SimulationRunner:
    def __init__(
        self,
        engine: SimulationEngine,
        output_sink: OutputSink,
        fail_fast: bool = True,
        owns_sink: bool = True,
        sleep_fn: Optional[Callable[[float], None]] = None,
    ) -> None: ...

    @property
    def engine(self) -> SimulationEngine: ...
    @property
    def output_sink(self) -> OutputSink: ...
    @property
    def fail_fast(self) -> bool: ...
    @property
    def owns_sink(self) -> bool: ...
    @property
    def is_stopped(self) -> bool: ...
    @property
    def is_closed(self) -> bool: ...
    @property
    def status(self) -> str: ...

    def run_cycle(self, observed_at: Optional[Union[str, datetime]] = None) -> List[SensorEvent]: ...
    def run(self, max_cycles: Optional[int] = None) -> RunnerMetrics: ...
    def run_continuously(self) -> RunnerMetrics: ...
    def stop(self) -> None: ...
    def flush(self) -> None: ...
    def close(self, in_flight_exc: Optional[BaseException] = None) -> None: ...
    def get_metrics(self) -> RunnerMetrics: ...

    def __enter__(self) -> "SimulationRunner": ...
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: ...
```

---

## 4. Execution Lifecycle & State Machine

```
      +-------------+
      | INITIALIZED |
      +------+------+
             |
             | run() / run_cycle()
             v
      +-------------+
      |   RUNNING   | <-------+ (Next Cycle Loop)
      +------+------+         |
             |                |
             +----------------+
             |
             +--------------------+---------------------+
             |                    |                     |
             | Normal Drain       | stop()              | Exception
             v                    v                     v
      +-------------+      +-------------+       +-------------+
      |  COMPLETED  |      |   STOPPED   |       |   FAILED    |
      +------+------+      +------+------+       +------+------+
             |                    |                     |
             +--------------------+---------------------+
                                  |
                                  v
                      +-----------------------+
                      | Shutdown:             |
                      | 1. output_sink.flush()|
                      | 2. close() if owned   |
                      | 3. Preserve in-flight |
                      +-----------------------+
```

### Lifecycle Rules:
1. **Validation & Entry**: `run(max_cycles)` validates that `max_cycles` is a positive non-zero integer. Rejects `0`, negative numbers, booleans, and floats with `ValueError`.
2. **Cycle Dispatch Loop**:
   - `engine.run_cycle()` generates events across configured containers.
   - For every generated event:
     a. Increment `events_generated`.
     b. Increment `sink_dispatch_attempted`.
     c. Submit event to `output_sink.write_event(event)`.
     d. Increment `sink_dispatch_succeeded` on success, or `sink_dispatch_failed` on error.
     e. If `fail_fast=True`, immediately raise the exception and abort subsequent dispatches.
   - Increment `simulation_cycles_completed` if all dispatches succeeded, or `simulation_cycles_failed` if any failed.
   - If more cycles remain, sleep for `config.simulation.interval_seconds` using `sleep_fn`.
3. **Shutdown & Cleanup**:
   - In all termination paths (`COMPLETED`, `STOPPED`, `FAILED`), runner invokes `_shutdown()`.
   - `output_sink.flush()` is invoked unconditionally to flush in-memory file buffers and drain Kafka message queues.
   - If `owns_sink=True`, `output_sink.close()` is invoked to release open file descriptors and producer resources.
   - If `owns_sink=False`, the sink is flushed but left open for the external owner to close.
   - If an application exception occurred during simulation or dispatch, `_shutdown()` logs any secondary cleanup errors but **never masks the original application exception**.

---

## 5. Metrics & Kafka Delivery Accounting

The runner strictly enforces architectural honesty by distinguishing between synchronous client dispatch and asynchronous broker acknowledgements:

| Metric Name | Type | Scope | Description |
| :--- | :---: | :--- | :--- |
| `events_generated` | `int` | Simulation | Total readings produced by virtual sensors. |
| `sink_dispatch_attempted` | `int` | Output Sink | Total events passed to `output_sink.write_event()`. |
| `sink_dispatch_succeeded` | `int` | Output Sink | Total events accepted synchronously by local sinks (e.g. written to JSONL buffer and accepted into librdkafka queue). |
| `sink_dispatch_failed` | `int` | Output Sink | Total events rejected synchronously during sink write. |
| `simulation_cycles_completed`| `int` | Pipeline | Cycles where all container events were generated and dispatched without error. |
| `simulation_cycles_failed` | `int` | Pipeline | Cycles interrupted by simulation or dispatch exceptions. |
| `kafka_delivery_acknowledged`| `Optional[int]` | Kafka Broker | Messages confirmed delivered by Kafka broker receipt (`None` if no Kafka sink is active). |
| `kafka_delivery_failed` | `Optional[int]` | Kafka Broker | Messages rejected or timed out by Kafka broker (`None` if no Kafka sink is active). |
| `kafka_pending` | `Optional[int]` | Kafka Producer | Messages enqueued in local producer buffer awaiting broker acknowledgement. |

### Kafka Metric Extraction Mechanism
When `KafkaSink` (or a `CompositeOutputSink` containing `KafkaSink`) is attached, the runner dynamically inspects `producer.stats`:
- `kafka_delivery_acknowledged = sum(producer.stats.delivered)`
- `kafka_delivery_failed = sum(producer.stats.failed)`
- `kafka_pending = sum(producer.stats.pending)`
Upon runner normal shutdown, `output_sink.flush()` blocks to drain pending messages, ensuring `kafka_pending == 0` on clean completion. If `flush()` times out with unsent messages, a `KafkaPublishError` is raised and `status` transitions to `FAILED`.

---

## 6. Failure Modes & Distributed Reality

Composite sinks coordinate local file append and remote Kafka publishing. They **do not** implement a distributed two-phase commit (2PC) or rollback transaction:

| Failure Scenario | Immediate Behavior | Post-Condition State |
| :--- | :--- | :--- |
| **Engine Fails Mid-Cycle** | Engine exception propagates unmasked. | `simulation_cycles_failed += 1`, runner flushes partial cycle, marks `FAILED`. |
| **JSONL Write Fails** | `JsonlWriterError` propagates immediately under `fail_fast=True`. Kafka write is skipped for that event. | `sink_dispatch_failed += 1`, `status = "FAILED"`. |
| **Kafka Enqueue Fails** | JSONL record was already written to disk. `KafkaPublishError` or `KafkaBackpressureError` propagates immediately. | `sink_dispatch_failed += 1`. JSONL record remains on disk. Consumers can deduplicate using `event_id`. |
| **Kafka Broker Delivery Fails** | Enqueue succeeded initially, but broker delivery callback reports error. | `kafka_delivery_failed += 1`. `kafka_pending` decreases. |
| **Final Flush Timeout** | Flush times out with unsent messages in client buffer. | `KafkaPublishError` raised. Runner marks `FAILED`. |
| **Cleanup Fails During Error** | Secondary cleanup exception is logged with `logger.error()`. | Original application exception propagates without being overwritten. |

---

## 7. Programmatic Usage Examples

### Example 1: Dual-Sink Coordinated Execution (Finite Run)
```python
from pathlib import Path
from simulation.config.config_loader import ConfigLoader
from simulation.kafka.kafka_config import KafkaConfig
from simulation.output.sink import CompositeOutputSink, JsonlSink, KafkaSink
from simulation.runner import SimulationRunner
from simulation.simulator.simulation_engine import SimulationEngine

# 1. Load application configuration
config = ConfigLoader.load("config/simulation_config.yaml")

# 2. Instantiate simulation engine
engine = SimulationEngine(config=config)

# 3. Configure dual output sinks
jsonl_sink = JsonlSink(output_path=Path("data/telemetry.jsonl"))
kafka_sink = KafkaSink(config=KafkaConfig.from_env())
composite_sink = CompositeOutputSink([jsonl_sink, kafka_sink], fail_fast=True)

# 4. Instantiate and execute runner within context manager
with SimulationRunner(engine=engine, output_sink=composite_sink, owns_sink=True) as runner:
    metrics = runner.run(max_cycles=10)

print(f"Status: {metrics.status}")
print(f"Events Generated: {metrics.events_generated}")
print(f"JSONL / Sink Dispatches: {metrics.sink_dispatch_succeeded}")
print(f"Kafka Acknowledged: {metrics.kafka_delivery_acknowledged}")
print(f"Kafka Pending: {metrics.kafka_pending}")
```

### Example 2: Continuous Execution with Graceful Halt
```python
import threading
import time
from simulation.runner import SimulationRunner

runner = SimulationRunner(engine=engine, output_sink=jsonl_sink)

def halt_after_delay():
    time.sleep(30.0)
    runner.stop()

# Spawn background timer to stop simulation
threading.Thread(target=halt_after_delay, daemon=True).start()

# Run continuous streaming
summary = runner.run_continuously()
assert summary.status == "STOPPED"
```

---

## 8. Scope Boundaries & Future Stages

### Implemented in Stage 6.2:
- Coordinated multi-cycle and continuous runner (`SimulationRunner`).
- Immutable metrics snapshot (`RunnerMetrics`, `SimulationResult`).
- Integration across `SimulationEngine`, `SensorEvent`, `JsonlSink`, `KafkaSink`, `CompositeOutputSink`.
- Automated regression suite (unit, failure, and live broker integration tests).

### Explicitly Excluded (Deferred to Stage 6.3):
- CLI entrypoint (`simulation/__main__.py`).
- Command-line argument parsing (`argparse`).
- OS signal registration (`SIGINT`, `SIGTERM`).
- Downstream storage integrations (Snowflake, dbt, Superset).
