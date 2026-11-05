"""Integration tests for AtmoSync Kafka Producer with a live Kafka broker.

These tests are marked with `@pytest.mark.integration` and require a live, reachable
Kafka cluster (e.g. running on localhost:9092). If no broker is detected, tests are
gracefully skipped.
"""

import os
import socket
import uuid
import pytest

from simulation.events.event_schema import SensorEvent
from simulation.kafka.delivery_report import DeliveryReport
from simulation.kafka.kafka_config import KafkaConfig
from simulation.kafka.kafka_producer import SensorEventKafkaProducer


def is_broker_reachable(host: str = "localhost", port: int = 9092, timeout: float = 1.0) -> bool:
    """Check if a TCP connection can be established to the Kafka broker."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


@pytest.fixture
def live_kafka_config() -> KafkaConfig:
    """Provide KafkaConfig pointing to live broker from environment or defaults."""
    bootstrap_servers = os.getenv("ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    host, port_str = bootstrap_servers.split(",")[0].strip().rsplit(":", 1)
    port = int(port_str)

    if not is_broker_reachable(host, port):
        pytest.skip(f"Live Kafka broker is not reachable at {host}:{port}")

    return KafkaConfig.from_env(
        bootstrap_servers=bootstrap_servers,
        topic=os.getenv("ATMOSYNC_KAFKA_TOPIC", "atmosync.telemetry.integration"),
        client_id="atmosync-integration-tester",
        delivery_timeout_ms=10000,
    )


@pytest.mark.integration
class TestKafkaLiveIntegration:
    """Live broker integration test suite."""

    def test_live_broker_publish_and_ack(self, live_kafka_config: KafkaConfig) -> None:
        """Publish a real sensor event to a live Kafka broker and receive delivery receipt."""
        event = SensorEvent(
            schema_version=1,
            event_id=str(uuid.uuid4()),
            container_id="CONTAINER_LIVE_001",
            sensor_type="temperature",
            observed_at="2026-11-05T08:00:00.000Z",
            value=21.5,
            unit="celsius",
        )

        receipts = []

        def ack(report: DeliveryReport) -> None:
            receipts.append(report)

        with SensorEventKafkaProducer(config=live_kafka_config) as producer:
            producer.publish_event(event, on_delivery=ack)
            remaining = producer.flush(timeout=5.0)

        assert remaining == 0
        assert len(receipts) == 1
        assert receipts[0].is_success is True
        assert receipts[0].event_id == event.event_id
        assert receipts[0].partition is not None
        assert receipts[0].offset is not None

    def test_live_multi_container_and_consumer_validation(self, live_kafka_config: KafkaConfig) -> None:
        """Publish across multiple containers and sensor types, then consume and validate schema."""
        import json
        import time
        from confluent_kafka import Consumer, KafkaError
        from simulation.events.event_serializer import EventSerializer
        from simulation.validation.event_validator import EventValidator

        containers = ["CONTAINER_001", "CONTAINER_002", "CONTAINER_003"]
        sensors = [
            ("temperature", 4.25, "celsius"),
            ("humidity", 65.2, "percent"),
            ("vibration", 0.015, "m/s^2"),
        ]

        published_events = []
        receipts = []

        def ack(report: DeliveryReport) -> None:
            receipts.append(report)

        with SensorEventKafkaProducer(config=live_kafka_config) as producer:
            for c_id in containers:
                for s_type, s_val, s_unit in sensors:
                    event = SensorEvent(
                        schema_version=1,
                        event_id=str(uuid.uuid4()),
                        container_id=c_id,
                        sensor_type=s_type,
                        observed_at="2026-11-05T08:15:30.123Z",
                        value=s_val,
                        unit=s_unit,
                    )
                    EventValidator.validate(event)
                    published_events.append(event)
                    producer.publish_event(event, on_delivery=ack)

            remaining = producer.flush(timeout=10.0, raise_on_timeout=True)

        assert remaining == 0
        assert len(receipts) == len(published_events)
        assert all(r.is_success for r in receipts)

        # Verify partition affinity (all events for a container route to the same partition)
        container_to_partitions = {}
        for r in receipts:
            container_to_partitions.setdefault(r.container_id, set()).add(r.partition)
        for c_id, parts in container_to_partitions.items():
            assert len(parts) == 1, f"Container {c_id} routed to multiple partitions: {parts}"

        # Consumer verification
        consumer = Consumer({
            "bootstrap.servers": live_kafka_config.bootstrap_servers,
            "group.id": f"atmosync-int-verify-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([live_kafka_config.topic])

        published_ids = {e.event_id for e in published_events}
        consumed_records = []
        start_t = time.time()

        try:
            while len(consumed_records) < len(published_events) and (time.time() - start_t) < 15.0:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    pytest.fail(f"Consumer error: {msg.error()}")

                raw_val = msg.value().decode("utf-8")
                event_dict = json.loads(raw_val)
                # Filter only this test run's events
                if event_dict.get("event_id") in published_ids:
                    # Validate key
                    assert msg.key().decode("utf-8") == event_dict["container_id"]
                    # Validate 7 fields
                    assert list(event_dict.keys()) == [
                        "schema_version",
                        "event_id",
                        "container_id",
                        "sensor_type",
                        "observed_at",
                        "value",
                        "unit",
                    ]
                    # Validate deserialization
                    deserialized = EventSerializer.deserialize(raw_val)
                    EventValidator.validate(deserialized)
                    # Verify unit strictly matches configured event unit
                    expected_unit = "celsius" if deserialized.sensor_type == "temperature" else ("percent" if deserialized.sensor_type == "humidity" else "m/s^2")
                    assert deserialized.unit == expected_unit, f"Unit mismatch for {deserialized.sensor_type}: {deserialized.unit} != {expected_unit}"
                    consumed_records.append((msg.partition(), msg.offset(), deserialized))
        finally:
            consumer.close()

        assert len(consumed_records) == len(published_events)
        consumed_ids = {rec[2].event_id for rec in consumed_records}
        assert consumed_ids == published_ids

    def test_live_consumer_timeout_graceful(self, live_kafka_config: KafkaConfig) -> None:
        """Consumer polling on empty partition/topic gracefully returns None without hanging."""
        from confluent_kafka import Consumer

        consumer = Consumer({
            "bootstrap.servers": live_kafka_config.bootstrap_servers,
            "group.id": f"atmosync-empty-int-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
        })
        consumer.subscribe([live_kafka_config.topic])
        try:
            msg = consumer.poll(timeout=0.5)
            assert msg is None
        finally:
            consumer.close()

    def test_live_unreachable_broker_failure(self) -> None:
        """Producer configured with unreachable broker reports delivery failure."""
        unreachable_config = KafkaConfig(
            bootstrap_servers="localhost:9999",
            topic="atmosync.telemetry.integration",
            delivery_timeout_ms=1000,
            request_timeout_ms=500,
            retry_backoff_ms=50,
            queue_full_retries=1,
        )
        receipts = []
        with SensorEventKafkaProducer(config=unreachable_config) as producer:
            event = SensorEvent(
                schema_version=1,
                event_id=str(uuid.uuid4()),
                container_id="CONTAINER_FAIL_001",
                sensor_type="temperature",
                observed_at="2026-11-05T08:00:00.000Z",
                value=20.0,
                unit="celsius",
            )
            producer.publish_event(event, on_delivery=lambda r: receipts.append(r))
            try:
                producer.flush(timeout=2.0, raise_on_timeout=True)
            except Exception:
                pass  # flush timeout is an acceptable failure mode

        # Either flush raised or delivery callback received error
        assert (len(receipts) == 1 and not receipts[0].is_success) or len(receipts) == 0


