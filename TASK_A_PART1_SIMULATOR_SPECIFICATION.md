# AtmoSync --- Task A, Part 1

## Python IoT Sensor Simulator: Implementation Specification

**Owner:** Siddarth\
**Project:** AtmoSync --- Micro-Climate Arbitrage Analytics\
**Assigned responsibility:** Simulation & Event Engineering\
**Development environment:** Antigravity IDE\
**Primary language:** Python\
**Document status:** Working implementation specification\
**Scope:** Task A --- Part 1 only

------------------------------------------------------------------------

## 1. Purpose

This document is the working specification for Siddarth's Python Sensor
Simulator. Antigravity must read it before planning or changing code.

If this document conflicts with an official assignment, the existing
repository, or an agreed team interface, stop and report the conflict.
Do not silently invent a resolution.

This specification reduces ambiguity but does not guarantee bug-free
software. All functionality must be verified through execution and
tests.

## 2. Official project context

The official AtmoSync project document describes micro-climate analytics
for agricultural commodities transported in shipping containers. It
identifies mock IoT telemetry for temperature, humidity and vibration.
Its Week 1 plan explicitly calls for a Python IoT simulator generating
container telemetry. The wider Week 1 pipeline also includes Kafka
topics and streaming data into Snowflake raw tables.

Siddarth's current assigned scope is the simulator shown in the supplied
role architecture. Apache Kafka Producer work is Task A --- Part 2 and
is currently assigned to another teammate.

## 3. Scope boundary

### Included in Part 1

1.  Configuration
2.  Container simulator
3.  Temperature sensor
4.  Humidity sensor
5.  Vibration sensor
6.  Sensor event generator
7.  Event validation
8.  JSON serialization
9.  Console output
10. Local JSON Lines (JSONL) storage
11. Automated tests
12. Technical documentation and handover information

### Explicitly excluded for now

-   Apache Kafka producer
-   Kafka topic creation or administration
-   Kafka message publishing or consumption
-   Snowflake ingestion
-   dbt transformations
-   Apache Superset dashboards
-   Other team members' assigned modules

Do not add a Kafka dependency in Part 1. Keep a clean output boundary so
the Part 2 owner can consume validated event objects or serialized JSON
later.

If Siddarth is subsequently asked to take over Part 2, treat that as a
separately approved scope change.

## 4. Required architecture

``` text
Configuration
    |
    v
Container Simulator
    |
    +-------------------+-------------------+
    |                   |                   |
    v                   v                   v
Temperature          Humidity            Vibration
Sensor               Sensor              Sensor
    |                   |                   |
    +-------------------+-------------------+
                        |
                        v
               Sensor Event Generator
                        |
                        v
                Event Validation
                        |
                        v
                 JSON Serialization
                        |
             +----------+----------+
             |                     |
             v                     v
       Console Output         Local JSONL File
                        |
                        v
          Future Integration Boundary
             (Task A — Part 2)
                Apache Kafka
```

The simulator must run without Kafka, a broker, Snowflake, dbt or
Superset.

## 5. Functional requirements

### FR-01: Configuration

Provide configurable settings for:

-   Container identifiers
-   Simulation interval
-   Number of events or run duration, if supported by the chosen
    execution mode
-   Random seed for reproducible test runs
-   Sensor configuration and permitted value limits

Separate configuration from simulation logic. YAML is a proposed option,
not an official requirement.

Do not invent official operating thresholds, sensor limits, alert
thresholds or business rules. The official project document names sensor
types but does not define numerical ranges. Keep unconfirmed values
configurable and label them provisional until approved by the team or
mentor.

Validate configuration at startup. Invalid configuration must produce a
clear error rather than silently using unexplained defaults.

### FR-02: Container simulation

-   Support configured container identifiers.
-   Associate readings with the correct container.
-   Generate telemetry for each configured container on each cycle,
    unless the approved design specifies otherwise.
-   Separate container logic from individual sensor logic.
-   Support finite runs for tests and demonstrations.
-   Support continuous generation using the configured interval.

### FR-03: Virtual sensors

Implement separate, testable modules for temperature, humidity and
vibration.

Each sensor must:

-   Produce a numeric reading.
-   Use its configuration.
-   Respect configured limits.
-   Support deterministic behaviour in tests through a fixed seed or
    injected random generator.
-   Avoid unapproved business thresholds.
-   Be independently unit-testable.

Document the generation model and its assumptions. Do not describe
random values as physically realistic unless the model justifies that
claim.

### FR-04: Sensor event generation

Each event must contain, at minimum, the fields shown in the supplied
role architecture:

-   Unique event ID
-   Container ID
-   Event timestamp
-   Temperature reading
-   Humidity reading
-   Vibration reading

The following is a proposed example, not a finalized team schema:

``` json
{
  "event_id": "unique-event-identifier",
  "container_id": "CONTAINER_001",
  "event_timestamp": "2026-11-05T10:30:00Z",
  "sensor_data": {
    "temperature": 0.0,
    "humidity": 0.0,
    "vibration": 0.0
  }
}
```

Requirements:

-   Generate a unique ID for each event.
-   Include the correct container identifier.
-   Use timezone-aware UTC timestamps and a consistent ISO 8601
    representation.
-   Keep field names and numeric units consistent.
-   Do not add fields required only by another module unless agreed with
    its owner.
-   Document the schema for Kafka integration.

### FR-05: Validation

Validate events before writing them.

At minimum, validate:

-   Required fields exist.
-   Event ID is present.
-   Container ID is permitted.
-   Timestamp is parseable and timezone-aware.
-   Sensor readings are numeric and finite.
-   Readings satisfy configured limits.
-   Event can be serialized to JSON.

Invalid events must not be written as valid. Provide useful errors and
logging. Document whether an invalid event stops the run or is rejected
and counted.

### FR-06: JSON and JSONL output

Provide:

-   Human-readable console output.
-   Append-only JSONL output, one JSON object per line.
-   Configurable output path.
-   Clear handling of directory creation and file-write errors.
-   Finite-run mode for tests and demonstrations.
-   Continuous generation at the configured interval.

Do not commit large generated telemetry files, logs or temporary outputs
unless the team explicitly requires them. If a small sample is
committed, label it as sample/generated data.

## 6. Engineering requirements

Use professional, maintainable Python practices:

-   Modular separation of concerns
-   Clear names
-   Type hints for public interfaces
-   Docstrings for important modules and public functions
-   Centralized configuration
-   Explicit error handling
-   Structured logging where useful
-   Reproducible tests
-   Minimal, justified dependencies
-   No secrets or credentials in source control
-   No unnecessary frameworks or complexity
-   No hard-coded local absolute paths
-   No hidden network dependency

The simulator should be straightforward to install, run, test and
explain.

## 7. Proposed repository layout

First inspect the existing team repository. Reuse its conventions and
avoid duplicate project roots or parallel structures.

A possible layout, subject to repository review:

``` text
simulation/
    config/
        simulation_config.yaml
    sensors/
        temperature_sensor.py
        humidity_sensor.py
        vibration_sensor.py
    simulator/
        container_simulator.py
    events/
        event_generator.py
        event_schema.py
    validation/
        event_validator.py
    output/
        console_writer.py
        jsonl_writer.py
    tests/
        test_configuration.py
        test_temperature_sensor.py
        test_humidity_sensor.py
        test_vibration_sensor.py
        test_container_simulator.py
        test_event_generator.py
        test_event_validator.py
        test_jsonl_writer.py
    main.py
    requirements.txt
    README.md

docs/
    task_a_part1_simulator_specification.md
```

Final paths and names must follow the existing repository structure and
team agreement.

## 8. Execution expectations

Document:

-   Supported Python version
-   Environment creation and activation
-   Dependency installation
-   Container and sensor configuration
-   Finite simulation command
-   Continuous simulation command
-   Output path selection
-   Random seed usage for reproducible tests
-   Test command
-   Output file location

Verify commands in the actual environment before documenting them as
successful.

## 9. Testing and acceptance criteria

Do not mark the component complete until these have been implemented and
verified.

### Configuration tests

-   Valid configuration loads.
-   Missing required configuration is reported.
-   Invalid intervals and limits are rejected.
-   Container identifiers are handled correctly.

### Sensor tests

-   Each sensor returns a numeric finite value.
-   Configured limits are respected.
-   Invalid limits are rejected.
-   Seeded/injected randomness is reproducible where designed.

### Container and event tests

-   Multiple containers are handled correctly.
-   Each event is associated with the correct container.
-   Required event fields are present.
-   Event IDs are unique across generated events.
-   Timestamps are valid timezone-aware UTC values.
-   Sensor readings are numeric and valid.

### Validation and output tests

-   Invalid events are rejected.
-   Valid events serialize to JSON.
-   JSONL output contains one valid JSON object per line.
-   Multiple writes append correctly.
-   File and directory errors are handled.
-   Console output is understandable.

### Application tests

-   A finite simulation completes successfully.
-   Continuous simulation uses the configured interval and can be
    stopped cleanly.
-   Simulator runs without Kafka or downstream services.
-   Documented commands work in the target environment.

Record actual test commands, pass/fail counts and unresolved issues.
Never invent results.

## 10. Git and GitHub contribution rules

Siddarth's intended branch:

``` text
feature/siddarth-simulation-event-engineering
```

Before creating or switching branches:

1.  Inspect current branch and working tree.
2.  Check for uncommitted work.
3.  Confirm repository and remote.
4.  Do not overwrite or discard another contributor's changes.
5.  Create/use the dedicated branch according to team policy.

Daily workflow:

1.  Implement a meaningful increment.
2.  Run relevant tests.
3.  Review `git status` and the diff.
4.  Check for secrets, credentials, virtual environments, caches and
    unwanted generated data.
5.  Stage only intended files.
6.  Commit with a descriptive message.
7.  Push to Siddarth's branch.
8.  Verify the remote commit.

Do not commit directly to `main`. Do not create empty, duplicate,
cosmetic or misleading commits. Do not claim a push succeeded unless
verified.

The team lead reviews and merges work according to the team's process.
Follow the current Infotact handbook for contribution-day and review
requirements.

## 11. Daily reporting

For each working day, record:

-   Date and daily objective
-   Work actually completed
-   Files created or modified
-   Technical decisions and reasons
-   Commands executed
-   Tests performed and actual results
-   Commit hash and branch
-   Push verification
-   Issues and blockers
-   Next day's objective

Keep progress factual. Do not report planned work as completed.

## 12. Integration contract and Part 2 handover

Before finalizing the event schema, coordinate with the teammate
responsible for Kafka Producer work.

Agree on:

-   Field names
-   Data types
-   Timestamp format and timezone
-   Sensor measurement units
-   Container identifier format
-   Nested versus top-level readings
-   Required metadata

The simulator output should be consumable by a future producer without
changing sensor simulation logic.

For Part 1, provide a documented boundary such as a function or iterator
yielding validated event dictionaries, plus JSONL output. Do not
implement Kafka itself.

Handover package:

-   Source code
-   Configuration example
-   Event schema documentation
-   Sample JSONL output, if approved
-   Test suite and actual results
-   Setup and execution instructions
-   Known limitations
-   Git branch and commit references

## 13. Change-control rules for Antigravity

Before editing:

1.  Read this specification.
2.  Inspect the repository.
3.  Identify existing conventions and relevant files.
4.  Report conflicts, missing information and assumptions.
5.  Propose a staged plan.

During implementation:

-   Work on one stage at a time.
-   Explain the purpose of each change.
-   Do not rewrite unrelated files.
-   Do not implement Task A Part 2.
-   Do not add unapproved requirements.
-   Do not claim tests pass without running them.
-   Show changed files and test results after each stage.

If an important requirement is unclear, ask Siddarth one focused
question and wait rather than inventing a specification.

## 14. Definition of Done

Task A --- Part 1 is complete only when:

-   All in-scope components are implemented.
-   Configuration is validated.
-   The three sensor modules work.
-   Multiple containers can be simulated as configured.
-   Events contain agreed fields and valid timestamps.
-   Event validation works.
-   Console and JSONL output work.
-   Finite and continuous execution modes work as specified.
-   Automated tests pass, with results recorded.
-   Setup and usage documentation is complete.
-   Simulator runs independently of Kafka.
-   Event contract is documented and ready for teammate integration.
-   Verified work is pushed to Siddarth's dedicated GitHub branch.
-   Remaining limitations are disclosed.

**Final scope reminder:** This document governs Siddarth's Python Sensor
Simulator (Task A --- Part 1). Apache Kafka Producer (Task A --- Part 2)
remains outside the current scope unless the team explicitly reallocates
it.
