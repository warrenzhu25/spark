# Repository Guidelines

## Project Structure & Modules
- Core Scala sources live under `core/src/main/scala`; tests mirror in `core/src/test/scala`.
- Protobufs: `core/src/main/protobuf`.
- SQL, streaming, MLlib, GraphX modules follow the same `src/main` / `src/test` layout under their respective top-level dirs.
- Docs live in `docs/`; scripts in `bin/`, `sbin/`; build logic in the root `pom.xml`, `build/`, and `project/`.

## Build, Test, and Development Commands
- Compile/packaging: `./build/sbt -Phadoop-3.3 package` (per-module) or `-Phadoop-3.3 assembly` (fat jars).
- Quick core tests: `./build/sbt -Phadoop-3.3 "core/testOnly org.apache.spark.shuffle.ShuffleFetchWaitStatsAggregatorSuite"`.
- Full test sweep (slow): `./build/sbt -Phadoop-3.3 test` or `./build/mvn -DskipTests` to build without tests.
- Protobuf regen (if proto changes): `./build/sbt -Phadoop-3.3 "core/compile"`.

## Coding Style & Naming
- Scala: 2-space indentation, avoid tabs; prefer CamelCase types and lowerCamelCase methods/vals.
- Follow existing Spark patterns: immutable vals where possible, `Option` over nulls, and explicit types for public APIs.
- Lint/format: `scalastyle-config.xml` drives style checks; run `./dev/scalastyle` if making broad changes.

## Testing Guidelines
- Tests use ScalaTest; name suites `*Suite.scala` and keep fixtures local to the suite.
- For new features, add focused unit tests alongside implementation (e.g., `core/src/test/scala/...`).
- When touching serde/proto or JSON paths, add/update round-trip tests (e.g., `KVStoreProtobufSerializerSuite`, `JsonProtocolSuite`).
- After every change, ensure targeted tests and `dev/scalastyle` both pass.

## Commit & Pull Request Guidelines
- Commit messages: short, imperative summaries (e.g., “add protobuf serialization”); include scope when possible.
- Pull requests: describe behavior changes, configs, and user-facing impacts; link issues; include test commands/output for relevant modules.
- Update docs (`docs/`) and configs when changing public APIs or metrics; keep changelog notes concise where applicable.
