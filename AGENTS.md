# Repository Guidelines

## Project Structure & Module Organization
- Source code: `src/main/java/io/debezium/connector/kingbasees/**`
- Resources: `src/main/resources/**` (e.g., build metadata)
- Tests & demos: `src/test/java/` (see `KingbaseTest.java` integration-style example)
- Build output: `target/`

## Build, Test, and Development Commands
- Build (skip tests): `mvn clean package -DskipTests`
- Run tests: `mvn test` (add unit tests under `src/test/java`)
- Run the demo: open `KingbaseTest` in your IDE and run its `main`. Configure connection props for your local KingbaseES before running. Do not commit real credentials.

## Coding Style & Naming Conventions
- Language: Java 8; 4‑space indentation; UTF‑8 source files.
- Packages: lower case (e.g., `io.debezium.connector.kingbasees`).
- Classes/Interfaces: UpperCamelCase; methods/fields: lowerCamelCase; constants: UPPER_SNAKE_CASE.
- Keep lines reasonably short and imports organized; prefer meaningful names over abbreviations.
- Follow Debezium patterns when adding connectors, decoders, or snapshotters (see packages `connection/**`, `snapshot/**`).

## Testing Guidelines
- Prefer fast, isolated unit tests for converters, schema, and message decoding.
- Place tests under `src/test/java` mirroring package structure; name files `*Test.java`.
- Integration tests that require a running KingbaseES instance should be explicitly opt‑in (e.g., disabled by default or clearly documented). Document required env vars (host, port, user, db) in the test class Javadoc.

## Commit & Pull Request Guidelines
- Use Conventional Commits where possible: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`.
- Keep subjects concise and imperative; include scope when helpful (e.g., `feat(snapshot): add exported snapshotter`).
- PRs should include: a clear description, rationale, sample config or reproduction steps, and updates to docs if user‑visible. Ensure `mvn clean package` and `mvn test` pass locally.

## Security & Configuration Tips
- Never commit real database hosts, usernames, passwords, or offset/history file paths. Use placeholders or environment variables (e.g., `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`).
- If adding example configs, place redacted examples and mark real local files in `.gitignore`.

## Agent‑Specific Notes
- Do not change the base package `io.debezium.connector.kingbasees` or public APIs without careful review.
- When adding new types or decoders, update the relevant registry and keep behavior consistent with existing Debezium connectors.
- The KingBase SQL sync service now supports chunked/streamed execution, worker-pool parallelism, and persistent cursors. Observe the configuration defaults in `ApplicationProperties.KingBase` when adding new statements.
- SQL statements can be supplied inline or via `tap.kingbase.sql-statements-file` (YAML map keyed by statement name). Prefer external files for large statement sets and document any non-default chunk/fetch sizes.
- Startup triggers enqueue a one-time sync on the same executor as scheduled runs; avoid blocking calls inside `KingBaseSqlSyncService` to keep the scheduler responsive.
- Each statement is guarded by its own execution lock. If a long-running statement is still processing when the cron fires again, the service skips only that statement while continuing to dispatch the rest.
- Use `tap.kingbase.sql-statement-groups` to give heavy tables their own YAML file and cron expression. Group keys are prefixed into the statement cursor so state tracking remains independent across groups.
- When paths are declared with `classpath:` the runtime still checks for a file of the same name next to the deployed JAR; place editable copies there for hot overrides.
- Cursor type: statements support numeric or string cursors. Set `id-type: number|string` per statement in YAML. The sync state table now includes a `cursor_value` column; code keeps backward compatibility with the legacy numeric `last_id`.
- API endpoints for ops: `POST /kingbase/sync`, `DELETE /kingbase/sync-state`, `PUT /kingbase/sync-groups/refresh`.
- Log sampling: per-document queue logs in `ElasticsearchService` are sampled to reduce noise. Control with JVM property `-Dtap.es.logEvery=<N>` (default 1000). Raise `N` in production to further quiet logs, or set the class log level to INFO.
- Elasticsearch bulk tuning: expose `tap.elasticsearch.bulk-size`, `tap.elasticsearch.bulk-flush-interval-seconds`, and `tap.elasticsearch.bulk-threads` (overridable via environment, system properties, or an `elasticsearch.yml` next to the JAR). KingBase connection values can likewise be overridden via `kingbase.yml`. Bulk debug logs now summarize operations per index.
