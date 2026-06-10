"""SQL-first migration discovery and execution helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import re
from pathlib import Path
from typing import Iterable

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

from YM_data_collection.utils.constants import SCHEMA_MIGRATIONS_TABLE


MIGRATION_FILENAME_PATTERN = re.compile(r"^(?P<version>\d{3})_(?P<name>[a-z0-9_]+)\.sql$")


class MigrationError(Exception):
    """Base migration runner failure."""


class MigrationDefinitionError(MigrationError):
    """Raised when migration files are malformed."""


class MigrationChecksumError(MigrationError):
    """Raised when a previously applied migration has changed."""


class MigrationExecutionError(MigrationError):
    """Raised when a migration cannot be executed successfully."""


class MigrationStateError(MigrationError):
    """Raised when migration state in the database is invalid."""


@dataclass(frozen=True)
class MigrationFile:
    """A versioned SQL migration file."""

    version: str
    name: str
    path: Path
    checksum: str
    sql_text: str


@dataclass(frozen=True)
class AppliedMigrationRecord:
    """A stored migration execution record."""

    version: str
    name: str
    checksum: str
    status: str
    executed_at_utc: datetime | None


@dataclass(frozen=True)
class MigrationRunSummary:
    """Migration execution or check summary."""

    total_migrations: int
    applied_versions: list[str]
    pending_versions: list[str]
    applied_now: list[str]
    check_only: bool

    @property
    def schema_up_to_date(self) -> bool:
        """Whether there are no remaining pending migrations."""

        return not self.pending_versions


def compute_checksum(sql_text: str) -> str:
    """Return a stable checksum for a migration file."""

    return hashlib.sha256(sql_text.encode("utf-8")).hexdigest()


def parse_migration_filename(path: Path) -> tuple[str, str]:
    """Parse the migration version and name from a filename."""

    match = MIGRATION_FILENAME_PATTERN.match(path.name)
    if match is None:
        raise MigrationDefinitionError(f"Invalid migration filename: {path.name}")
    return match.group("version"), match.group("name")


def load_migration_files(migrations_dir: str | Path) -> list[MigrationFile]:
    """Load and validate all versioned SQL migration files."""

    directory = Path(migrations_dir)
    if not directory.exists():
        raise MigrationDefinitionError(f"Migration directory not found: {directory}")

    migrations: list[MigrationFile] = []
    seen_versions: set[str] = set()
    for path in sorted(directory.glob("*.sql")):
        version, name = parse_migration_filename(path)
        if version in seen_versions:
            raise MigrationDefinitionError(f"Duplicate migration version detected: {version}")
        sql_text = path.read_text(encoding="utf-8").strip()
        if not sql_text:
            raise MigrationDefinitionError(f"Migration file is empty: {path}")
        migrations.append(
            MigrationFile(
                version=version,
                name=name,
                path=path,
                checksum=compute_checksum(sql_text),
                sql_text=sql_text,
            )
        )
        seen_versions.add(version)

    if not migrations:
        raise MigrationDefinitionError(f"No SQL migration files found in: {directory}")
    if migrations[0].version != "000":
        raise MigrationDefinitionError("Bootstrap migration 000_schema_migrations.sql is required")

    return migrations


def split_sql_statements(sql_text: str) -> list[str]:
    """Split a migration file into executable SQL statements."""

    statements: list[str] = []
    buffer: list[str] = []
    for raw_line in sql_text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(raw_line)
        if stripped.endswith(";"):
            statement = "\n".join(buffer).strip()
            statements.append(statement[:-1].strip())
            buffer = []

    if buffer:
        raise MigrationDefinitionError("SQL migration must terminate every statement with ';'")

    return [statement for statement in statements if statement]


def table_exists(connection: Connection, table_name: str) -> bool:
    """Check whether a table exists in the current database."""

    return inspect(connection).has_table(table_name)


def fetch_applied_migrations(connection: Connection) -> dict[str, AppliedMigrationRecord]:
    """Fetch migration execution records keyed by version."""

    if not table_exists(connection, SCHEMA_MIGRATIONS_TABLE):
        return {}

    rows = connection.execute(
        text(
            """
            SELECT version, name, checksum, status, executed_at_utc
            FROM schema_migrations
            ORDER BY version
            """
        )
    )
    records: dict[str, AppliedMigrationRecord] = {}
    for row in rows:
        mapping = row._mapping
        records[mapping["version"]] = AppliedMigrationRecord(
            version=mapping["version"],
            name=mapping["name"],
            checksum=mapping["checksum"],
            status=mapping["status"],
            executed_at_utc=mapping["executed_at_utc"],
        )
    return records


def plan_pending_migrations(
    migrations: Iterable[MigrationFile],
    applied_records: dict[str, AppliedMigrationRecord],
) -> list[MigrationFile]:
    """Determine which migrations still need to run."""

    pending: list[MigrationFile] = []
    for migration in migrations:
        record = applied_records.get(migration.version)
        if record is None:
            pending.append(migration)
            continue

        if record.name != migration.name:
            raise MigrationStateError(
                f"Applied migration name mismatch for version {migration.version}: "
                f"db={record.name} file={migration.name}"
            )
        if record.checksum != migration.checksum:
            raise MigrationChecksumError(
                f"Checksum mismatch for migration {migration.version}_{migration.name}.sql"
            )
        if record.status == "success":
            continue
        if record.status == "failed":
            pending.append(migration)
            continue
        raise MigrationStateError(
            f"Unsupported migration status for version {migration.version}: {record.status}"
        )
    return pending


def upsert_migration_record(connection: Connection, migration: MigrationFile, status: str) -> None:
    """Insert or update a migration execution record."""

    executed_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    exists = connection.execute(
        text("SELECT id FROM schema_migrations WHERE version = :version"),
        {"version": migration.version},
    ).first()

    params = {
        "version": migration.version,
        "name": migration.name,
        "checksum": migration.checksum,
        "status": status,
        "executed_at_utc": executed_at_utc,
    }
    if exists is None:
        connection.execute(
            text(
                """
                INSERT INTO schema_migrations (
                    version,
                    name,
                    checksum,
                    executed_at_utc,
                    status
                ) VALUES (
                    :version,
                    :name,
                    :checksum,
                    :executed_at_utc,
                    :status
                )
                """
            ),
            params,
        )
        return

    connection.execute(
        text(
            """
            UPDATE schema_migrations
            SET name = :name,
                checksum = :checksum,
                executed_at_utc = :executed_at_utc,
                status = :status
            WHERE version = :version
            """
        ),
        params,
    )


def execute_sql_statements(connection: Connection, migration: MigrationFile) -> None:
    """Execute all SQL statements contained in one migration file."""

    for statement in split_sql_statements(migration.sql_text):
        connection.exec_driver_sql(statement)


def ensure_bootstrap_schema_table(engine: Engine, bootstrap_migration: MigrationFile) -> bool:
    """Create schema_migrations if it does not exist yet."""

    with engine.begin() as connection:
        if table_exists(connection, SCHEMA_MIGRATIONS_TABLE):
            return False

        execute_sql_statements(connection, bootstrap_migration)
        upsert_migration_record(connection, bootstrap_migration, status="success")
        return True


def apply_single_migration(engine: Engine, migration: MigrationFile) -> None:
    """Execute one migration and persist its final status."""

    try:
        with engine.begin() as connection:
            execute_sql_statements(connection, migration)
            upsert_migration_record(connection, migration, status="success")
    except Exception as exc:
        try:
            with engine.begin() as connection:
                if table_exists(connection, SCHEMA_MIGRATIONS_TABLE):
                    upsert_migration_record(connection, migration, status="failed")
        except Exception:
            pass
        raise MigrationExecutionError(
            f"Failed to execute migration {migration.version}_{migration.name}.sql"
        ) from exc


def run_migrations(engine: Engine, migrations_dir: str | Path, *, apply: bool) -> MigrationRunSummary:
    """Check or apply migrations against the target database."""

    migrations = load_migration_files(migrations_dir)
    bootstrap_migration = migrations[0]
    bootstrap_applied = False

    if apply:
        bootstrap_applied = ensure_bootstrap_schema_table(engine, bootstrap_migration)

    with engine.connect() as connection:
        applied_records = fetch_applied_migrations(connection)
    pending = plan_pending_migrations(migrations, applied_records)

    if not apply:
        return MigrationRunSummary(
            total_migrations=len(migrations),
            applied_versions=sorted(
                version for version, record in applied_records.items() if record.status == "success"
            ),
            pending_versions=[migration.version for migration in pending],
            applied_now=[],
            check_only=True,
        )

    applied_now: list[str] = []
    if bootstrap_applied:
        applied_now.append(bootstrap_migration.version)
    for migration in pending:
        apply_single_migration(engine, migration)
        applied_now.append(migration.version)

    with engine.connect() as connection:
        final_records = fetch_applied_migrations(connection)
    final_pending = plan_pending_migrations(migrations, final_records)
    return MigrationRunSummary(
        total_migrations=len(migrations),
        applied_versions=sorted(
            version for version, record in final_records.items() if record.status == "success"
        ),
        pending_versions=[migration.version for migration in final_pending],
        applied_now=applied_now,
        check_only=False,
    )


def summarize_run(summary: MigrationRunSummary) -> str:
    """Return a compact human-readable result summary."""

    if summary.schema_up_to_date:
        if summary.check_only:
            return f"schema up to date: {len(summary.applied_versions)}/{summary.total_migrations} migrations applied"
        if summary.applied_now:
            applied = ",".join(summary.applied_now)
            return f"applied migrations: {applied}; schema up to date"
        return "no new migrations applied; schema already up to date"

    pending = ",".join(summary.pending_versions)
    if summary.check_only:
        return f"pending migrations detected: {pending}"
    return f"schema still has pending migrations: {pending}"
