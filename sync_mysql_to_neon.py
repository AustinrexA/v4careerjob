"""Incrementally sync V4Career MySQL rows into an existing Neon database.

Default mode is DRY RUN. Pass --apply to make changes.
Requires DB_* for MySQL and NEON_* for PostgreSQL in the project .env.
"""
import argparse
import json
import os
import sys
from collections import Counter

import mysql.connector
import psycopg
from psycopg.types.json import Jsonb
from dotenv import load_dotenv

load_dotenv()


def mysql_connection():
    required = ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing MySQL settings: " + ", ".join(missing))
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.getenv("DB_PORT", "3306")),
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connection_timeout=10,
    )


def neon_connection():
    required = ("NEON_HOST", "NEON_DB", "NEON_USER", "NEON_PASSWORD")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing Neon settings: " + ", ".join(missing))
    return psycopg.connect(
        host=os.environ["NEON_HOST"],
        port=int(os.getenv("NEON_PORT", "5432")),
        dbname=os.environ["NEON_DB"],
        user=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        sslmode=os.getenv("NEON_SSLMODE", "require"),
        connect_timeout=15,
    )


def fetch_all(conn, sql, params=()):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        cur.close()


def count(conn, table, dialect):
    # MySQL uses backticks for identifiers; PostgreSQL uses double quotes.
    if table not in {"sources", "jobs", "collection_runs"}:
        raise ValueError(f"Unsupported table: {table}")
    quoted = f"`{table}`" if dialect == "mysql" else f'"{table}"'
    return fetch_all(conn, f"SELECT COUNT(*) FROM {quoted}")[0][0]


def normalize_skills(value):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            # Keep non-JSON legacy skills as a JSON string.
            pass
    return Jsonb(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Commit changes. Without this flag, only report counts.")
    args = parser.parse_args()
    my = pg = None
    try:
        my = mysql_connection()
        pg = neon_connection()

        mysql_counts = {t: count(my, t, "mysql") for t in ("sources", "jobs", "collection_runs")}
        neon_counts = {t: count(pg, t, "postgres") for t in ("sources", "jobs", "collection_runs")}
        print("Current counts:")
        for table in ("sources", "jobs", "collection_runs"):
            print(f"  {table}: MySQL={mysql_counts[table]}, Neon={neon_counts[table]}")

        mysql_sources = fetch_all(my, "SELECT id, name, base_url, source_type, is_enabled, created_at, updated_at FROM sources ORDER BY id")
        neon_sources = fetch_all(pg, "SELECT id, name, base_url, source_type, is_enabled, created_at, updated_at FROM sources ORDER BY id")
        neon_by_name = {}
        for row in neon_sources:
            key = (row[1] or "").strip().casefold()
            if key in neon_by_name:
                raise RuntimeError(f"Neon has ambiguous source names after case-folding: {row[1]!r}. Resolve these before syncing.")
            neon_by_name[key] = row

        source_map = {}
        missing_sources = []
        for row in mysql_sources:
            old_id, name = row[0], row[1]
            key = (name or "").strip().casefold()
            if key in neon_by_name:
                source_map[old_id] = neon_by_name[key][0]
            else:
                missing_sources.append(row)

        # Load source/job rows from MySQL. No data is changed in dry-run mode.
        job_cols = "id, source_id, external_id, url_hash, title, company, location, experience, salary_min, salary_max, salary_currency, salary_period, skills, description, job_url, posted_at, first_seen_at, last_seen_at, status, created_at, updated_at"
        mysql_jobs = fetch_all(my, f"SELECT {job_cols} FROM jobs ORDER BY id")
        run_cols = "id, source_id, started_at, finished_at, status, records_found, records_inserted, records_updated, error_message"
        mysql_runs = fetch_all(my, f"SELECT {run_cols} FROM collection_runs ORDER BY id")

        # Estimate job inserts by checking Neon uniqueness keys with mapped source IDs.
        neon_job_keys = fetch_all(pg, "SELECT source_id, external_id, url_hash FROM jobs")
        neon_by_source_url = {(r[0], r[2]) for r in neon_job_keys if r[2] is not None}
        neon_by_source_external = {(r[0], r[1]) for r in neon_job_keys if r[1] is not None}
        estimated_job_inserts = 0
        estimated_job_duplicates = 0
        for row in mysql_jobs:
            old_source_id, external_id, url_hash = row[1], row[2], row[3]
            mapped_source_id = source_map.get(old_source_id)
            if mapped_source_id is None:
                continue
            conflict_url = (mapped_source_id, url_hash) in neon_by_source_url
            conflict_external = external_id is not None and (mapped_source_id, external_id) in neon_by_source_external
            if conflict_url or conflict_external:
                estimated_job_duplicates += 1
            else:
                estimated_job_inserts += 1
                # PostgreSQL UNIQUE constraints permit multiple NULL values, so
                # only track non-NULL URL hashes as uniqueness keys.
                if url_hash is not None:
                    neon_by_source_url.add((mapped_source_id, url_hash))
                if external_id is not None:
                    neon_by_source_external.add((mapped_source_id, external_id))

        # Estimate duplicate run records using the same comparison as the apply loop.
        existing_run_keys = set(fetch_all(pg, "SELECT source_id, started_at, status, records_found, records_inserted, records_updated FROM collection_runs"))
        estimated_run_inserts = 0
        estimated_run_duplicates = 0
        for row in mysql_runs:
            old_source_id, started_at, status, found, inserted, updated = row[1], row[2], row[4], row[5], row[6], row[7]
            mapped_source_id = source_map.get(old_source_id)
            # Match the values used by the insert path, where nullable counts are
            # normalized to zero and a missing status becomes "unknown".
            normalized_status = status or "unknown"
            normalized_found = found or 0
            normalized_inserted = inserted or 0
            normalized_updated = updated or 0
            key = (mapped_source_id, started_at, normalized_status, normalized_found, normalized_inserted, normalized_updated)
            if key in existing_run_keys:
                estimated_run_duplicates += 1
            else:
                estimated_run_inserts += 1
                existing_run_keys.add(key)

        if not args.apply:
            print("\nDRY RUN ONLY — no changes were made.")
            print(f"  Sources requiring insertion: {len(missing_sources)}")
            print(f"  MySQL job rows examined: {len(mysql_jobs)}")
            print(f"  Estimated jobs to insert: {estimated_job_inserts}")
            print(f"  Estimated jobs skipped as duplicates: {estimated_job_duplicates}")
            print(f"  MySQL collection runs examined: {len(mysql_runs)}")
            print(f"  Estimated collection runs to insert: {estimated_run_inserts}")
            print(f"  Estimated collection runs skipped as duplicates: {estimated_run_duplicates}")
            print("These are estimates based on source-name mapping and Neon uniqueness keys; no changes were made.")
            print("Review these estimates before rerunning with --apply.")
            return

        # Transactional incremental sync. Existing Neon jobs are never updated or deleted.
        with pg.transaction():
            with pg.cursor() as cur:
                for row in missing_sources:
                    _, name, base_url, source_type, enabled, created_at, updated_at = row
                    cur.execute(
                        """INSERT INTO sources (name, base_url, source_type, is_enabled, created_at, updated_at)
                           VALUES (%s, %s, %s, %s, COALESCE(%s, CURRENT_TIMESTAMP), COALESCE(%s, CURRENT_TIMESTAMP))
                           ON CONFLICT (name) DO NOTHING RETURNING id""",
                        (name, base_url, source_type or "job_website", bool(enabled), created_at, updated_at),
                    )
                    inserted = cur.fetchone()
                    if inserted:
                        source_map[row[0]] = inserted[0]
                    else:
                        cur.execute("SELECT id FROM sources WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s)) ORDER BY id LIMIT 1", (name,))
                        found = cur.fetchone()
                        if not found:
                            raise RuntimeError(f"Could not map source {name!r} after insert attempt")
                        source_map[row[0]] = found[0]

                unmapped = [row for row in mysql_sources if row[0] not in source_map]
                if unmapped:
                    raise RuntimeError("Some MySQL source IDs could not be mapped; transaction aborted.")

                inserted_jobs = 0
                skipped_jobs = 0
                for row in mysql_jobs:
                    (_, old_source_id, external_id, url_hash, title, company, location, experience,
                     salary_min, salary_max, salary_currency, salary_period, skills, description,
                     job_url, posted_at, first_seen_at, last_seen_at, status, created_at, updated_at) = row
                    cur.execute(
                        """INSERT INTO jobs (
                               source_id, external_id, url_hash, title, company, location, experience,
                               salary_min, salary_max, salary_currency, salary_period, skills, description,
                               job_url, posted_at, first_seen_at, last_seen_at, status, created_at, updated_at
                           ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT DO NOTHING RETURNING id""",
                        (source_map[old_source_id], external_id, url_hash, title, company, location, experience,
                         salary_min, salary_max, salary_currency or "INR", salary_period or "year",
                         normalize_skills(skills), description, job_url, posted_at, first_seen_at,
                         last_seen_at, status or "unknown", created_at, updated_at),
                    )
                    if cur.fetchone():
                        inserted_jobs += 1
                    else:
                        skipped_jobs += 1

                inserted_runs = 0
                skipped_runs = 0
                for row in mysql_runs:
                    (_, old_source_id, started_at, finished_at, status, records_found,
                     records_inserted, records_updated, error_message) = row
                    # Collection runs have no natural unique key. Avoid exact duplicates on source/time/status/counts.
                    cur.execute(
                        """SELECT 1 FROM collection_runs
                           WHERE source_id=%s AND started_at IS NOT DISTINCT FROM %s
                             AND status IS NOT DISTINCT FROM %s
                             AND records_found=%s AND records_inserted=%s AND records_updated=%s
                           LIMIT 1""",
                        (source_map[old_source_id], started_at, status, records_found, records_inserted, records_updated),
                    )
                    if cur.fetchone():
                        skipped_runs += 1
                        continue
                    cur.execute(
                        """INSERT INTO collection_runs
                           (source_id, started_at, finished_at, status, records_found, records_inserted, records_updated, error_message)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (source_map[old_source_id], started_at, finished_at, status or "unknown",
                         records_found or 0, records_inserted or 0, records_updated or 0, error_message),
                    )
                    inserted_runs += 1

                for table in ("sources", "jobs", "collection_runs"):
                    cur.execute(
                        "SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE((SELECT MAX(id) FROM " + table + "), 1), EXISTS (SELECT 1 FROM " + table + "))",
                        (f"public.{table}",),
                    )

        print("\nSync committed successfully.")
        print(f"  New source rows: {len(missing_sources)} (source names mapped case-insensitively)")
        print(f"  Jobs inserted: {inserted_jobs}; jobs skipped as duplicates: {skipped_jobs}")
        print(f"  Collection runs inserted: {inserted_runs}; exact duplicates skipped: {skipped_runs}")
        print("\nVerified counts:")
        for table in ("sources", "jobs", "collection_runs"):
            print(f"  {table}: {count(my, table, 'mysql')} MySQL; {count(pg, table, 'postgres')} Neon")
        print("Keep the MySQL database as a backup until you validate the website and counts.")

    except Exception as exc:
        print(f"\nSync failed: {type(exc).__name__}: {exc}")
        sys.exit(1)
    finally:
        if my is not None:
            my.close()
        if pg is not None:
            pg.close()


if __name__ == "__main__":
    main()
