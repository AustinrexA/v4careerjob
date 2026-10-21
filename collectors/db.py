"""Database configuration and driver-neutral helpers for V4Career."""
import os
from dotenv import load_dotenv

load_dotenv()

DB_ENGINE = os.getenv("DB_ENGINE", "postgres").strip().lower()
if DB_ENGINE in {"postgresql", "neon"}:
    DB_ENGINE = "postgres"
if DB_ENGINE not in {"mysql", "postgres"}:
    raise RuntimeError("DB_ENGINE must be 'mysql' or 'postgres'.")

# The tuple is used by Flask routes to catch driver-specific database errors.
_errors = []
try:
    from psycopg import Error as PsycopgError
    _errors.append(PsycopgError)
except ImportError:
    pass
try:
    from mysql.connector import Error as MySQLError
    _errors.append(MySQLError)
except ImportError:
    pass
DATABASE_ERRORS = tuple(_errors) or (Exception,)


def is_mysql():
    return DB_ENGINE == "mysql"


def get_connection():
    """Open a connection using the selected engine's environment settings."""
    if is_mysql():
        required = ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD")
        missing = [key for key in required if not os.getenv(key)]
        if missing:
            raise RuntimeError("Missing MySQL configuration: " + ", ".join(missing))
        try:
            import mysql.connector
        except ImportError as exc:
            raise RuntimeError("MySQL selected but mysql-connector-python is not installed.") from exc
        kwargs = {
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT", "3306")),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "connection_timeout": 10,
            "autocommit": False,
        }
        if os.getenv("DB_SSL_DISABLED", "false").lower() != "true":
            # Connector negotiates TLS with hosted providers; local servers may opt out.
            kwargs["ssl_disabled"] = False
        return mysql.connector.connect(**kwargs)

    required = ("NEON_HOST", "NEON_DB", "NEON_USER", "NEON_PASSWORD")
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing PostgreSQL configuration: " + ", ".join(missing))
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("PostgreSQL selected but psycopg is not installed.") from exc
    return psycopg.connect(
        host=os.getenv("NEON_HOST"),
        port=int(os.getenv("NEON_PORT", "5432")),
        dbname=os.getenv("NEON_DB"),
        user=os.getenv("NEON_USER"),
        password=os.getenv("NEON_PASSWORD"),
        sslmode=os.getenv("NEON_SSLMODE", "require"),
        connect_timeout=10,
    )


def column_names(cursor):
    """Return column names across psycopg and mysql-connector cursor metadata."""
    return [(column.name if hasattr(column, "name") else column[0]) for column in (cursor.description or [])]
