#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Modern PostgreSQL connection manager with logging, retry mechanism, and Unicode support.
"""

import functools
import logging
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def retry_on_connection_failure(retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator for retrying operations on connection failures.

    Args:
        retries: Number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier for delay
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            current_delay = delay
            last_exception = None

            for attempt in range(retries + 1):
                try:
                    return func(self, *args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    last_exception = e

                    if attempt == retries:
                        logger.error("Operation failed after %d attempts: %s", retries, e)
                        raise

                    logger.warning("Connection error (attempt %d/%d): %s",
                                   attempt + 1, retries, e)
                    time.sleep(current_delay)
                    current_delay *= backoff

                    # Try to reconnect
                    try:
                        self.reconnect()
                    except psycopg2.Error as reconnect_error:
                        logger.error("Reconnection failed: %s", reconnect_error)

            # This should never be reached, but for type safety

            if last_exception:
                raise last_exception
            raise psycopg2.OperationalError("Operation failed without specific exception")

        return wrapper

    return decorator


class UnicodeLoggingCursor(psycopg2.extras.LoggingCursor):
    """
    Custom cursor that logs queries and results with proper Unicode support.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(f"{__name__}.cursor")

    def _log(self, message: str, level: int = logging.DEBUG):
        """Helper method for logging with proper encoding."""
        self.logger.log(level, message)

    def execute(self, query, vars=None):
        # pylint: disable=redefined-builtin
        """
        Execute a query with logging.

        Args:
            query: SQL query string
            vars: Query parameters
        """
        self._log(f"Executing query: {query}")

        if vars:
            self._log(f"Query parameters: {vars}")

        try:
            result = super().execute(query, vars)
            self._log("Query executed successfully")

            return result
        except Exception as e:  # pylint: disable=broad-except
            self._log(f"Query failed: {e}", logging.ERROR)
            raise

    def fetchone(self):
        """Fetch one row and log the result."""
        fetched_result = super().fetchone()
        self._log(f"Fetchone result: {fetched_result}")

        return fetched_result

    def fetchmany(self, size: int):
        """Fetch multiple rows and log the results."""
        fetched_result = super().fetchmany(size)
        self._log(f"Fetchmany result: {fetched_result}")

        return fetched_result

    def fetchall(self):
        """Fetch all rows and log the results."""
        fetched_result = super().fetchall()
        self._log(f"Fetchall result: {fetched_result}")

        return fetched_result


class UnicodeLoggingConnection(psycopg2.extras.LoggingConnection):
    """
    Custom connection that uses UnicodeLoggingCursor.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(f"{__name__}.connection")

    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', UnicodeLoggingCursor)

        return super().cursor(*args, **kwargs)


class PostgresManager:
    """
    A robust PostgreSQL connection manager with logging, retry mechanism, and Unicode support.
    """

    def __init__(self,
                 connection_params: Dict[str, Any],
                 autocommit: bool = False,
                 log_queries: bool = True,
                 log_results: bool = False):
        """
        Initialize the PostgreSQL connection manager.

        Args:
            connection_params: Dictionary with PostgreSQL connection parameters
            autocommit: Enable autocommit mode
            log_queries: Enable query logging
            log_results: Enable result logging (can be verbose)
        """
        # Set default values for optional parameters
        connection_params.setdefault('port', 5432)
        connection_params.setdefault('connect_timeout', 10)

        # Set database to user if not provided

        if 'database' not in connection_params and 'user' in connection_params:
            connection_params['database'] = connection_params['user']

        # Remove password if it's None or empty string

        if 'password' in connection_params and not connection_params['password']:
            del connection_params['password']

        self.connection_params = connection_params
        self.autocommit = autocommit
        self.log_queries = log_queries
        self.log_results = log_results
        self.connection = None
        self._is_connected = False

        # Configure connection factory based on logging settings

        if log_queries or log_results:
            self.connection_factory = UnicodeLoggingConnection
        else:
            self.connection_factory = psycopg2.extensions.connection

        self.connect()

    @classmethod
    def from_individual_params(cls,
                               host: str,
                               user: str,
                               database: str = None,
                               port: int = 5432,
                               autocommit: bool = False,
                               log_queries: bool = True,
                               log_results: bool = False,
                               **kwargs):
        # pylint: disable=too-many-arguments
        """
        Alternative constructor for backward compatibility.

        Args:
            host: PostgreSQL host
            user: PostgreSQL user
            database: Database name (defaults to user name if not provided)
            port: PostgreSQL port
            autocommit: Enable autocommit mode
            log_queries: Enable query logging
            log_results: Enable result logging
            **kwargs: Additional connection parameters
        """
        connection_params = {
            'host': host,
            'user': user,
            'database': database or user,
            'port': port,
            **kwargs
        }

        return cls(
            connection_params=connection_params,
            autocommit=autocommit,
            log_queries=log_queries,
            log_results=log_results
        )

    def connect(self) -> bool:
        """Establish a connection to PostgreSQL."""
        try:
            if self.log_queries or self.log_results:
                self.connection = psycopg2.connect(
                    connection_factory=self.connection_factory,
                    **self.connection_params
                )
                # Initialize logging for the connection

                if self.log_queries or self.log_results:
                    self.connection.initialize(logger)
            else:
                self.connection = psycopg2.connect(**self.connection_params)

            self.connection.autocommit = self.autocommit
            self._is_connected = True
            logger.info("Connected to PostgreSQL database %s on %s:%s",
                        self.connection_params.get('database', 'unknown'),
                        self.connection_params.get('host', 'unknown'),
                        self.connection_params.get('port', 'unknown'))

            return True

        except psycopg2.Error as e:
            logger.error("Connection failed: %s", e)
            self._is_connected = False
            raise

    def reconnect(self) -> bool:
        """Reconnect to PostgreSQL."""
        self.close()

        return self.connect()

    def close(self):
        """Close the connection."""

        if self.connection and not self.connection.closed:
            self.connection.close()
            self._is_connected = False
            logger.info("Connection closed")

    def is_connected(self) -> bool:
        """Check if the connection is active."""

        if self._is_connected and self.connection and not self.connection.closed:
            try:
                with self.connection.cursor() as cur:
                    cur.execute('SELECT 1')

                return True
            except psycopg2.Error:
                self._is_connected = False

        return self._is_connected

    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """
        Get a cursor context manager with automatic cleanup.

        Args:
            cursor_factory: Cursor factory (e.g., psycopg2.extras.DictCursor,
                           psycopg2.extras.RealDictCursor)

        Yields:
            A cursor object
        """

        if not self.is_connected():
            self.reconnect()

        cursor = self.connection.cursor(
            cursor_factory=cursor_factory or psycopg2.extras.DictCursor
        )

        try:
            yield cursor
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error("Transaction rolled back due to error: %s", e)
            raise
        finally:
            cursor.close()

    @retry_on_connection_failure()
    def execute(self,
                query_str: Union[str, sql.Composed],
                params: Optional[Union[Dict, List, Tuple]] = None,
                cursor_factory=None) -> Optional[Any]:
        """
        Execute a query and return the result.

        Args:
            query_str: SQL query to execute
            params: Query parameters
            cursor_factory: Cursor factory

        Returns:
            Query result or None for commands that don't return results
        """
        with self.get_cursor(cursor_factory) as cur:
            cur.execute(query_str, params)

            if cur.description:  # Query returns results
                return cur.fetchall()

            return None

    @retry_on_connection_failure()
    def execute_many(self,
                     query_str: Union[str, sql.Composed],
                     params_list: List[Union[Dict, List, Tuple]],
                     cursor_factory=None) -> None:
        """
        Execute a query with multiple parameter sets.

        Args:
            query_str: SQL query to execute
            params_list: List of parameter sets
            cursor_factory: Cursor factory
        """
        with self.get_cursor(cursor_factory) as cur:
            cur.executemany(query_str, params_list)

    @retry_on_connection_failure()
    def copy_from(self, file_obj, table, **kwargs):
        """
        Copy data from a file-like object to a table.

        Args:
            file_obj: File-like object to read from
            table: Table name to copy to
            **kwargs: Additional copy parameters
        """
        with self.get_cursor() as cur:
            cur.copy_from(file=file_obj, table=table, **kwargs)

    @retry_on_connection_failure()
    def copy_to(self, file_obj, table, **kwargs):
        """
        Copy data from a table to a file-like object.

        Args:
            file_obj: File-like object to write to
            table: Table name to copy from
            **kwargs: Additional copy parameters
        """
        with self.get_cursor() as cur:
            cur.copy_to(file=file_obj, table=table, **kwargs)

    def __enter__(self):
        """Context manager entry."""

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Example usage and test

if __name__ == '__main__':
    # New recommended way - using connection_params dictionary
    config = {
        'host': 'vm-pg-clone',
        'user': 'arc_energo',
        'database': 'arc_energo',
        'port': 5432,
        # Note: no password - will use .pgpass
    }

    try:
        with PostgresManager(
            connection_params=config,
            autocommit=True,
            log_queries=True,
            log_results=False
        ) as db:
            # Example query with DictCursor
            query_result = db.execute(
                "SELECT * FROM arc_constants WHERE id = %s",
                [1],
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            print("Query result:", query_result)

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Database operation failed: %s", e)

    # Backward compatibility way
    try:
        with PostgresManager.from_individual_params(
            host='vm-pg-clone',
            user='arc_energo',
            database='arc_energo',
            port=5432,
            autocommit=True,
            log_queries=True
        ) as db:
            # Example query with parameterized SQL
            TABLE_NAME = "arc_constants"
            COLUMN = "id"
            VALUE = 1

            loc_query = sql.SQL("SELECT * FROM {} WHERE {} = %s").format(
                sql.Identifier(TABLE_NAME),
                sql.Identifier(COLUMN)
            )

            query_result = db.execute(loc_query, [VALUE])
            print("Parameterized query result:", query_result)

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Database operation failed: %s", e)
