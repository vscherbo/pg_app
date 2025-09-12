#!/usr/bin/env python
"""
Modern PostgreSQL connection manager with logging, retry mechanism, and Unicode support.
"""
# pylint: disable=broad-exception-caught,line-too-long,too-many-arguments,unnecessary-dunder-call

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, List, Optional, TextIO, Union

import psycopg2
import psycopg2.extensions
import psycopg2.extras


@dataclass
class LoggerConfig:
    """Конфигурация логгера"""
    log_queries: bool = False
    log_results: bool = False
    logger_name: Optional[str] = None
    log_to_file: Optional[str] = None
    log_level: int = logging.INFO


@dataclass
class RetryConfig:
    """Конфигурация повторных попыток"""
    retry_attempts: int = 3
    retry_delay: float = 1


@dataclass
class PostgresConfig:
    """Конфигурация для подключения к PostgreSQL"""
    dsn: str
    cursor_factory: Optional[Callable] = None
    logger_config: Optional[LoggerConfig] = None
    retry_config: Optional[RetryConfig] = None


class PostgresCursor(psycopg2.extensions.cursor):
    """Расширенный курсор с логированием"""

    def __init__(self, *args, logger_instance=None, logger_config=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logger_instance or logging.getLogger(__name__)
        self.logger_config = logger_config or LoggerConfig()

    def _mask_sensitive_data(self, data):
        """
        Маскирует чувствительные данные в параметрах запроса.
        """

        if not self.logger_config.hide_sensitive_params:
            return data

        if isinstance(data, dict):
            return {k: '***' if any(pattern in k.lower() for pattern in self.logger_config.sensitive_patterns) else v  # noqa: E501
                    for k, v in data.items()}

        if isinstance(data, (list, tuple)):
            return [self._mask_sensitive_data(item) for item in data]

        if isinstance(data, str) and len(data) > 0:
            # Проверяем, не содержит ли строка чувствительные данные

            if any(pattern in data.lower() for pattern in self.logger_config.sensitive_patterns):
                return '***'

        return data

    def execute(self, query, vars_=None):
        """
        Выполняет SQL запрос с логированием.

        Args:
            query: SQL запрос
            vars_: Параметры запроса

        Returns:
            Результат выполнения запроса
        """

        if self.logger_config.log_queries:
            masked_vars = self._mask_sensitive_data(vars_) if vars_ else None

            self.logger.info("Executing query: %s", query)

            if masked_vars:
                self.logger.info("With parameters: %s", masked_vars)

        result = super().execute(query, vars_)

        return result

    def callproc(self, procname, parameters=None):
        """
        Вызывает хранимую процедуру с логированием.

        Args:
            procname: Имя процедуры
            parameters: Параметры процедуры

        Returns:
            Результат выполнения процедуры
        """

        if self.logger_config.log_queries:
            self.logger.info("Calling procedure: %s", procname)

            if parameters:
                self.logger.info("With parameters: %s", parameters)

        result = super().callproc(procname, parameters)

        if self.logger_config.log_results and result:
            self.logger.info("Procedure result: %s", result)

        return result

    def copy_from(self, file, table, sep='\t', null='\\N', columns=None, **kwargs):
        """
        Копирует данные из файла в таблицу.

        Args:
            file: Файл для чтения данных
            table: Имя таблицы
            sep: Разделитель
            null: Обозначение NULL значений
            columns: Список колонок

        Returns:
            None
        """

        if self.logger_config.log_queries:
            self.logger.info("Copy FROM file to table: %s", table)
            self.logger.info("Separator: %s, Null: %s", sep, null)

            if columns:
                self.logger.info("Columns: %s", columns)

        result = super().copy_from(file, table, sep, null, columns, **kwargs)

        return result

    def copy_to(self, file, table, sep='\t', null='\\N', columns=None, **kwargs):
        """
        Копирует данные из таблицы в файл.

        Args:
            file: Файл для записи данных
            table: Имя таблицы
            sep: Разделитель
            null: Обозначение NULL значений
            columns: Список колонок

        Returns:
            None
        """

        if self.logger_config.log_queries:
            self.logger.info("Copy TO file from table: %s", table)
            self.logger.info("Separator: %s, Null: %s", sep, null)

            if columns:
                self.logger.info("Columns: %s", columns)

        result = super().copy_to(file, table, sep, null, columns, **kwargs)

        return result

    def copy_expert(self, sql, file, **kwargs):
        """
        Выполняет COPY команду с расширенными возможностями.

        Args:
            sql: COPY SQL команда
            file: Файл для операции

        Returns:
            None
        """

        if self.logger_config.log_queries:
            self.logger.info("Copy expert: %s", sql)

        result = super().copy_expert(sql, file, **kwargs)

        return result


class PostgresConnection(psycopg2.extensions.connection):
    """Расширенное соединение с retry-логикой"""

    def __init__(self, dsn, cursor_factory=None, logger_instance=None,
                 logger_config=None, retry_config=None):
        """
        Инициализация расширенного соединения.

        Args:
            dsn: Строка подключения
            cursor_factory: Фабрика курсоров
            logger_instance: Инстанс логгера
            logger_config: Конфигурация логгера
            retry_config: Конфигурация повторных попыток
        """
        self.logger = logger_instance or logging.getLogger(__name__)
        self.logger_config = logger_config or LoggerConfig()
        self.retry_config = retry_config or RetryConfig()

        # Если cursor_factory не задан, используем наш расширенный курсор

        if cursor_factory is None:
            def factory():
                return PostgresCursor(
                    logger_instance=self.logger,
                    logger_config=self.logger_config
                )
            cursor_factory = factory

        super().__init__(dsn, cursor_factory=cursor_factory)

    def cursor(self, *args, **kwargs):
        """
        Создает курсор с настройками логирования.

        Returns:
            PostgresCursor: Расширенный курсор
        """
        kwargs.setdefault('logger_instance', self.logger)
        kwargs.setdefault('logger_config', self.logger_config)

        return super().cursor(*args, **kwargs)

    @staticmethod
    def is_connection_error(exception):
        """
        Определяет, является ли ошибка проблемой соединения.

        Args:
            exception: Исключение для проверки

        Returns:
            bool: True если это ошибка соединения
        """
        connection_errors = (
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
            psycopg2.DatabaseError
        )

        return isinstance(exception, connection_errors)

    def execute_with_retry(self, operation_func, *args, **kwargs):
        """
        Выполняет операцию с повторными попытками при обрыве соединения
        с экспоненциальным бэкоффом.
        """
        last_exception = None
        retry_attempts = self.retry_config.retry_attempts

        for attempt in range(retry_attempts):
            try:
                return operation_func(*args, **kwargs)
            except Exception as e:
                last_exception = e

                if self.is_connection_error(e):
                    # Вычисляем задержку с экспоненциальным бэкоффом
                    delay = min(
                        self.retry_config.base_delay * (self.retry_config.backoff_factor ** attempt),
                        self.retry_config.max_delay
                    )

                    self.logger.warning(
                        "DB connection error (attempt %d/%d), retrying in %.2f seconds: %s",
                        attempt + 1,
                        retry_attempts,
                        delay,
                        e
                    )

                    if attempt < retry_attempts - 1:
                        time.sleep(delay)
                        # Пытаемся восстановить соединение
                        try:
                            self.rollback()
                        except Exception:
                            # Если не удалось откатить, пробуем переподключиться
                            try:
                                self.close()
                                self.__init__(self.dsn,
                                              cursor_factory=self.cursor_factory,
                                              logger_instance=self.logger,
                                              logger_config=self.logger_config,
                                              retry_config=self.retry_config)
                            except Exception:
                                pass  # Продолжаем попытки

                        continue

                # Не ошибка соединения - пробрасываем сразу
                raise

        # Если дошли до сюда, значит все попытки исчерпаны
        self.logger.error("All %d retry attempts failed", retry_attempts)
        raise last_exception

    def _execute_query_with_fetch(self, query, vars_, fetch_method, **fetch_kwargs):
        """
        Выполняет запрос и получает результаты.

        Args:
            query: SQL запрос
            vars_: Параметры запроса
            fetch_method: Метод получения данных
            fetch_kwargs: Аргументы для fetch методов

        Returns:
            Результат выполнения запроса
        """
        with self.cursor() as cur:
            cur.execute(query, vars_)

            if fetch_method == 'fetchall':
                return cur.fetchall()

            if fetch_method == 'fetchone':
                return cur.fetchone()

            if fetch_method == 'fetchmany':
                size = fetch_kwargs.get('size')

                return cur.fetchmany(size) if size else cur.fetchmany()

            if fetch_method is None:
                return None

            raise ValueError(f"Unknown fetch method: {fetch_method}")

    def execute_query_with_retry(self, query, vars_=None, fetch_method='fetchall', **fetch_kwargs):
        """
        Выполняет запрос с повторными попытками при обрыве соединения.

        Args:
            query: SQL запрос
            vars_: Параметры запроса
            fetch_method: Метод получения данных
            fetch_kwargs: Аргументы для fetch методов

        Returns:
            Результат выполнения запроса
        """
        def operation():
            return self._execute_query_with_fetch(query, vars_, fetch_method, **fetch_kwargs)

        return self.execute_with_retry(operation)

    def callproc_with_retry(self, procname, parameters=None):
        """
        Вызывает хранимую процедуру с повторными попытками.

        Args:
            procname: Имя процедуры
            parameters: Параметры процедуры

        Returns:
            Результат выполнения процедуры
        """
        def operation():
            with self.cursor() as cur:
                return cur.callproc(procname, parameters)

        return self.execute_with_retry(operation)

    def copy_operation_with_retry(self, copy_func, *args, **kwargs):
        """
        Выполняет COPY операцию с повторными попытками.

        Args:
            copy_func: Функция COPY операции
            *args: Позиционные аргументы
            **kwargs: Именованные аргументы

        Returns:
            Результат выполнения операции
        """
        def operation():
            with self.cursor() as cur:
                return copy_func(cur, *args, **kwargs)

        return self.execute_with_retry(operation)


class PostgresApp:
    """Базовый класс для прикладных классов работы с PostgreSQL"""

    def __init__(self, config: PostgresConfig):
        """
        Инициализация приложения PostgreSQL.

        Args:
            config: Конфигурация подключения
        """
        self.config = config
        self.logger_config = config.logger_config or LoggerConfig()
        self.retry_config = config.retry_config or RetryConfig()

        # Настройка логгера
        self.logger = self._setup_logger(
            self.logger_config.logger_name or self.__class__.__name__,
            self.logger_config.log_to_file,
            self.logger_config.log_level
        )

        # Создание соединения
        self.connection = PostgresConnection(
            config.dsn,
            cursor_factory=config.cursor_factory,
            logger_instance=self.logger,
            logger_config=self.logger_config,
            retry_config=self.retry_config
        )

    def _setup_logger(self, name: str, log_file: Optional[str], level: int):
        """
        Настройка логгера.

        Args:
            name: Имя логгера
            log_file: Файл для логирования (None для stdout)
            level: Уровень логирования

        Returns:
            logging.Logger: Настроенный логгер
        """
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Очищаем существующие хендлеры
        logger.handlers.clear()

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        if log_file:
            handler = logging.FileHandler(log_file, encoding='utf-8')
        else:
            handler = logging.StreamHandler()

        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    # def health_check(self, timeout: int = 5) -> dict:
    def health_check(self) -> dict:
        """
        Проверяет состояние соединения с БД.

        Returns:
            Словарь с результатами проверки
        """
        health_status = {
            "status": "unhealthy",
            "timestamp": time.time(),
            "details": {}
        }

        try:
            # Используем execute_with_retry для проверки соединения
            result = self.execute_query("SELECT 1", fetch_method='fetchone')

            if result and result[0] == 1:
                health_status["status"] = "healthy"
                health_status["details"]["connection"] = "ok"
                health_status["details"]["query_execution"] = "ok"
            else:
                health_status["details"]["error"] = "Health check query returned unexpected result"

        except Exception as e:
            health_status["details"]["error"] = str(e)

        return health_status

    def extended_health_check(self) -> dict:
        """
        Расширенная проверка здоровья с дополнительной информацией о БД.
        """
        health_status = self.health_check()

        if health_status["status"] == "healthy":
            try:
                # Получаем дополнительную информацию о БД
                db_info = self.execute_query("""
                    SELECT
                        version() as version,
                        pg_database_size(current_database()) as db_size,
                        now() as server_time,
                        (SELECT count(*) FROM pg_stat_activity) as active_connections
                """, fetch_method='fetchone')

                if db_info:
                    health_status["details"].update({
                        "postgres_version": db_info[0],
                        "database_size_bytes": db_info[1],
                        "server_time": db_info[2].isoformat() if hasattr(db_info[2], 'isoformat')
                        else str(db_info[2]),
                        "active_connections": db_info[3]
                    })

            except Exception as e:
                health_status["details"]["extended_check_error"] = str(e)

        return health_status

    def execute_query(self, query: str, vars_: Optional[tuple] = None,
                      fetch_method: str = 'fetchall'):
        """
        Выполняет запрос с обработкой ошибок и повторными попытками.

        Args:
            query: SQL запрос
            vars_: Параметры запроса
            fetch_method: Метод получения данных

        Returns:
            Результат выполнения запроса
        """

        return self.connection.execute_query_with_retry(query, vars_, fetch_method)

    def execute(self, query: str, vars_: Optional[tuple] = None):
        """
        Выполняет запрос без возврата результата.

        Args:
            query: SQL запрос
            vars_: Параметры запроса

        Returns:
            None
        """

        return self.execute_query(query, vars_, fetch_method=None)

    def safe_execute(self, operation, *args, **kwargs):
        """
        Безопасно выполняет операцию с обработкой ошибок.
        Не позволяет исключениям БД провалиться в приложение-наследник.

        Returns:
            Результат операции или None в случае ошибки
        """
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            self.logger.error("Safe execute failed: %s", e)
            return None

    def safe_execute_query(self, query, vars_=None, fetch_method='fetchall'):
        """
        Безопасно выполняет запрос с обработкой ошибок.

        Returns:
            Результат запроса или None в случае ошибки
        """
        return self.safe_execute(self.execute_query, query, vars_, fetch_method)

    def fetchone(self, query: str, vars_: Optional[tuple] = None):
        """
        Выполняет запрос и возвращает одну запись.

        Args:
            query: SQL запрос
            vars_: Параметры запроса

        Returns:
            Одна запись из результата запроса
        """

        return self.execute_query(query, vars_, fetch_method='fetchone')

    def fetchall(self, query: str, vars_: Optional[tuple] = None):
        """
        Выполняет запрос и возвращает все записи.

        Args:
            query: SQL запрос
            vars_: Параметры запроса

        Returns:
            Все записи из результата запроса
        """

        return self.execute_query(query, vars_, fetch_method='fetchall')

    def fetchmany(self, query: str, vars_: Optional[tuple] = None, size: Optional[int] = None):
        """
        Выполняет запрос и возвращает несколько записей.

        Args:
            query: SQL запрос
            vars_: Параметры запроса
            size: Количество записей

        Returns:
            Несколько записей из результата запроса
        """
        def operation():
            with self.connection.cursor() as cur:
                cur.execute(query, vars_)

                return cur.fetchmany(size)

        return self.connection.execute_with_retry(operation)

    def callproc(self, procname: str, parameters: Optional[tuple] = None):
        """
        Вызывает хранимую процедуру с retry-логикой.

        Args:
            procname: Имя процедуры
            parameters: Параметры процедуры

        Returns:
            Результат выполнения процедуры
        """

        return self.connection.callproc_with_retry(procname, parameters)

    def copy_from(self, file: Union[TextIO], table: str, sep: str = '\t',
                  null: str = '\\N', columns: Optional[List[str]] = None, **kwargs):
        """
        Копирует данные из файла в таблицу.

        Args:
            file: Файл для чтения данных
            table: Имя таблицы
            sep: Разделитель
            null: Обозначение NULL значений
            columns: Список колонок

        Returns:
            None
        """
        def copy_func(cursor, file_obj, table_name, separator, null_value, cols, **kw):
            return cursor.copy_from(file_obj, table_name, separator, null_value, cols, **kw)

        return self.connection.copy_operation_with_retry(
            copy_func, file, table, sep, null, columns, **kwargs
        )

    def copy_to(self, file: Union[TextIO], table: str, sep: str = '\t',
                null: str = '\\N', columns: Optional[List[str]] = None, **kwargs):
        """
        Копирует данные из таблицы в файл.

        Args:
            file: Файл для записи данных
            table: Имя таблицы
            sep: Разделитель
            null: Обозначение NULL значений
            columns: Список колонок

        Returns:
            None
        """
        def copy_func(cursor, file_obj, table_name, separator, null_value, cols, **kw):
            return cursor.copy_to(file_obj, table_name, separator, null_value, cols, **kw)

        return self.connection.copy_operation_with_retry(
            copy_func, file, table, sep, null, columns, **kwargs
        )

    def copy_expert(self, sql: str, file: Union[TextIO], **kwargs):
        """
        Выполняет COPY команду с расширенными возможностями.

        Args:
            sql: COPY SQL команда
            file: Файл для операции

        Returns:
            None
        """
        def copy_func(cursor, sql_cmd, file_obj, **kw):
            return cursor.copy_expert(sql_cmd, file_obj, **kw)

        return self.connection.copy_operation_with_retry(
            copy_func, sql, file, **kwargs
        )

    @contextmanager
    def transaction(self):
        """
        Контекстный менеджер для транзакций.

        Yields:
            PostgresConnection: Соединение для транзакции
        """
        try:
            yield self.connection
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def close(self):
        """Закрывает соединение."""
        try:
            self.connection.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
