import contextlib
import os
import sqlite3
from typing import Optional

__all__ = ["setup", "session"]


class Session:
    """A Singleton pattern managing the session with the database."""

    _instance = None
    _uri = os.environ["DATABASE_URI"] if "DATABASE_URI" in os.environ else None

    @classmethod
    def setup(
        cls,
        *,
        filename: Optional[str] = None,
        uri: Optional[str] = None,
        read_only: bool = False,
    ):
        """Configure the session manager with database connection parameters.

        Args:
            filename (str, optional): Database filename.  If not provided we
                fallback to the value provided by `uri`.
            uri (str, optional): Uniform resource identifier for the database.
                If this parameter is used, then `read_only` is ignored.  If
                neither filename nor uri are used, then we raise a warning and
                connect to an in-memory database.
            read_only (bool, optional): Whether the database file should be
                opened in read only mode.
        """
        if filename:
            ro = "?mode=ro" if read_only else ""
            cls._uri = f"file:{filename}{ro}"

        if uri:
            cls._uri = uri

        if cls._uri is None:
            cls._uri = ":memory:"

    @classmethod
    def get(cls):
        if cls._instance is None:
            if cls._uri is None:
                cls.setup()
            cls._instance = SessionContext(cls._uri)
        return cls._instance


class SessionContext:
    """A context manager for database sessions."""

    def __init__(self, uri):
        self._uri = uri
        self._conn = None

    @property
    def uri(self):
        return self._uri

    @property
    def conn(self):
        return self._conn

    @property
    def connection(self):
        return self.conn

    def cursor(self):
        teardown = self._conn is None
        self._setup()
        return cursor_context(self, teardown)

    def cur(self):
        return self.cursor()

    def _setup(self):
        if self._conn is None:
            self._conn = sqlite3.connect(self.uri, uri=True)
            self._conn.row_factory = sqlite3.Row

    def _teardown(self):
        if self._conn:
            self.conn.commit()
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self._setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._teardown()


@contextlib.contextmanager
def cursor_context(session_context, teardown):
    cur = session_context.conn.cursor()
    yield cur
    cur.close()
    if teardown:
        session_context._teardown()


def setup(*, uri: Optional[str] = None, read_only: bool = False):
    """Configure the session manager with database connection parameters.

    Args:
        uri (str, optional): Database URI connection string.  If not
            provided we use the DATABASE_URI environment variable.  If
            neither the argument nor the environment variable is set, we
            raise an error.
        read_only (bool, optional): Whether the database should be opened
            in read only mode. Defaults to False.

    Raises:
        sqlite3.DatabaseError: when no database URI is provided.
    """
    Session.setup(uri=uri, read_only=read_only)


def session(
    *,
    filename: Optional[str] = None,
    uri: Optional[str] = None,
    read_only: bool = False,
):
    """Configure the session manager with database connection parameters.

    Args:
        filename (str, optional): Database filename.
        uri (str, optional): Uniform resource identifier for the database.
            If this parameter is used, then `read_only` is ignored.  If
            neither filename nor uri are used, then we raise a warning and
            connect to an in-memory database.
        read_only (bool, optional): Whether the database should be opened
            in read only mode. Defaults to False.

    Raises:
        sqlite3.DatabaseError: when no database URI is provided.
    """
    if filename is not None:
        Session.setup(filename=filename, read_only=read_only)
    if uri is not None:
        Session.setup(uri=uri)
    return Session.get()
