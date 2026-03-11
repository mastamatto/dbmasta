"""Centralized fatal connection error detection for all client types."""


_FATAL_MYSQL_MESSAGES = (
    "Packet sequence number wrong",
    "Commands out of sync",
    "Lost connection to MySQL server",
    "Can't connect to MySQL server",
)

_FATAL_PG_MESSAGES = (
    "connection is closed",
    "server closed the connection unexpectedly",
    "SSL connection has been closed unexpectedly",
    "could not connect to server",
    "terminating connection due to administrator command",
)


def is_fatal_mysql_connection_error(e: Exception) -> bool:
    s = str(e)
    return any(msg in s for msg in _FATAL_MYSQL_MESSAGES)


def is_fatal_pg_connection_error(e: Exception) -> bool:
    s = str(e)
    return any(msg in s for msg in _FATAL_PG_MESSAGES)


def is_fatal_connection_error(e: Exception, dialect: str = "mysql") -> bool:
    if dialect == "postgresql":
        return is_fatal_pg_connection_error(e)
    return is_fatal_mysql_connection_error(e)
