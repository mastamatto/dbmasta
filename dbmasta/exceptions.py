



class InvalidDate(TypeError):
    """
    Value must be a valid datetime object
    """
    pass
class NonConfiguredDataType(TypeError):
    """
    DataType not yet configured for auto-conversion
    """
    pass


class SchemaQueryError(RuntimeError):
    """
    Raised when the INFORMATION_SCHEMA / information_schema query fails
    or returns no columns (e.g. table does not exist or run() failed).
    """
    pass


class MissingColumnError(KeyError, RuntimeError):
    """
    Raised when a record key is not in the table schema (coldata).
    Often indicates a prior failed schema query or empty metadata.
    """
    pass