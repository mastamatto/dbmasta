# Dbmasta tests

## Run tests

```bash
# Unit tests only (no DB required)
pytest tests/test_exceptions_and_validation.py tests/test_response_re_raise.py -v

# All tests (DB tests skip if env not set)
pytest tests/ -v

# With coverage (if pytest-cov installed)
pytest tests/ -v --cov=dbmasta --cov-report=term-missing
```

## Environment (for DB tests)

Set in env or `.env` (same as `_test.py`):

- `DB_USERNAME` or `dbmasta_username`
- `DB_PASSWORD` or `dbmasta_password`
- `DB_HOST` or `dbmasta_host`
- `DB_DATABASE` or `dbmasta_default` (database name)
- `DB_PORT` / `dbmasta_port` (optional, default 3306)

Tests create a temporary table `_dbmasta_audit_test` and drop it after each test module.

## Install test deps

```bash
pip install -e ".[test]"
```
