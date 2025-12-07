# Pre-commit Hooks with Type Checking

This project uses pre-commit hooks to ensure code quality and type safety. The hooks are automatically run before each commit to catch issues early.

## Installed Hooks

### Code Quality Checks
- **trailing-whitespace**: Removes trailing whitespace
- **end-of-file-fixer**: Ensures files end with a newline
- **check-yaml**: Validates YAML file syntax
- **check-added-large-files**: Prevents committing large files
- **check-merge-conflict**: Detects merge conflict markers
- **debug-statements**: Catches debug statements like `pdb.set_trace()`

### Python Code Formatting
- **black**: Automatic code formatting with 88-character line length
- **isort**: Import statement sorting (compatible with black)

### Linting
- **flake8**: Python linting with:
  - Max line length: 88 characters
  - Ignores: E203 (whitespace before ':'), W503 (line break before binary operator)

### Type Checking
- **mypy**: Static type checking with:
  - Python 3.9+ compatibility
  - Lenient configuration for gradual typing adoption
  - Type stub support for boto3
  - Ignores missing imports for third-party libraries

## Setup

The hooks are already configured and can be installed with:

```bash
uv sync
uv run pre-commit install
```

## Running Hooks

### Automatic (Recommended)
Hooks run automatically on `git commit`. If any hook fails, the commit is blocked.

### Manual
Run all hooks on all files:
```bash
uv run pre-commit run --all-files
```

Run specific hook:
```bash
uv run pre-commit run mypy --all-files
uv run pre-commit run black --all-files
```

## Type Checking Configuration

The mypy configuration in `pyproject.toml` is set to be lenient initially:
- `disallow_untyped_defs = false`: Allows functions without type annotations
- `warn_return_any = false`: Doesn't warn about returning Any types
- `warn_no_return = false`: Allows missing return statements
- `warn_unreachable = false`: Allows unreachable code

These can be gradually made stricter as more type annotations are added.

## Benefits

1. **Consistent Code Style**: Black and isort ensure uniform formatting
2. **Early Bug Detection**: Flake8 and mypy catch issues before runtime
3. **Type Safety**: Mypy provides static type checking for better code reliability
4. **Automated Quality**: No manual formatting or linting needed
5. **Team Collaboration**: Consistent code style across all contributors

## Troubleshooting

If hooks fail:
1. Review the error messages
2. Fix the issues manually or let the tools auto-fix them
3. Stage the changes: `git add .`
4. Retry the commit

To skip hooks (not recommended):
```bash
git commit --no-verify
```
