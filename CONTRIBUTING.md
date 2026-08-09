# Contributing to s3lfs

Thanks for your interest. Issues and pull requests are welcome.

## Getting set up

```sh
pip install uv
uv sync
uv run pytest
```

Install the pre-commit hooks so formatting and linting run automatically:

```sh
uv run pre-commit install
```

They run black (88 columns), isort (black profile), flake8, mypy, and the
test suite.

## What CI checks

- Tests on Python 3.9 through 3.13 on Linux, plus a macOS run
- An integration job that runs the suite against a real MinIO server
- The pre-commit hooks above, on all files

Python 3.9 is the supported floor, so avoid 3.10+ syntax: no `X | Y`
annotations, no subscripted builtins (`list[str]`) evaluated at runtime, no
`match` statements, no `str.removeprefix`.

## Writing tests

Tests use `moto` to mock S3. Anything touching git should build a real
temporary repository and run real git commands rather than mocking git —
several bugs in this project came from assumptions about git's behaviour
that only a real repository disproves.

When you fix a bug, add the test that reproduces it first. A test that
passes before your change is not testing the fix.

## Things worth knowing

- **The manifest is a git-tracked file.** Anything that writes it on a
  read-only code path will dirty a user's working tree.
- **Tracked files are gitignored**, so git cannot warn a user that one has
  local modifications. Code that overwrites or deletes tracked content is
  the only thing standing between that content and permanent loss — check
  the recorded hash before touching a file.
- **Hooks run in other people's repositories.** A hook that fails loudly is
  recoverable; one that silently does nothing is not.
- **The concurrency-sensitive protocols have TLA+ models** in [`specs/`](specs/).
  If you change garbage collection, chunked upload, or manifest locking,
  read the relevant model first — it probably already describes the failure
  you are about to reintroduce.

## Commit messages

Explain why the change is needed, not just what it does. If it fixes a
defect, describe the failure the user would have seen.
