from pathlib import Path

import pytest

REPO_GITIGNORE = Path(__file__).resolve().parent.parent / ".gitignore"


@pytest.fixture(autouse=True)
def preserve_repo_gitignore():
    """Some CLI tests run s3lfs commands in the repository's own working
    directory, and those commands edit .gitignore by design (cache
    exclusions on init, tracked-path entries on track). Restore the repo's
    .gitignore afterwards so test runs don't dirty the developer's tree.
    Tests operating in temp dirs are unaffected."""
    original = REPO_GITIGNORE.read_text() if REPO_GITIGNORE.exists() else None
    yield
    if original is None:
        if REPO_GITIGNORE.exists():
            REPO_GITIGNORE.unlink()
    elif not REPO_GITIGNORE.exists() or REPO_GITIGNORE.read_text() != original:
        REPO_GITIGNORE.write_text(original)
