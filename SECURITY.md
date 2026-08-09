# Security Policy

## Supported versions

Fixes land on `main` and go out in the next release. Only the latest
release is supported.

## Reporting a vulnerability

Please report security issues privately via
[GitHub's private vulnerability reporting](https://github.com/kmatzen/s3lfs/security/advisories/new)
rather than opening a public issue.

Include what you can: affected version, reproduction steps, and what an
attacker gains. You can expect an initial response within a week.

## Scope notes

s3lfs runs as you, with your AWS credentials, and installs git hooks that
execute on ordinary git commands. A few consequences worth stating plainly:

- **Cloning a repository does not install hooks.** Git never transfers
  hooks, and `s3lfs install` is an explicit step. But `s3lfs clone` does
  install them, so treat it as you would any "clone and set up" command:
  only run it against repositories you trust.
- **The manifest determines where data is read from and written to.** It is
  a version-controlled file, so a malicious change to `bucket_name`,
  `repo_prefix`, or `endpoint_url` in a pull request would redirect your
  uploads. Review changes to `.s3_manifest.yaml` and `.s3lfsconfig` the way
  you would review changes to CI configuration.
- **`s3lfs cleanup` deletes S3 objects** that the current manifest does not
  reference. Deletion is irreversible; enable bucket versioning if that
  matters to you.

Credentials are handled entirely by boto3 — s3lfs never reads, stores, or
logs them.
