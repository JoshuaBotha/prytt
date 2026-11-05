"""The installed .pth bootstrap, exercised in fresh interpreters."""

import os
import subprocess
import sys
import sysconfig
from pathlib import Path

FORBIDDEN_AT_START = {
    "importlib.abc",
    "importlib.util",
    "typing",
    "inspect",
    "logging",
    "tomllib",
    "pyspark",
    "pysail",
    "grpc",
}


def _run(*args: str, **env_overrides: str) -> subprocess.CompletedProcess:
    """Run a fresh interpreter from this venv; `args` follow the executable."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("PRYTT_")}
    env.pop("SPARK_REMOTE", None)
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, *args],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )


def test_pth_file_is_installed_at_site_packages_root():
    pth = Path(sysconfig.get_paths()["purelib"]) / "prytt.pth"
    assert pth.read_text().strip() == "import prytt._bootstrap"


def test_interpreter_start_installs_prytt_finder():
    out = _run(
        "-c", "import sys; print(any(type(f).__module__ == 'prytt._hooks' for f in sys.meta_path))"
    )
    assert out.stdout.strip() == "True"


def test_prytt_disable_skips_finder():
    out = _run(
        "-c",
        "import sys; print(any(type(f).__module__ == 'prytt._hooks' for f in sys.meta_path))",
        PRYTT_DISABLE="1",
    )
    assert out.stdout.strip() == "False"


def test_prytt_disable_falsy_value_keeps_finder():
    out = _run(
        "-c",
        "import sys; print(any(type(f).__module__ == 'prytt._hooks' for f in sys.meta_path))",
        PRYTT_DISABLE="0",
    )
    assert out.stdout.strip() == "True"


def test_bootstrap_imports_no_heavy_modules():
    code = "import sys; print('\\n'.join(sorted(sys.modules)))"
    enabled = set(_run("-c", code).stdout.split())
    disabled = set(_run("-c", code, PRYTT_DISABLE="1").stdout.split())
    assert "prytt._hooks" in enabled
    assert (enabled - disabled) & FORBIDDEN_AT_START == set()


def _bootstrap_cumulative_us() -> int:
    err = _run("-X", "importtime", "-c", "pass").stderr
    for line in err.splitlines():
        # "import time:   self [us] | cumulative | imported package"
        parts = [p.strip() for p in line.removeprefix("import time:").split("|")]
        if len(parts) == 3 and parts[2] == "prytt._bootstrap":
            return int(parts[1])
    raise AssertionError(f"prytt._bootstrap not in importtime output:\n{err}")


def test_bootstrap_adds_under_20ms_to_interpreter_start():
    best = min(_bootstrap_cumulative_us() for _ in range(3))
    assert best < 20_000, f"prytt._bootstrap took {best} us at interpreter start"
