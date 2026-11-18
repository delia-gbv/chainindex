"""The alerting rules in deploy/prometheus parse and pass their promtool unit tests (needs promtool 3.9 or newer)."""

import shutil
import subprocess
from pathlib import Path

import pytest

PROMETHEUS_DIR = Path(__file__).parents[1] / "deploy" / "prometheus"
PROMTOOL = shutil.which("promtool")

pytestmark = pytest.mark.skipif(PROMTOOL is None, reason="promtool is not on PATH")


def promtool(*arguments: str) -> subprocess.CompletedProcess[str]:
    assert PROMTOOL is not None
    return subprocess.run([PROMTOOL, *arguments], cwd=PROMETHEUS_DIR, capture_output=True, text=True)


def test_the_rules_parse():
    result = promtool("check", "rules", "alerts.yml")

    assert result.returncode == 0, result.stdout + result.stderr


def test_the_rules_pass_their_unit_tests():
    result = promtool("test", "rules", "alerts_test.yml")

    assert result.returncode == 0, result.stdout + result.stderr
