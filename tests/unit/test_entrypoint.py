import subprocess
import sys


def test_module_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "cascade"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == "Cascade runtime initialized"
