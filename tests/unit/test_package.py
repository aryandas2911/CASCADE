import cascade
from cascade.runtime import initialize_runtime


def test_package_imports() -> None:
    assert cascade.__version__ == "0.1.0"
    assert callable(initialize_runtime)
