"""Run Cascade with ``python -m cascade``."""

from cascade.runtime import initialize_runtime


def main() -> None:
    """Start the Cascade runtime."""
    initialize_runtime()


if __name__ == "__main__":
    main()
