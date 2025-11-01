import sys
from pathlib import Path

# Ensure src/ is on sys.path for local runs without installing the package
sys.path.insert(0, str(Path(__file__).parent.joinpath("src")))

from eml_to_database.cli import main  # noqa: E402

if __name__ == "__main__":
    main()
