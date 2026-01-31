"""app_gracetulsi.py - Project script.


TODO: Your module should have:
- an appropriate name like yourname_type_pipeline.py (e.g., smith_csv_pipeline.py)
- start with a docstring similar to the examples
- add imports at the top.
- define an extract function (E that reads data from data/raw into memory)
- define a transform function (T that processes the extracted data)
- define a load function (L that writes output to data/processed)
- define a run_pipeline() function that calls E, T, L, and adds a new output file to data/processed/.

Author: Your Name or Alias
Date: 2026-01

Practice key Python skills:
- pathlib for cross-platform paths
- logging (preferred over print)
- calling functions from modules
- clear ETL pipeline stages:
  E = Extract (read, get data from source into memory)
  T = Transform (process, change data in memory)
  L = Load (write results, to data/processed or other destination)

OBS:
  This is your file to practice and customize.
  Find the TODO comments, and as you complete each task, remove the TODO note.
"""

# setup verified
# === DECLARE IMPORTS (BRING IN FREE CODE) ===

# Imports from the Python standard library (free stuff that comes with Python).
import logging
from pathlib import Path
from typing import Final

# REQ: imports from external packages must be listed in pyproject.toml dependencies
from datafun_toolkit.logger import get_logger, log_header

# === IMPORT LOCAL MODULE FUNCTIONS ===
# REQ: imports from other modules in this project must use full package path
from datafun_03_analytics.gracetulsi_xlsx_pipeline import run_pipeline

# === CONFIGURE LOGGER ONCE PER MODULE ===

LOG: logging.Logger = get_logger("P03", level="DEBUG")

# === DECLARE GLOBAL VARIABLES ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
RAW_DIR: Final[Path] = DATA_DIR / "raw"
PROCESSED_DIR: Final[Path] = DATA_DIR / "processed"

# === DEFINE THE MAIN FUNCTION THAT WILL CALL OUR FUNCTIONS ===


def main() -> None:
    """Entry point: run a simple ETVL pipeline."""
    log_header(LOG, "Pipelines: Read, Process, Verify, Write (ETVL)")
    LOG.info("START main()")

    # TODO: call your imported data pipeline that reads from data/raw and writes to data/processed.
    run_pipeline(raw_dir=RAW_DIR, processed_dir=PROCESSED_DIR, logger=LOG)

    LOG.info("END main()")


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
