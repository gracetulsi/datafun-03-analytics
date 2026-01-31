"""
gracetulsi_xlsx_pipeline.py

ETVL pipeline for SBA FY22 Home disaster loan data.
Computes total verified loss by state.
"""

from pathlib import Path
from typing import Any
import openpyxl


def _to_float(value: Any) -> float:
    """Convert Excel cell values to float safely."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return 0.0


def extract_state_verified_loss_rows(*, file_path: Path, sheet_name: str) -> list[tuple[str, float]]:
    """E: Extract (state_code, total_verified_loss) rows from an Excel sheet.

    Uses:
      H = state code (e.g., 'UT')
      I = total verified loss (numeric)

    Skips rows where state is missing or loss is missing/blank.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Missing input file: {file_path}")

    workbook = openpyxl.load_workbook(file_path, data_only=True)

    if sheet_name not in workbook.sheetnames:
        raise ValueError(f"Sheet not found: {sheet_name}. Found: {workbook.sheetnames}")

    sheet = workbook[sheet_name]

    rows: list[tuple[str, float]] = []

    # Data starts at row 6 per your sheet layout.
    for r in range(6, sheet.max_row + 1):
        state_val = sheet[f"H{r}"].value
        loss_val = sheet[f"I{r}"].value

        if state_val is None:
            continue

        state = str(state_val).strip()
        if not state:
            continue

        # Treat blank loss as skip (not zero), to avoid accidental misinterpretation.
        if loss_val is None or (isinstance(loss_val, str) and not loss_val.strip()):
            continue

        loss = _to_float(loss_val)
        rows.append((state, loss))

    return rows




if __name__ == "__main__":
    test_file = Path("data/raw/sba_disaster_loan_data_fy22.xlsx")
    data = extract_state_verified_loss_rows(file_path=test_file, sheet_name="FY22 Home")
    print(f"Extracted rows: {len(data)}")
    print("First 5:", data[:5])
