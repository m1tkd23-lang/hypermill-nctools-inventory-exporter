from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import os
from dotenv import load_dotenv

from hypermill_nctools_inventory_exporter.core import export_nctools_to_excel


def main() -> int:
    load_dotenv()

    db_path = os.getenv("HYPERMILL_TOOLDB_PATH")
    if not db_path:
        raise SystemExit("HYPERMILL_TOOLDB_PATH が未設定")

    out_path = os.getenv("OUTPUT_XLSX_PATH", "./nctools_inventory.xlsx")

    export_nctools_to_excel(
        db_path=Path(db_path),
        output_path=Path(out_path),
    )

    print(f"Export completed: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
