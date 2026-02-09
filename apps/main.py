"""
アプリケーションのエントリポイント。
CLI / GUI / Web いずれの場合も、このファイルは"薄く"保つ。
"""
from src.hypermill_nctools_inventory_exporter.core import main


if __name__ == "__main__":
    main()
