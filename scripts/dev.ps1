# 開発用スクリプト
# venv 作成・起動・実行を一本化

if (-not (Test-Path ".venv")) {
    py -3 -m venv .venv
}

.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -r requirements.txt

python -m apps.main
pytest -q
