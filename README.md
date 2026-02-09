# Project Template (Python)

## 新規プロジェクト開始手順

1. 本テンプレをコピーして新しいGitHubリポジトリを作成する
2. 作成したリポジトリを repos 配下に clone
3. このテンプレの中身を clone 先にコピー
4. hypermill_nctools_inventory_exporter を実際の名前に置換
5. 最初の commit を行う

## 開発ルール
- Git操作は repos 配下のみ
- .env は GitHub に上げない
- 作業開始時は git pull

## 実行方法
```powershell
.\.venv\Scripts\Activate.ps1
python .\apps\main.py



---


## 使い方まとめ（超重要）
- テンプレは **コピーされる側**
- 実プロジェクトは **最初からGitにつながる側**
- 作業は clone 後にしか始めない


このテンプレは必要に応じて拡張してよいが、
**構造の意味（役割分離）は崩さないこと**。
