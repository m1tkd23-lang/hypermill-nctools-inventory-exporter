# project_name

## 概要
- 目的：
- 入出力：
- 想定利用者：


## セットアップ（Windows / PowerShell）

py -3.12 -m venv .venv
.\.venv\Scripts\python -m pip install -U pip

# 依存があれば
# pip install -r requirements.lock.txt

.\.venv\Scripts\Activate.ps1

python .\apps\main.py




### AI（設計補助・実装支援）への運用ルール 
#### 受け取りタイミングの意味 
- **作業開始時に本.mdを受け取った場合** 
- 勝手な再構成・簡略化・省略を行わない 
- 不明点は推測せず、確認を取る

- 修正・追加が発生する場合は
  - 既存コードとの差分を明示する（コピペしやすい形を守る）
  - どの設計判断に基づく変更かを説明する


★最短で使える「tree /f 互換（地雷除外版）」
################################################################################################


function Show-Tree {
  param(
    [string]$Root = ".",

    [string[]]$ExcludeDirNames = @(
      ".git", ".venv", "__pycache__", "build", "dist",
      ".webview_storage", ".mypy_cache", ".pytest_cache", ".ruff_cache",
      "node_modules"
    ),

    [string[]]$ExcludeFileNames = @(
      "*.pyc", "*.pyo", "*.log"
    ),

    [int]$MaxDepth = 0
  )

  $rootItem = Get-Item -LiteralPath $Root
  $rootPath = $rootItem.FullName

  function IsExcludedFile([string]$name) {
    foreach ($pat in $ExcludeFileNames) {
      if ($name -like $pat) { return $true }
    }
    return $false
  }

  function Walk([string]$Path, [string]$Prefix, [int]$Depth) {
    if ($MaxDepth -gt 0 -and $Depth -ge $MaxDepth) { return }

    $items = Get-ChildItem -LiteralPath $Path -Force |
      Where-Object {
        if ($_.PSIsContainer) {
          $ExcludeDirNames -notcontains $_.Name
        } else {
          -not (IsExcludedFile $_.Name)
        }
      } |
      Sort-Object @{Expression = { -not $_.PSIsContainer }}, Name

    for ($i = 0; $i -lt $items.Count; $i++) {
      $it = $items[$i]
      $isLast = ($i -eq $items.Count - 1)

      $branch = $(if ($isLast) { "└─ " } else { "├─ " })
      $nextPrefix = $Prefix + $(if ($isLast) { "   " } else { "│  " })

      $Prefix + $branch + $it.Name

      if ($it.PSIsContainer) {
        Walk -Path $it.FullName -Prefix $nextPrefix -Depth ($Depth + 1)
      }
    }
  }

  $rootItem.Name
  Walk -Path $rootPath -Prefix "" -Depth 0
}

Show-Tree -Root "."