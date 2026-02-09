# PyInstaller spec template
# 実プロジェクト開始時に project_name を置換する


block_cipher = None


a = Analysis(
['apps/main.py'],
pathex=['.'],
binaries=[],
datas=[],
hiddenimports=[],
hookspath=[],
runtime_hooks=[],
excludes=[],
cipher=block_cipher,
)


pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)


exe = EXE(
pyz,
a.scripts,
[],
exclude_binaries=True,
name='project_name',
debug=False,
bootloader_ignore_signals=False,
strip=False,
upx=True,
console=True,
)