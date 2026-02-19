param(
  [Parameter(Mandatory = $true)]
  [string]$LibraryPath
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if (-not (Test-Path -LiteralPath $LibraryPath)) {
  throw "Library not found: $LibraryPath"
}

$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio/Installer/vswhere.exe'
if (-not (Test-Path -LiteralPath $vswhere)) {
  throw "vswhere not found: $vswhere"
}
$installPath = (& $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath).Trim()
if ([string]::IsNullOrWhiteSpace($installPath)) {
  throw 'Visual Studio installation not found via vswhere'
}
$devCmd = Join-Path $installPath 'Common7/Tools/VsDevCmd.bat'
if (-not (Test-Path -LiteralPath $devCmd)) {
  throw "VsDevCmd.bat not found: $devCmd"
}

$dumpCmd = "call `"$devCmd`" -no_logo && dumpbin /DEPENDENTS `"$LibraryPath`""
$dumpOut = cmd.exe /c $dumpCmd
if ($LASTEXITCODE -ne 0) {
  throw "dumpbin failed for $LibraryPath"
}

$dumpOut | Write-Host
if ($dumpOut -match '(?im)\bgdal[^\s]*\.dll\b') {
  throw "Unexpected dynamic GDAL dependency detected in $LibraryPath"
}
