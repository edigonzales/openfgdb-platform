param(
  [Parameter(Mandatory = $false)]
  [string]$ArtifactDir
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ProjectDir = (Resolve-Path (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) '../..')).Path
$RootDir = (Resolve-Path (Join-Path $ProjectDir '..')).Path

$TargetOs = $env:OPENFGDB4J_TARGET_OS
if ([string]::IsNullOrWhiteSpace($TargetOs)) { $TargetOs = 'windows' }
$TargetArch = $env:OPENFGDB4J_TARGET_ARCH
if ([string]::IsNullOrWhiteSpace($TargetArch)) { $TargetArch = 'amd64' }
$TargetArch = $TargetArch.ToLowerInvariant()
if ($TargetArch -eq 'x86_64') { $TargetArch = 'amd64' }
if ($TargetArch -eq 'aarch64') { $TargetArch = 'arm64' }

$ArtifactName = "openfgdb4j-bin-$TargetOs-$TargetArch"
if ([string]::IsNullOrWhiteSpace($ArtifactDir)) {
  $ArtifactDir = Join-Path $ProjectDir "build/artifacts/$ArtifactName"
}

$NativeDir = Join-Path $ArtifactDir 'native'
$JavaDir = Join-Path $ArtifactDir 'java'
$IncludeDir = Join-Path $ArtifactDir 'include'
$MetaDir = Join-Path $ArtifactDir 'metadata'

foreach ($dir in @($NativeDir, $JavaDir, $IncludeDir, $MetaDir)) {
  if (-not (Test-Path -LiteralPath $dir)) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
  }
}

$JarPath = Join-Path $ProjectDir 'build/java/openfgdb4j.jar'
if (-not (Test-Path -LiteralPath $JarPath)) {
  throw "Missing jar: $JarPath"
}

$LibCandidate = Join-Path $ProjectDir 'build/native/Release/openfgdb.dll'
if (-not (Test-Path -LiteralPath $LibCandidate)) {
  $alt = Get-ChildItem -Path (Join-Path $ProjectDir 'build/native') -Recurse -File -Filter 'openfgdb.dll' | Select-Object -First 1
  if (-not $alt) {
    throw "Missing native library under $ProjectDir/build/native"
  }
  $LibCandidate = $alt.FullName
}

Copy-Item -Force -LiteralPath $LibCandidate -Destination (Join-Path $NativeDir 'openfgdb.dll')
Copy-Item -Force -LiteralPath $JarPath -Destination (Join-Path $JavaDir 'openfgdb4j.jar')
Copy-Item -Force -LiteralPath (Join-Path $ProjectDir 'native/include/openfgdb_c_api.h') -Destination (Join-Path $IncludeDir 'openfgdb_c_api.h')

$gitSha = 'unknown'
try {
  $gitSha = (& git -C $RootDir rev-parse HEAD).Trim()
} catch {
}

$manifest = @{
  name = $ArtifactName
  target_os = $TargetOs
  target_arch = $TargetArch
  git_sha = $gitSha
  native_library = 'native/openfgdb.dll'
  java_jar = 'java/openfgdb4j.jar'
  c_api_header = 'include/openfgdb_c_api.h'
}
$manifestJson = ($manifest | ConvertTo-Json -Depth 3)
Set-Content -Path (Join-Path $MetaDir 'manifest.json') -Value $manifestJson -Encoding UTF8

$files = @(
  (Join-Path -Path $NativeDir -ChildPath 'openfgdb.dll')
  (Join-Path -Path $JavaDir -ChildPath 'openfgdb4j.jar')
  (Join-Path -Path $IncludeDir -ChildPath 'openfgdb_c_api.h')
  (Join-Path -Path $MetaDir -ChildPath 'manifest.json')
)

$shaLines = @()
foreach ($file in $files) {
  $hash = (Get-FileHash -Algorithm SHA256 -Path $file).Hash.ToLowerInvariant()
  $rel = $file.Substring($ArtifactDir.Length).TrimStart('\', '/') -replace '\\','/'
  $shaLines += "$hash  $rel"
}
Set-Content -Path (Join-Path $MetaDir 'sha256sums.txt') -Value ($shaLines -join "`n") -Encoding UTF8

Write-Host "Packaged artifact: $ArtifactDir"
