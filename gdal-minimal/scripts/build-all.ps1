$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent $ScriptDir
$ManifestDir = Join-Path $RootDir 'manifests'
$VersionsFile = Join-Path $ManifestDir 'versions.lock'
$ShaFile = Join-Path $ManifestDir 'SHA256SUMS'

$TargetOs = ($env:OPENFGDB4J_TARGET_OS)
if ([string]::IsNullOrWhiteSpace($TargetOs)) { $TargetOs = 'windows' }
$TargetArch = ($env:OPENFGDB4J_TARGET_ARCH)
if ([string]::IsNullOrWhiteSpace($TargetArch)) { $TargetArch = 'amd64' }
$TargetArch = $TargetArch.ToLowerInvariant()
if ($TargetArch -eq 'x86_64') { $TargetArch = 'amd64' }
if ($TargetArch -eq 'aarch64') { $TargetArch = 'arm64' }
if ($TargetOs.ToLowerInvariant() -ne 'windows') {
  throw "build-all.ps1 is only for windows targets (got $TargetOs)"
}
if ($TargetArch -ne 'amd64' -and $TargetArch -ne 'arm64') {
  throw "Unsupported OPENFGDB4J_TARGET_ARCH=$TargetArch (expected amd64|arm64)"
}

$DownloadsDir = Join-Path $RootDir 'third_party/downloads'
$SrcDir = Join-Path $RootDir 'third_party/src'
$BuildWorkDir = Join-Path $RootDir 'build/work'
$StageDir = $env:OPENFGDB4J_GDAL_MINIMAL_ROOT
if ([string]::IsNullOrWhiteSpace($StageDir)) {
  $StageDir = Join-Path $RootDir 'build/stage'
}

$CmakeBin = $env:CMAKE_BIN
if ([string]::IsNullOrWhiteSpace($CmakeBin)) { $CmakeBin = 'cmake' }
$CmakeGenerator = $env:OPENFGDB4J_CMAKE_GENERATOR
if ([string]::IsNullOrWhiteSpace($CmakeGenerator)) { $CmakeGenerator = 'Visual Studio 17 2022' }

function Ensure-Dir([string] $Path) {
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

function Read-LockValue([string] $Name) {
  $escapedName = [regex]::Escape($Name)
  $pattern = '^{0}="([^"]+)"' -f $escapedName
  $line = Select-String -Path $VersionsFile -Pattern $pattern | Select-Object -First 1
  if (-not $line) {
    throw "Missing $Name in $VersionsFile"
  }
  return $line.Matches[0].Groups[1].Value
}

function Resolve-LockTemplates([string] $Value, [int] $Depth = 0) {
  if ([string]::IsNullOrEmpty($Value)) {
    return $Value
  }
  if ($Depth -gt 10) {
    throw "Lockfile template resolution exceeded max depth for value: $Value"
  }
  $resolved = $Value
  $matches = [regex]::Matches($resolved, '\$\{([A-Za-z0-9_]+)\}')
  if ($matches.Count -eq 0) {
    return $resolved
  }
  foreach ($match in $matches) {
    $token = $match.Value
    $key = $match.Groups[1].Value
    $replacement = Resolve-LockTemplates (Read-LockValue $key) ($Depth + 1)
    $resolved = $resolved.Replace($token, $replacement)
  }
  if ($resolved -match '\$\{([A-Za-z0-9_]+)\}') {
    return Resolve-LockTemplates $resolved ($Depth + 1)
  }
  return $resolved
}

function Read-LockResolved([string] $Name) {
  return Resolve-LockTemplates (Read-LockValue $Name)
}

function Get-ExpectedSha([string] $ArchiveName) {
  $escapedArchive = [regex]::Escape($ArchiveName)
  $pattern = '^([0-9a-fA-F]+)\s+{0}$' -f $escapedArchive
  $line = Select-String -Path $ShaFile -Pattern $pattern | Select-Object -First 1
  if (-not $line) {
    throw "No SHA256 pinned for $ArchiveName in $ShaFile"
  }
  return $line.Matches[0].Groups[1].Value.ToLowerInvariant()
}

function Verify-Sha([string] $FilePath, [string] $ArchiveName) {
  $expected = Get-ExpectedSha $ArchiveName
  $actual = (Get-FileHash -Algorithm SHA256 -Path $FilePath).Hash.ToLowerInvariant()
  if ($actual -ne $expected) {
    throw "SHA256 mismatch for $ArchiveName (expected=$expected actual=$actual)"
  }
}

function Download-Archive([string] $Url, [string] $ArchiveName) {
  $out = Join-Path $DownloadsDir $ArchiveName
  if (Test-Path -LiteralPath $out) {
    Verify-Sha $out $ArchiveName
    Write-Host "Using cached archive: $ArchiveName"
    return
  }
  $tmp = "$out.part"
  Write-Host "Downloading $ArchiveName"
  Invoke-WebRequest -Uri $Url -OutFile $tmp
  Verify-Sha $tmp $ArchiveName
  Move-Item -Force -LiteralPath $tmp -Destination $out
}

function Extract-Archive([string] $ArchiveName, [string] $TargetDirName) {
  $archive = Join-Path $DownloadsDir $ArchiveName
  $dest = Join-Path $SrcDir $TargetDirName
  $tmpRoot = Join-Path $BuildWorkDir ("extract-" + $TargetDirName)
  if (Test-Path -LiteralPath $dest) { Remove-Item -Recurse -Force -LiteralPath $dest }
  if (Test-Path -LiteralPath $tmpRoot) { Remove-Item -Recurse -Force -LiteralPath $tmpRoot }
  Ensure-Dir $tmpRoot
  & tar -xzf $archive -C $tmpRoot
  if ($LASTEXITCODE -ne 0) { throw "Failed to extract $ArchiveName" }
  $top = Get-ChildItem -Directory -LiteralPath $tmpRoot | Select-Object -First 1
  if (-not $top) { throw "Failed to detect extracted root for $ArchiveName" }
  Move-Item -Force -LiteralPath $top.FullName -Destination $dest
  Remove-Item -Recurse -Force -LiteralPath $tmpRoot
}

function Get-VsDevCmdPath {
  $vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio/Installer/vswhere.exe'
  if (-not (Test-Path -LiteralPath $vswhere)) {
    throw "vswhere not found: $vswhere"
  }
  $installPath = (& $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath).Trim()
  if ([string]::IsNullOrWhiteSpace($installPath)) {
    throw 'Visual Studio Build Tools not found via vswhere'
  }
  $devCmd = Join-Path $installPath 'Common7/Tools/VsDevCmd.bat'
  if (-not (Test-Path -LiteralPath $devCmd)) {
    throw "VsDevCmd.bat not found: $devCmd"
  }
  return $devCmd
}

$VsDevCmd = Get-VsDevCmdPath

function Get-VsTargetArch {
  if ($TargetArch -eq 'arm64') { return 'arm64' }
  return 'x64'
}

function Get-LibMachineArg {
  if ($TargetArch -eq 'arm64') { return 'ARM64' }
  return 'X64'
}

function Invoke-WithVsDevCmd([string] $CommandLine) {
  $vsTargetArch = Get-VsTargetArch
  $wrapped = "call `"$VsDevCmd`" -no_logo -host_arch=x64 -arch=$vsTargetArch && $CommandLine"
  cmd.exe /c $wrapped
  if ($LASTEXITCODE -ne 0) {
    throw "Command failed: $CommandLine"
  }
}

function Get-CmakeArchArg {
  if ($TargetArch -eq 'arm64') { return 'ARM64' }
  return 'x64'
}

function Show-StageLibDir {
  $libDir = Join-Path $StageDir 'lib'
  Write-Host "Contents of $libDir"
  if (Test-Path -LiteralPath $libDir) {
    Get-ChildItem -LiteralPath $libDir -Force | ForEach-Object { Write-Host "  $($_.Name)" }
  } else {
    Write-Host '  <missing>'
  }
}

function Invoke-Cmake([string[]] $CmakeArgs) {
  & $CmakeBin @CmakeArgs
  if ($LASTEXITCODE -ne 0) {
    throw "cmake command failed: $($CmakeArgs -join ' ')"
  }
}

function To-CmakePath([string] $Path) {
  if ([string]::IsNullOrWhiteSpace($Path)) { return $Path }
  return $Path.Replace('\', '/')
}

Ensure-Dir $DownloadsDir
Ensure-Dir $SrcDir
Ensure-Dir $BuildWorkDir
Ensure-Dir $StageDir
Ensure-Dir (Join-Path $StageDir 'lib')
Ensure-Dir (Join-Path $StageDir 'bin')
Ensure-Dir (Join-Path $StageDir 'include')

if ($env:OPENFGDB4J_GDAL_MINIMAL_REBUILD -eq '1') {
  Write-Host 'Rebuild requested: clearing stage + work directories'
  if (Test-Path -LiteralPath $BuildWorkDir) { Remove-Item -Recurse -Force -LiteralPath $BuildWorkDir }
  if (Test-Path -LiteralPath $StageDir) { Remove-Item -Recurse -Force -LiteralPath $StageDir }
  Ensure-Dir $BuildWorkDir
  Ensure-Dir $StageDir
  Ensure-Dir (Join-Path $StageDir 'lib')
  Ensure-Dir (Join-Path $StageDir 'bin')
  Ensure-Dir (Join-Path $StageDir 'include')
}

# PROJ may need sqlite3.exe during resource/db generation.
$stageBin = Join-Path $StageDir 'bin'
if (Test-Path -LiteralPath $stageBin) {
  $env:PATH = "$stageBin;$env:PATH"
}
$StageDirCmake = To-CmakePath $StageDir
$StageIncludeDirCmake = To-CmakePath (Join-Path $StageDir 'include')
$StageSqliteLibCmake = To-CmakePath (Join-Path $StageDir 'lib/sqlite3.lib')
$StageProjConfigDirCmake = To-CmakePath (Join-Path $StageDir 'lib/cmake/proj')

$GdalVersion = Read-LockResolved 'GDAL_VERSION'
$GdalArchive = Read-LockResolved 'GDAL_ARCHIVE'
$GdalUrl = "https://github.com/OSGeo/gdal/archive/refs/tags/$GdalVersion.tar.gz"
$GdalSrcDirName = Read-LockResolved 'GDAL_SRC_DIR'

$ProjVersion = Read-LockResolved 'PROJ_VERSION'
$ProjArchive = Read-LockResolved 'PROJ_ARCHIVE'
$ProjUrl = "https://github.com/OSGeo/PROJ/archive/refs/tags/$ProjVersion.tar.gz"
$ProjSrcDirName = Read-LockResolved 'PROJ_SRC_DIR'

$SqliteArchive = Read-LockResolved 'SQLITE_ARCHIVE'
$SqliteYear = Read-LockResolved 'SQLITE_YEAR'
$SqliteUrl = "https://www.sqlite.org/$SqliteYear/$SqliteArchive"
$SqliteSrcDirName = Read-LockResolved 'SQLITE_SRC_DIR'

Download-Archive $GdalUrl $GdalArchive
Download-Archive $ProjUrl $ProjArchive
Download-Archive $SqliteUrl $SqliteArchive

Extract-Archive $GdalArchive $GdalSrcDirName
Extract-Archive $ProjArchive $ProjSrcDirName
Extract-Archive $SqliteArchive $SqliteSrcDirName

$SqliteLib = Join-Path $StageDir 'lib/sqlite3.lib'
if (-not (Test-Path -LiteralPath $SqliteLib)) {
  $sqliteSrc = Join-Path $SrcDir $SqliteSrcDirName
  $sqliteBuild = Join-Path $BuildWorkDir 'sqlite'
  Ensure-Dir $sqliteBuild
  $sqliteMachine = Get-LibMachineArg
  $obj = Join-Path $sqliteBuild 'sqlite3.obj'
  Invoke-WithVsDevCmd "cl /nologo /O2 /MT /c /DSQLITE_THREADSAFE=1 /DSQLITE_OMIT_LOAD_EXTENSION=1 /Fo`"$obj`" `"$sqliteSrc/sqlite3.c`""
  Invoke-WithVsDevCmd "lib /nologo /MACHINE:$sqliteMachine /OUT:`"$SqliteLib`" `"$obj`""
  Invoke-WithVsDevCmd "cl /nologo /O2 /MT /Fe`"$StageDir/bin/sqlite3.exe`" `"$sqliteSrc/shell.c`" `"$sqliteSrc/sqlite3.c`""
  Copy-Item -Force -LiteralPath (Join-Path $sqliteSrc 'sqlite3.h') -Destination (Join-Path $StageDir 'include/sqlite3.h')
  Copy-Item -Force -LiteralPath (Join-Path $sqliteSrc 'sqlite3ext.h') -Destination (Join-Path $StageDir 'include/sqlite3ext.h')
  Write-Host "Built sqlite static ($sqliteMachine): $SqliteLib"
} else {
  Write-Host "sqlite already built: $SqliteLib"
}

$ProjLibCandidate1 = Join-Path $StageDir 'lib/proj.lib'
$ProjLibCandidate2 = Join-Path $StageDir 'lib/libproj.lib'
if (-not (Test-Path -LiteralPath $ProjLibCandidate1) -and -not (Test-Path -LiteralPath $ProjLibCandidate2)) {
  $projSrc = Join-Path $SrcDir $ProjSrcDirName
  $projBuild = Join-Path $BuildWorkDir 'proj'
  if (Test-Path -LiteralPath $projBuild) { Remove-Item -Recurse -Force -LiteralPath $projBuild }
  Ensure-Dir $projBuild

  $cmArgs = @(
    '-S', $projSrc,
    '-B', $projBuild,
    '-G', $CmakeGenerator,
    '-A', (Get-CmakeArchArg),
    '-DCMAKE_BUILD_TYPE=Release',
    '-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded',
    '-DCMAKE_POSITION_INDEPENDENT_CODE=ON',
    "-DCMAKE_INSTALL_PREFIX=$StageDirCmake",
    '-DBUILD_SHARED_LIBS=OFF',
    '-DBUILD_TESTING=OFF',
    '-DBUILD_CCT=OFF',
    '-DBUILD_CS2CS=OFF',
    '-DBUILD_GEOD=OFF',
    '-DBUILD_GIE=OFF',
    '-DBUILD_PROJ=OFF',
    '-DBUILD_PROJINFO=OFF',
    '-DBUILD_PROJSYNC=OFF',
    '-DENABLE_TIFF=OFF',
    '-DENABLE_CURL=OFF',
    '-DEMBED_RESOURCE_FILES=ON',
    # Keep embedded proj.db, but do not force embedded-only mode on Windows.
    # With PROJ 9.6.0 this mode excludes Win32 helper code paths in filemanager.cpp
    # and triggers unresolved UTF8/WString helpers during compilation.
    '-DUSE_ONLY_EMBEDDED_RESOURCE_FILES=OFF',
    "-DSQLITE3_INCLUDE_DIR=$StageIncludeDirCmake",
    "-DSQLITE3_LIBRARY=$StageSqliteLibCmake"
  )
  Invoke-Cmake $cmArgs
  $projBuildArgs = @('--build', $projBuild, '--config', 'Release', '--target', 'install', '--verbose')
  Invoke-Cmake $projBuildArgs
  if (-not (Test-Path -LiteralPath $ProjLibCandidate1) -and -not (Test-Path -LiteralPath $ProjLibCandidate2)) {
    Show-StageLibDir
    throw "proj static library missing after build (expected $ProjLibCandidate1 or $ProjLibCandidate2)"
  }
  $projLibPath = $ProjLibCandidate1
  if (-not (Test-Path -LiteralPath $projLibPath)) { $projLibPath = $ProjLibCandidate2 }
  Write-Host "Built proj static: $projLibPath"
} else {
  Write-Host 'proj already built.'
}

$GdalLibCandidate1 = Join-Path $StageDir 'lib/gdal.lib'
$GdalLibCandidate2 = Join-Path $StageDir 'lib/libgdal.lib'
if (-not (Test-Path -LiteralPath $GdalLibCandidate1) -and -not (Test-Path -LiteralPath $GdalLibCandidate2)) {
  $gdalSrc = Join-Path $SrcDir $GdalSrcDirName
  $gdalBuild = Join-Path $BuildWorkDir 'gdal'
  if (Test-Path -LiteralPath $gdalBuild) { Remove-Item -Recurse -Force -LiteralPath $gdalBuild }
  Ensure-Dir $gdalBuild

  $cmArgs = @(
    '-S', $gdalSrc,
    '-B', $gdalBuild,
    '-G', $CmakeGenerator,
    '-A', (Get-CmakeArchArg),
    '-DCMAKE_BUILD_TYPE=Release',
    '-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded',
    '-DCMAKE_POSITION_INDEPENDENT_CODE=ON',
    "-DCMAKE_INSTALL_PREFIX=$StageDirCmake",
    '-DBUILD_SHARED_LIBS=OFF',
    '-DBUILD_APPS=OFF',
    '-DBUILD_TESTING=OFF',
    '-DGDAL_BUILD_OPTIONAL_DRIVERS=OFF',
    '-DOGR_BUILD_OPTIONAL_DRIVERS=OFF',
    '-DOGR_ENABLE_DRIVER_OPENFILEGDB=ON',
    '-DGDAL_USE_EXTERNAL_LIBS=OFF',
    '-DGDAL_USE_INTERNAL_LIBS=ON',
    '-DGDAL_USE_ZLIB_INTERNAL=ON',
    '-DGDAL_USE_TIFF_INTERNAL=ON',
    '-DGDAL_USE_GEOTIFF_INTERNAL=ON',
    '-DGDAL_USE_JSONC_INTERNAL=ON',
    '-DGDAL_USE_LIBPNG_INTERNAL=ON',
    '-DGDAL_USE_CURL=OFF',
    '-DGDAL_USE_EXPAT=OFF',
    "-DPROJ_DIR=$StageProjConfigDirCmake",
    "-DSQLite3_INCLUDE_DIR=$StageIncludeDirCmake",
    "-DSQLite3_LIBRARY=$StageSqliteLibCmake"
  )
  Invoke-Cmake $cmArgs
  $gdalBuildArgs = @('--build', $gdalBuild, '--config', 'Release', '--target', 'install', '--verbose')
  Invoke-Cmake $gdalBuildArgs
  if (-not (Test-Path -LiteralPath $GdalLibCandidate1) -and -not (Test-Path -LiteralPath $GdalLibCandidate2)) {
    Show-StageLibDir
    throw "gdal static library missing after build (expected $GdalLibCandidate1 or $GdalLibCandidate2)"
  }
  $gdalLibPath = $GdalLibCandidate1
  if (-not (Test-Path -LiteralPath $gdalLibPath)) { $gdalLibPath = $GdalLibCandidate2 }
  Write-Host "Built gdal static: $gdalLibPath"
} else {
  Write-Host 'gdal already built.'
}

Write-Host "gdal-minimal build complete"
Write-Host "  target: windows/$TargetArch"
Write-Host "  stage: $StageDir"
