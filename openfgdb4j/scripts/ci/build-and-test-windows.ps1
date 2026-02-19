$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ProjectDir = (Resolve-Path (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) '../..')).Path
$RootDir = (Resolve-Path (Join-Path $ProjectDir '..')).Path

$TargetArch = $env:OPENFGDB4J_TARGET_ARCH
if ([string]::IsNullOrWhiteSpace($TargetArch)) { $TargetArch = 'amd64' }
$TargetArch = $TargetArch.ToLowerInvariant()
if ($TargetArch -eq 'x86_64') { $TargetArch = 'amd64' }
if ($TargetArch -eq 'aarch64') { $TargetArch = 'arm64' }
if ($TargetArch -ne 'amd64' -and $TargetArch -ne 'arm64') {
  throw "Unsupported OPENFGDB4J_TARGET_ARCH=$TargetArch"
}

$env:OPENFGDB4J_TARGET_OS = 'windows'
$env:OPENFGDB4J_TARGET_ARCH = $TargetArch
if ([string]::IsNullOrWhiteSpace($env:OPENFGDB4J_CMAKE_GENERATOR)) {
  $env:OPENFGDB4J_CMAKE_GENERATOR = 'Visual Studio 17 2022'
}
if ([string]::IsNullOrWhiteSpace($env:CMAKE_BIN)) {
  $env:CMAKE_BIN = 'cmake'
}

& (Join-Path $RootDir 'gdal-minimal/scripts/build-all.ps1')

$BuildDir = Join-Path $ProjectDir 'build/native'
if (Test-Path -LiteralPath $BuildDir) {
  Remove-Item -Recurse -Force -LiteralPath $BuildDir
}

$cmArch = if ($TargetArch -eq 'arm64') { 'ARM64' } else { 'x64' }
$stageRoot = $env:OPENFGDB4J_GDAL_MINIMAL_ROOT
if ([string]::IsNullOrWhiteSpace($stageRoot)) {
  $stageRoot = Join-Path $RootDir 'gdal-minimal/build/stage'
}

& $env:CMAKE_BIN -S (Join-Path $ProjectDir 'native') -B $BuildDir -G $env:OPENFGDB4J_CMAKE_GENERATOR -A $cmArch -DCMAKE_BUILD_TYPE=Release -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded "-DOPENFGDB4J_GDAL_MINIMAL_ROOT=$stageRoot"
if ($LASTEXITCODE -ne 0) { throw 'cmake configure failed' }
& $env:CMAKE_BIN --build $BuildDir --config Release
if ($LASTEXITCODE -ne 0) { throw 'cmake build failed' }

$javac = $env:JAVAC_BIN
if ([string]::IsNullOrWhiteSpace($javac)) { $javac = 'javac' }
$jar = $env:JAR_BIN
if ([string]::IsNullOrWhiteSpace($jar)) { $jar = 'jar' }
$java = $env:JAVA_BIN
if ([string]::IsNullOrWhiteSpace($java)) { $java = 'java' }

$classesDir = Join-Path $ProjectDir 'build/classes'
$jarDir = Join-Path $ProjectDir 'build/java'
if (Test-Path -LiteralPath $classesDir) {
  Remove-Item -Recurse -Force -LiteralPath $classesDir
}
if (-not (Test-Path -LiteralPath $jarDir)) {
  New-Item -ItemType Directory -Path $jarDir -Force | Out-Null
}
New-Item -ItemType Directory -Path $classesDir -Force | Out-Null

$mainSources = @()
$mainSources += @(Get-ChildItem -Path (Join-Path $ProjectDir 'src/main/java') -Recurse -File -Filter '*.java' | ForEach-Object { $_.FullName })
$mainSources += @(Get-ChildItem -Path (Join-Path $ProjectDir 'src/generated/java') -Recurse -File -Filter '*.java' | ForEach-Object { $_.FullName })
if ($mainSources.Count -eq 0) { throw 'No openfgdb4j Java sources found' }

& $javac '--release' '22' '-d' $classesDir @mainSources
if ($LASTEXITCODE -ne 0) { throw 'javac failed for openfgdb4j sources' }
& $jar '--create' '--file' (Join-Path $jarDir 'openfgdb4j.jar') '-C' $classesDir '.'
if ($LASTEXITCODE -ne 0) { throw 'jar creation failed' }

$ciClasses = Join-Path $ProjectDir 'build/ci-classes'
if (Test-Path -LiteralPath $ciClasses) {
  Remove-Item -Recurse -Force -LiteralPath $ciClasses
}
New-Item -ItemType Directory -Path $ciClasses -Force | Out-Null

$ciSources = @(Get-ChildItem -Path (Join-Path $ProjectDir 'src/test/java/ch/ehi/openfgdb4j/ci') -Recurse -File -Filter '*.java' | ForEach-Object { $_.FullName })
if ($ciSources.Count -eq 0) {
  throw 'No CI smoke Java sources found in openfgdb4j/src/test/java/ch/ehi/openfgdb4j/ci'
}

$openfgdbJar = Join-Path $jarDir 'openfgdb4j.jar'
& $javac '--release' '22' '-cp' $openfgdbJar '-d' $ciClasses @ciSources
if ($LASTEXITCODE -ne 0) { throw 'javac failed for CI smoke sources' }

$libPath = Join-Path $ProjectDir 'build/native/Release/openfgdb.dll'
if (-not (Test-Path -LiteralPath $libPath)) {
  $alt = Get-ChildItem -Path (Join-Path $ProjectDir 'build/native') -Recurse -File -Filter 'openfgdb.dll' | Select-Object -First 1
  if (-not $alt) {
    throw 'openfgdb.dll not found after native build'
  }
  $libPath = $alt.FullName
}

$cp = "$ciClasses;$openfgdbJar"

function Invoke-Scenario([string]$backend, [string]$scenario, [string]$gdalForceFail = '') {
  $oldBackend = $env:OPENFGDB4J_BACKEND
  $oldFail = $env:OPENFGDB4J_GDAL_FORCE_FAIL
  try {
    $env:OPENFGDB4J_BACKEND = $backend
    if ([string]::IsNullOrWhiteSpace($gdalForceFail)) {
      if (Test-Path env:OPENFGDB4J_GDAL_FORCE_FAIL) { Remove-Item env:OPENFGDB4J_GDAL_FORCE_FAIL }
    } else {
      $env:OPENFGDB4J_GDAL_FORCE_FAIL = $gdalForceFail
    }

    & $java '--enable-native-access=ALL-UNNAMED' "-Dopenfgdb4j.lib=$libPath" '-cp' $cp 'ch.ehi.openfgdb4j.ci.OpenFgdbCiSmokeMain' $scenario
    if ($LASTEXITCODE -ne 0) {
      throw "Scenario failed: backend=$backend scenario=$scenario"
    }
  } finally {
    if ($null -eq $oldBackend) {
      if (Test-Path env:OPENFGDB4J_BACKEND) { Remove-Item env:OPENFGDB4J_BACKEND }
    } else {
      $env:OPENFGDB4J_BACKEND = $oldBackend
    }
    if ($null -eq $oldFail) {
      if (Test-Path env:OPENFGDB4J_GDAL_FORCE_FAIL) { Remove-Item env:OPENFGDB4J_GDAL_FORCE_FAIL }
    } else {
      $env:OPENFGDB4J_GDAL_FORCE_FAIL = $oldFail
    }
  }
}

Invoke-Scenario 'gdal' 'gdal'
Invoke-Scenario 'adapter' 'adapter'
Invoke-Scenario 'gdal' 'gdal-fail' '1'
Invoke-Scenario 'invalid' 'invalid-backend'

& (Join-Path $ProjectDir 'scripts/ci/check-linkage-windows.ps1') -LibraryPath $libPath

Write-Host "build-and-test-windows.ps1 OK for windows/$TargetArch"
