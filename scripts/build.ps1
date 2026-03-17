$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$BuildDir = Join-Path $Root "build"
$MainOut = Join-Path $BuildDir "classes"
$JarPath = Join-Path $BuildDir "wsh-scheduler.jar"

if (Test-Path $BuildDir) {
    Remove-Item -Recurse -Force $BuildDir
}

New-Item -ItemType Directory -Force -Path $MainOut | Out-Null
$Sources = @(Get-ChildItem -Path (Join-Path $Root "src/main/java") -Recurse -Filter *.java | ForEach-Object { $_.FullName })
if ($Sources.Count -eq 0) {
    throw "No Java sources found."
}

javac -d $MainOut $Sources
jar --create --file $JarPath --main-class org.bds.wsh.cli.Main -C $MainOut .
Write-Host "Built $JarPath" -ForegroundColor Green
