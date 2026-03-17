$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$BuildDir = Join-Path $Root "build"
$MainOut = Join-Path $BuildDir "classes"
$TestOut = Join-Path $BuildDir "test-classes"

& (Join-Path $PSScriptRoot "build.ps1")
New-Item -ItemType Directory -Force -Path $TestOut | Out-Null
$TestSources = @(Get-ChildItem -Path (Join-Path $Root "src/test/java") -Recurse -Filter *.java | ForEach-Object { $_.FullName })
javac -cp $MainOut -d $TestOut $TestSources
java -cp "$MainOut;$TestOut" org.bds.wsh.tests.TestMain
