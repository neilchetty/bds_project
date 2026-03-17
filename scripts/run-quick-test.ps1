$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot

# Build the project.
Write-Host "`nBuilding Java project..." -ForegroundColor Cyan
& (Join-Path $PSScriptRoot "build.ps1")

# Run quick single-workflow execution to verify everything works.
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Quick Test: Gene2life + WSH (4 nodes)" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

$TestArgs = @(
    "-jar", (Join-Path $Root "build\wsh-scheduler.jar"),
    "execute",
    "--workflow", "Gene2life",
    "--algorithm", "WSH",
    "--nodes-file", (Join-Path $Root "config\cluster\nodes-4.csv"),
    "--output", (Join-Path $Root "results\quick-test-execution.csv")
)

java @TestArgs

Write-Host "`nQuick test complete! Results: results\quick-test-execution.csv" -ForegroundColor Green
