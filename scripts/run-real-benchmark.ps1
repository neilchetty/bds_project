$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot

# Build the project.
Write-Host "`nBuilding Java project..." -ForegroundColor Cyan
& (Join-Path $PSScriptRoot "build.ps1")

# Build benchmark arguments.
$BenchmarkArgs = @(
    "-jar", (Join-Path $Root "build\wsh-scheduler.jar"),
    "real-benchmark",
    "--node-counts", "4,7,10,13",
    "--output", (Join-Path $Root "results\real-metrics.csv"),
    "--details-dir", (Join-Path $Root "results\real-executions")
)

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Running Real-World HEFT vs WSH Benchmark" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan
Write-Host "This will execute real workloads on Docker containers."
Write-Host "Ensure all Docker containers are running on all machines.`n"

java @BenchmarkArgs

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "Benchmark Complete!" -ForegroundColor Green
Write-Host "Results: results\real-metrics.csv" -ForegroundColor Green
Write-Host "Details: results\real-executions\" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
