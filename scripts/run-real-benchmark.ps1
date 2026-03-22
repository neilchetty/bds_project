param(
    [Parameter(Mandatory=$false)]
    [switch]$SlowWorkflows  # Pass -SlowWorkflows to run the original (time-consuming) workflows
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot

# Build the project.
Write-Host "`nBuilding Java project..." -ForegroundColor Cyan
& (Join-Path $PSScriptRoot "build.ps1")

# Decide which workflows to use.
$fastFlag = if ($SlowWorkflows) { @() } else { @("--fast") }
$workflowLabel = if ($SlowWorkflows) { "Original workflows (SLOW - may take hours)" } else { "Fast variants (Gene2life + Avianflu_fast + Epigenomics_fast)" }

# Build benchmark arguments.
$BenchmarkArgs = @(
    "-jar", (Join-Path $Root "build\wsh-scheduler.jar"),
    "real-benchmark",
    "--node-counts", "4,7,10,13,16,20,24,28",
    "--output", (Join-Path $Root "results\real-metrics.csv"),
    "--details-dir", (Join-Path $Root "results\real-executions")
) + $fastFlag

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Running Real-World HEFT vs WSH Benchmark" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Workflows : $workflowLabel"
Write-Host "Node counts: 4, 7, 10, 13, 16, 20, 24, 28"
Write-Host "Ensure all Docker containers are running.`n"

java @BenchmarkArgs

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "Benchmark Complete!" -ForegroundColor Green
Write-Host "Results : results\real-metrics.csv" -ForegroundColor Green
Write-Host "Details : results\real-executions\" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
