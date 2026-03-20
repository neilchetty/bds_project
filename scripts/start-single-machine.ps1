$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Starts all 28 Docker worker containers on this single machine.
.DESCRIPTION
    Uses docker compose to start all 4 tiers (C1-C4, 7 nodes each = 28 containers).
    
    Requirements:
      - Docker Desktop installed and running
      - At least 36 GB free RAM and 24 CPU cores recommended
#>

$Root = Split-Path -Parent $PSScriptRoot

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  WSH Single-Machine Setup (28 containers on this PC)   " -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

# Check Docker is available.
try {
    docker version | Out-Null
} catch {
    Write-Host "ERROR: Docker is not installed or not running." -ForegroundColor Red
    Write-Host "Please install Docker Desktop and start it first." -ForegroundColor Red
    exit 1
}

# Pull the base image first.
Write-Host "Pulling ubuntu:22.04 image..." -ForegroundColor Yellow
docker pull ubuntu:22.04

# Start all containers.
Write-Host ""
Write-Host "Starting all 28 containers (C1-C4 tiers)..." -ForegroundColor Yellow
Push-Location $Root
docker compose up -d
Pop-Location

# Wait for containers to initialize (installing coreutils).
Write-Host ""
Write-Host "Waiting 30 seconds for containers to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Verify containers are running.
Write-Host ""
Write-Host "Verifying containers..." -ForegroundColor Yellow
$running = docker ps --format "{{.Names}}" | Where-Object { $_ -match "^worker-c" } | Sort-Object
$count = ($running | Measure-Object).Count

Write-Host ""
Write-Host "Running containers ($count):" -ForegroundColor Green
$running | ForEach-Object { Write-Host "  $_" -ForegroundColor Green }

if ($count -lt 28) {
    Write-Host ""
    Write-Host "WARNING: Expected 28 containers but found $count." -ForegroundColor Yellow
    Write-Host "Some containers may still be starting. Run 'docker ps' to check." -ForegroundColor Yellow
} else {
    Write-Host ""
    Write-Host "========================================================" -ForegroundColor Green
    Write-Host "  All 28 containers running! Ready for benchmarks.      " -ForegroundColor Green
    Write-Host "========================================================" -ForegroundColor Green
}

Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Build:     powershell -ExecutionPolicy Bypass -File scripts\build.ps1" -ForegroundColor White
Write-Host "  2. Test:      powershell -ExecutionPolicy Bypass -File scripts\run-quick-test.ps1" -ForegroundColor White
Write-Host "  3. Benchmark: powershell -ExecutionPolicy Bypass -File scripts\run-real-benchmark.ps1" -ForegroundColor White
Write-Host ""
