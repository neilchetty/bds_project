$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Starts C1 tier containers on this (main) PC for multi-machine mode.
.DESCRIPTION
    In multi-machine mode:
      - Main PC (this machine):  Runs C1 tier (7 containers) + Java scheduler
      - PC2 (remote):            Runs C2 tier (7 containers)
      - PC3 (remote):            Runs C3 tier (7 containers)
      - PC4 (remote):            Runs C4 tier (7 containers)
    
    This script starts only the C1 tier on the main PC.
    Remote PCs must run their own containers using the per-tier compose files.
#>

$Root = Split-Path -Parent $PSScriptRoot

Write-Host ""
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "  WSH Multi-Machine Setup - MAIN PC (C1 tier, 7 containers)             " -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host ""

# Check Docker is available.
try {
    docker version | Out-Null
} catch {
    Write-Host "ERROR: Docker is not installed or not running." -ForegroundColor Red
    Write-Host "Please install Docker Desktop and start it first." -ForegroundColor Red
    exit 1
}

# Pull the base image.
Write-Host "Pulling ubuntu:22.04 image..." -ForegroundColor Yellow
docker pull ubuntu:22.04

# Start C1 tier containers.
Write-Host ""
Write-Host "Starting C1 tier containers (worker-c1-1 through worker-c1-7)..." -ForegroundColor Yellow
Push-Location $Root
docker compose up -d
Pop-Location

# Wait for initialization.
Write-Host ""
Write-Host "Waiting 15 seconds for containers to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# Verify local containers.
Write-Host ""
Write-Host "Local C1 containers:" -ForegroundColor Green
docker ps --format "table {{.Names}}`t{{.Status}}" --filter "name=worker-c1"

# Get this machine's IP.
Write-Host ""
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "  This PC's network addresses:                                          " -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Get-NetIPAddress -AddressFamily IPv4 | Where-Object {
    $_.InterfaceAlias -notmatch "Loopback" -and $_.IPAddress -ne "127.0.0.1"
} | ForEach-Object {
    Write-Host "  Interface: $($_.InterfaceAlias)  IP: $($_.IPAddress)" -ForegroundColor White
}

Write-Host ""
Write-Host "========================================================================" -ForegroundColor Yellow
Write-Host "  NEXT STEPS FOR MULTI-MACHINE SETUP:                                   " -ForegroundColor Yellow
Write-Host "========================================================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "  1. On EACH remote PC, enable Docker TCP API:" -ForegroundColor White
Write-Host "       - Open Docker Desktop > Settings > General" -ForegroundColor Gray
Write-Host "       - Check 'Expose daemon on tcp://localhost:2375 without TLS'" -ForegroundColor Gray
Write-Host "       - Then run (as Administrator):" -ForegroundColor Gray
Write-Host "         powershell -ExecutionPolicy Bypass -File docker-inbound.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "  2. Copy the per-tier compose file to each remote PC:" -ForegroundColor White
Write-Host "       PC2: docker-compose-c2.yml  (run: docker compose -f docker-compose-c2.yml up -d)" -ForegroundColor Cyan
Write-Host "       PC3: docker-compose-c3.yml  (run: docker compose -f docker-compose-c3.yml up -d)" -ForegroundColor Cyan
Write-Host "       PC4: docker-compose-c4.yml  (run: docker compose -f docker-compose-c4.yml up -d)" -ForegroundColor Cyan
Write-Host ""
Write-Host "  3. Edit config\ip.txt with the 3 remote PC IPs (one per line)" -ForegroundColor White
Write-Host ""
Write-Host "  4. Run configure-cluster.ps1 to update the node CSV:" -ForegroundColor White
Write-Host "       powershell -ExecutionPolicy Bypass -File scripts\configure-cluster.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "  5. Verify connectivity from this PC:" -ForegroundColor White
Write-Host "       docker -H tcp://<PC2_IP>:2375 ps" -ForegroundColor Cyan
Write-Host "       docker -H tcp://<PC3_IP>:2375 ps" -ForegroundColor Cyan
Write-Host "       docker -H tcp://<PC4_IP>:2375 ps" -ForegroundColor Cyan
Write-Host ""
Write-Host "  6. Run the benchmark:" -ForegroundColor White
Write-Host "       powershell -ExecutionPolicy Bypass -File scripts\run-real-benchmark.ps1" -ForegroundColor Cyan
Write-Host ""
