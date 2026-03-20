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
    Remote PCs must run their own containers via setup-remote-machine.ps1.
    
    After all machines are up, edit the node CSV files to set the remote IPs.
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

# Start only C1 tier (no profile = only services without profiles).
Write-Host ""
Write-Host "Starting C1 tier containers (worker-c1-1 through worker-c1-7)..." -ForegroundColor Yellow
Push-Location $Root
docker compose up -d
Pop-Location

# Wait for initialization.
Write-Host ""
Write-Host "Waiting 10 seconds for containers to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

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
Write-Host "  1. Copy these files to EACH remote PC (PC2, PC3, PC4):" -ForegroundColor White
Write-Host "       - scripts\setup-remote-machine.ps1" -ForegroundColor White
Write-Host "       - docker-compose-worker.yml" -ForegroundColor White
Write-Host ""
Write-Host "  2. On each remote PC, run (as Administrator):" -ForegroundColor White
Write-Host "       powershell -ExecutionPolicy Bypass -File setup-remote-machine.ps1 -Tier C2" -ForegroundColor Cyan
Write-Host "       (Use -Tier C3 for PC3, -Tier C4 for PC4)" -ForegroundColor Gray
Write-Host ""
Write-Host "  3. Note each remote PC's IP address (run 'ipconfig' on each)" -ForegroundColor White
Write-Host ""
Write-Host "  4. Back on THIS PC, update the node CSV files with the IPs:" -ForegroundColor White
Write-Host "       Edit: config\cluster\multi-machine-nodes.csv" -ForegroundColor White
Write-Host "       Replace REPLACE_PC2_IP, REPLACE_PC3_IP, REPLACE_PC4_IP" -ForegroundColor White
Write-Host ""
Write-Host "  5. Verify connectivity from this PC:" -ForegroundColor White
Write-Host "       docker -H tcp://<PC2_IP>:2375 ps" -ForegroundColor Cyan
Write-Host "       docker -H tcp://<PC3_IP>:2375 ps" -ForegroundColor Cyan
Write-Host "       docker -H tcp://<PC4_IP>:2375 ps" -ForegroundColor Cyan
Write-Host ""
Write-Host "  6. Run the benchmark:" -ForegroundColor White
Write-Host "       powershell -ExecutionPolicy Bypass -File scripts\run-real-benchmark.ps1" -ForegroundColor Cyan
Write-Host ""
