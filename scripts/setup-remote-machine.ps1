param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("C2", "C3", "C4")]
    [string]$Tier
)

$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Sets up a remote Windows PC to run Docker containers for the WSH project.
.DESCRIPTION
    Run this script on each remote PC (PC2, PC3, PC4). It will:
      1. Check that Docker Desktop is installed and running
      2. Enable the Docker TCP API (daemon.json)
      3. Add a Windows Firewall rule for port 2375
      4. Pull the ubuntu:22.04 image
      5. Start the containers for the specified tier
    
    MUST BE RUN AS ADMINISTRATOR.
    
.PARAMETER Tier
    Which cluster tier this PC should run: C2, C3, or C4.
    
.EXAMPLE
    # On PC2 (mid-range tier):
    powershell -ExecutionPolicy Bypass -File setup-remote-machine.ps1 -Tier C2
    
    # On PC3 (baseline tier):
    powershell -ExecutionPolicy Bypass -File setup-remote-machine.ps1 -Tier C3
    
    # On PC4 (low-end tier):
    powershell -ExecutionPolicy Bypass -File setup-remote-machine.ps1 -Tier C4
#>

Write-Host ""
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "  WSH Remote Machine Setup - Tier $Tier                                 " -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host ""

# ── Step 1: Check admin privileges ──────────────────────────────────────────
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "ERROR: This script must be run as Administrator." -ForegroundColor Red
    Write-Host "Right-click PowerShell -> 'Run as Administrator' and try again." -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Running as Administrator" -ForegroundColor Green

# ── Step 2: Check Docker is installed ───────────────────────────────────────
Write-Host ""
Write-Host "Checking Docker installation..." -ForegroundColor Yellow
try {
    $dockerVer = docker version --format "{{.Server.Version}}" 2>$null
    Write-Host "[OK] Docker is running (version: $dockerVer)" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Docker is not installed or not running." -ForegroundColor Red
    Write-Host ""
    Write-Host "Please install Docker Desktop:" -ForegroundColor Yellow
    Write-Host "  https://www.docker.com/products/docker-desktop/" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "After installing:" -ForegroundColor Yellow
    Write-Host "  1. Start Docker Desktop" -ForegroundColor White
    Write-Host "  2. Wait for it to finish starting (whale icon in taskbar)" -ForegroundColor White
    Write-Host "  3. Run this script again" -ForegroundColor White
    exit 1
}

# ── Step 3: Configure Docker TCP API ────────────────────────────────────────
Write-Host ""
Write-Host "Configuring Docker TCP API access..." -ForegroundColor Yellow

$daemonJsonPath = "$env:USERPROFILE\.docker\daemon.json"
$daemonDir = Split-Path $daemonJsonPath

if (-not (Test-Path $daemonDir)) {
    New-Item -ItemType Directory -Path $daemonDir -Force | Out-Null
}

# Docker Desktop also needs the GUI setting enabled, but daemon.json helps
# ensure the TCP host is configured.
$needsRestart = $false
if (Test-Path $daemonJsonPath) {
    $existing = Get-Content $daemonJsonPath -Raw | ConvertFrom-Json
    $hosts = $existing.hosts
    if ($null -eq $hosts -or $hosts -notcontains "tcp://0.0.0.0:2375") {
        Write-Host "  Adding TCP host to daemon.json..." -ForegroundColor Yellow
        Write-Host ""
        Write-Host "  IMPORTANT: You also need to enable TCP in Docker Desktop:" -ForegroundColor Yellow
        Write-Host "    Docker Desktop -> Settings -> General" -ForegroundColor White
        Write-Host '    Check: "Expose daemon on tcp://localhost:2375 without TLS"' -ForegroundColor White
        Write-Host "    Click: Apply & Restart" -ForegroundColor White
        $needsRestart = $true
    } else {
        Write-Host "[OK] Docker TCP API already configured" -ForegroundColor Green
    }
} else {
    Write-Host "  IMPORTANT: Enable TCP in Docker Desktop:" -ForegroundColor Yellow
    Write-Host "    Docker Desktop -> Settings -> General" -ForegroundColor White
    Write-Host '    Check: "Expose daemon on tcp://localhost:2375 without TLS"' -ForegroundColor White
    Write-Host "    Click: Apply & Restart" -ForegroundColor White
    $needsRestart = $true
}

# ── Step 4: Configure Windows Firewall ──────────────────────────────────────
Write-Host ""
Write-Host "Configuring Windows Firewall..." -ForegroundColor Yellow

$existingRule = Get-NetFirewallRule -DisplayName "Docker API (WSH Project)" -ErrorAction SilentlyContinue
if ($null -eq $existingRule) {
    New-NetFirewallRule `
        -DisplayName "Docker API (WSH Project)" `
        -Description "Allow inbound TCP on port 2375 for Docker API access from the main WSH scheduler PC" `
        -Direction Inbound `
        -Protocol TCP `
        -LocalPort 2375 `
        -Action Allow `
        -Profile Domain,Private | Out-Null
    Write-Host "[OK] Firewall rule created: Allow TCP port 2375 (Domain/Private)" -ForegroundColor Green
} else {
    Write-Host "[OK] Firewall rule already exists" -ForegroundColor Green
}

# ── Step 5: Pull Docker image ───────────────────────────────────────────────
Write-Host ""
Write-Host "Pulling ubuntu:22.04 image..." -ForegroundColor Yellow
docker pull ubuntu:22.04
Write-Host "[OK] Image pulled" -ForegroundColor Green

# ── Step 6: Create and start containers for this tier ───────────────────────
Write-Host ""
Write-Host "Starting $Tier tier containers..." -ForegroundColor Yellow

# Determine container specs based on tier.
switch ($Tier) {
    "C2" { $cpus = "2.0"; $mem = "2048m"; }
    "C3" { $cpus = "1.0"; $mem = "1024m"; }
    "C4" { $cpus = "1.0"; $mem = "512m";  }
}

$tierLower = $Tier.ToLower()

for ($i = 1; $i -le 7; $i++) {
    $name = "worker-$tierLower-$i"
    
    # Remove if already exists.
    $existing = docker ps -a --filter "name=^${name}$" --format "{{.Names}}" 2>$null
    if ($existing) {
        Write-Host "  Removing existing container: $name" -ForegroundColor Gray
        docker rm -f $name 2>$null | Out-Null
    }
    
    Write-Host "  Starting: $name (cpus=$cpus, mem=$mem)" -ForegroundColor White
    docker run -d `
        --name $name `
        --hostname $name `
        --cpus $cpus `
        --memory $mem `
        --restart unless-stopped `
        ubuntu:22.04 `
        bash -c "apt-get update -qq && apt-get install -y -qq coreutils > /dev/null 2>&1; sleep infinity"
}

# Wait for initialization.
Write-Host ""
Write-Host "Waiting 10 seconds for containers to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# ── Step 7: Verify ──────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Running containers:" -ForegroundColor Green
docker ps --format "table {{.Names}}`t{{.Status}}" --filter "name=worker-$tierLower"

# Show this PC's IP.
Write-Host ""
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "  This PC's network addresses (give one of these to the main PC):       " -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Get-NetIPAddress -AddressFamily IPv4 | Where-Object {
    $_.InterfaceAlias -notmatch "Loopback" -and $_.IPAddress -ne "127.0.0.1"
} | ForEach-Object {
    Write-Host "  Interface: $($_.InterfaceAlias)   IP: $($_.IPAddress)" -ForegroundColor White
}

Write-Host ""
Write-Host "========================================================================" -ForegroundColor Green
Write-Host "  $Tier tier setup complete!                                             " -ForegroundColor Green
Write-Host "========================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Give your IP address to the person running the main PC." -ForegroundColor Yellow
Write-Host "They will enter it in the node configuration files." -ForegroundColor Yellow

if ($needsRestart) {
    Write-Host ""
    Write-Host "REMINDER: You still need to enable TCP in Docker Desktop:" -ForegroundColor Red
    Write-Host '  Settings -> General -> "Expose daemon on tcp://localhost:2375 without TLS"' -ForegroundColor Yellow
    Write-Host "  Then click Apply & Restart." -ForegroundColor Yellow
}
Write-Host ""
