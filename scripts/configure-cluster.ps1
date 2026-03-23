$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Reads IPs from config/ip.txt and updates multi-machine node CSV files.
.DESCRIPTION
    Automatically replaces REPLACE_PC2_IP, REPLACE_PC3_IP, REPLACE_PC4_IP
    placeholders in the multi-machine node CSV with actual IPs from ip.txt.
    Also validates IP format and optionally tests connectivity.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File configure-cluster.ps1
#>

$Root = Split-Path -Parent $PSScriptRoot
$IpFile = Join-Path $Root "config\ip.txt"
$NodesCsv = Join-Path $Root "config\cluster\multi-machine-nodes.csv"

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Configuring Multi-Machine Cluster from ip.txt         " -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

# Read IPs.
if (-not (Test-Path $IpFile)) {
    Write-Host "ERROR: $IpFile not found." -ForegroundColor Red
    Write-Host "Create it with one IP per line (PC2, PC3, PC4)." -ForegroundColor Red
    exit 1
}

$ips = @(Get-Content $IpFile | Where-Object { $_ -and $_ -notmatch "^\s*#" } | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })

if ($ips.Count -ne 3) {
    Write-Host "ERROR: Expected 3 IPs in ip.txt (PC2, PC3, PC4), found $($ips.Count)." -ForegroundColor Red
    Write-Host "Contents:" -ForegroundColor Yellow
    $ips | ForEach-Object { Write-Host "  $_" -ForegroundColor White }
    exit 1
}

$pc2Ip = $ips[0]
$pc3Ip = $ips[1]
$pc4Ip = $ips[2]

# Validate IP format.
$ipRegex = '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
foreach ($ip in $ips) {
    if ($ip -notmatch $ipRegex) {
        Write-Host "ERROR: Invalid IP format: $ip" -ForegroundColor Red
        exit 1
    }
}

Write-Host "  PC2 (C2 tier): $pc2Ip" -ForegroundColor White
Write-Host "  PC3 (C3 tier): $pc3Ip" -ForegroundColor White
Write-Host "  PC4 (C4 tier): $pc4Ip" -ForegroundColor White

# Update multi-machine-nodes.csv.
Write-Host ""
Write-Host "Updating $NodesCsv ..." -ForegroundColor Yellow

$csvContent = Get-Content $NodesCsv -Raw
$csvContent = $csvContent -replace "REPLACE_PC2_IP", $pc2Ip
$csvContent = $csvContent -replace "REPLACE_PC3_IP", $pc3Ip
$csvContent = $csvContent -replace "REPLACE_PC4_IP", $pc4Ip
Set-Content $NodesCsv -Value $csvContent

Write-Host "[OK] Updated multi-machine-nodes.csv" -ForegroundColor Green

# Test connectivity (optional).
Write-Host ""
Write-Host "Testing Docker API connectivity..." -ForegroundColor Yellow
$allOk = $true
foreach ($ip in $ips) {
    Write-Host -NoNewline "  $ip ... "
    try {
        $result = docker -H "tcp://${ip}:2375" version --format "{{.Server.Version}}" 2>$null
        Write-Host "OK (Docker $result)" -ForegroundColor Green
    } catch {
        Write-Host "FAILED" -ForegroundColor Red
        Write-Host "    Ensure Docker TCP API is enabled on this PC." -ForegroundColor Yellow
        $allOk = $false
    }
}

Write-Host ""
if ($allOk) {
    Write-Host "========================================================" -ForegroundColor Green
    Write-Host "  All PCs configured and accessible!                    " -ForegroundColor Green
    Write-Host "========================================================" -ForegroundColor Green
} else {
    Write-Host "========================================================" -ForegroundColor Yellow
    Write-Host "  Configuration saved, but some PCs are unreachable.    " -ForegroundColor Yellow
    Write-Host "  Fix connectivity before running benchmarks.           " -ForegroundColor Yellow
    Write-Host "========================================================" -ForegroundColor Yellow
}
Write-Host ""
