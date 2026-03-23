param(
    [Parameter(Mandatory=$false)]
    [ValidateRange(1, 100)]
    [int]$SizeGB = 30
)

$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Orchestrates the full big data pipeline for Gene2life on HDFS.
.DESCRIPTION
    Complete end-to-end pipeline:
      1. Ensure Hadoop HDFS is running (or set it up)
      2. Generate synthetic data (if not already present)
      3. Upload data to HDFS
      4. Run Gene2life workflow with WSH and HEFT on real containers
      5. Display comparison results

.PARAMETER SizeGB
    Size of synthetic data in GB (default: 30).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File run-bigdata-gene2life.ps1
    powershell -ExecutionPolicy Bypass -File run-bigdata-gene2life.ps1 -SizeGB 60
#>

$Root = Split-Path -Parent $PSScriptRoot
$ScriptsDir = $PSScriptRoot

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  Gene2life Big Data Pipeline (multi-machine, ${SizeGB}GB) ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# ── Step 1: Ensure Docker worker containers are running ─────────────────
Write-Host "[Step 1/5] Checking Docker containers..." -ForegroundColor Yellow
$workers = docker ps --format "{{.Names}}" | Where-Object { $_ -match "^worker-c" }
$workerCount = ($workers | Measure-Object).Count
if ($workerCount -lt 4) {
    Write-Host "  WARNING: Only $workerCount worker containers running." -ForegroundColor Yellow
    Write-Host "  Starting C1 containers on this PC..." -ForegroundColor Yellow
    & (Join-Path $ScriptsDir "start-multi-machine.ps1")
} else {
    Write-Host "  [OK] $workerCount worker containers running" -ForegroundColor Green
}

# ── Step 2: Ensure Hadoop HDFS is running ───────────────────────────────
Write-Host ""
Write-Host "[Step 2/5] Checking Hadoop HDFS..." -ForegroundColor Yellow
$namenode = docker ps --format "{{.Names}}" | Where-Object { $_ -eq "hadoop-namenode" }
if (-not $namenode) {
    Write-Host "  Setting up Hadoop HDFS..." -ForegroundColor Yellow
    & (Join-Path $ScriptsDir "setup-hadoop.ps1")
} else {
    Write-Host "  [OK] Hadoop NameNode running" -ForegroundColor Green
}

# ── Step 3: Generate data ──────────────────────────────────────────────
Write-Host ""
Write-Host "[Step 3/5] Generating synthetic data ($SizeGB GB)..." -ForegroundColor Yellow
$DataDir = Join-Path $Root "bigdata"
$existingSize = 0
if (Test-Path $DataDir) {
    $existingSize = (Get-ChildItem $DataDir -File | Measure-Object -Property Length -Sum).Sum
}
$targetBytes = [long]$SizeGB * 1024 * 1024 * 1024
if ($existingSize -ge ($targetBytes * 0.90)) {
    Write-Host "  [SKIP] Data already generated ($([math]::Round($existingSize / 1GB, 1)) GB)" -ForegroundColor Green
} else {
    & (Join-Path $ScriptsDir "generate-bigdata.ps1") -SizeGB $SizeGB
}

# ── Step 4: Upload to HDFS ─────────────────────────────────────────────
Write-Host ""
Write-Host "[Step 4/5] Uploading data to HDFS..." -ForegroundColor Yellow
& (Join-Path $ScriptsDir "upload-to-hdfs.ps1")

# ── Step 5: Build and Run Gene2life Benchmark ──────────────────────────
Write-Host ""
Write-Host "[Step 5/5] Running Gene2life benchmark..." -ForegroundColor Yellow

# Build Java project.
& (Join-Path $ScriptsDir "build.ps1")

# Always use multi-machine nodes.
$nodesFile = Join-Path $Root "config\cluster\multi-machine-nodes.csv"

# Run Gene2life with WSH.
Write-Host ""
Write-Host "Running Gene2life with WSH..." -ForegroundColor Cyan
$wshArgs = @(
    "-jar", (Join-Path $Root "build\wsh-scheduler.jar"),
    "execute",
    "--workflow", "Gene2life",
    "--algorithm", "WSH",
    "--nodes-file", $nodesFile,
    "--output", (Join-Path $Root "results\bigdata-gene2life-wsh.csv")
)
java @wshArgs

# Run Gene2life with HEFT.
Write-Host ""
Write-Host "Running Gene2life with HEFT..." -ForegroundColor Cyan
$heftArgs = @(
    "-jar", (Join-Path $Root "build\wsh-scheduler.jar"),
    "execute",
    "--workflow", "Gene2life",
    "--algorithm", "HEFT",
    "--nodes-file", $nodesFile,
    "--output", (Join-Path $Root "results\bigdata-gene2life-heft.csv")
)
java @heftArgs

# ── Results ────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║  Big Data Pipeline Complete!                            ║" -ForegroundColor Green
Write-Host "╠══════════════════════════════════════════════════════════╣" -ForegroundColor Green
Write-Host "║  Results:                                               ║" -ForegroundColor Green
Write-Host "║    WSH:  results\bigdata-gene2life-wsh.csv              ║" -ForegroundColor Green
Write-Host "║    HEFT: results\bigdata-gene2life-heft.csv             ║" -ForegroundColor Green
Write-Host "║  HDFS:   http://localhost:9870                          ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
