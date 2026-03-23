$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Uploads generated big data files to Hadoop HDFS.
.DESCRIPTION
    The NameNode container has /bigdata volume-mounted from the host.
    This script simply runs 'hdfs dfs -put' via docker exec to upload
    files from /bigdata into HDFS. No docker cp, no separate containers,
    no cross-network issues.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File upload-to-hdfs.ps1
#>

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "bigdata"
$HdfsDir = "/wsh/gene2life"

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Uploading Big Data to HDFS                            " -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

# Check that data exists.
if (-not (Test-Path $DataDir)) {
    Write-Host "ERROR: bigdata/ directory not found. Run generate-bigdata.ps1 first." -ForegroundColor Red
    exit 1
}

$dataFiles = Get-ChildItem $DataDir -File
if ($dataFiles.Count -eq 0) {
    Write-Host "ERROR: No data files found in bigdata/." -ForegroundColor Red
    exit 1
}

# Check NameNode is running.
$nnCheck = docker ps --filter "name=hadoop-namenode" --format "{{.Names}}" 2>&1
if (-not $nnCheck) {
    Write-Host "ERROR: Hadoop NameNode is not running. Run setup-hadoop.ps1 first." -ForegroundColor Red
    exit 1
}

# Verify /bigdata mount exists inside the NameNode.
$mountCheck = docker exec hadoop-namenode ls /bigdata 2>&1 | Out-String
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: /bigdata not mounted in NameNode container." -ForegroundColor Red
    Write-Host "  Re-run setup-hadoop.ps1 to recreate the NameNode with the volume mount." -ForegroundColor Yellow
    exit 1
}

# Create HDFS directory.
Write-Host "Creating HDFS directory: $HdfsDir" -ForegroundColor Yellow
docker exec hadoop-namenode /opt/hadoop/bin/hdfs dfs -mkdir -p $HdfsDir 2>&1 | Out-Null

# Upload each file directly from the /bigdata mount inside the NameNode.
# Since the local DataNode is on the same Docker network, writes always succeed.
foreach ($file in $dataFiles) {
    $hdfsPath = "$HdfsDir/$($file.Name)"
    $containerPath = "/bigdata/$($file.Name)"
    $sizeMB = [math]::Round($file.Length / (1024 * 1024), 1)

    Write-Host "  Uploading: $($file.Name) ($sizeMB MB) ..." -ForegroundColor Yellow

    # Temporarily relax error preference so HDFS WARN logs don't crash PowerShell.
    $savedEAP = $ErrorActionPreference
    $ErrorActionPreference = "Continue"

    $output = docker exec hadoop-namenode /opt/hadoop/bin/hdfs dfs -put -f $containerPath $hdfsPath 2>&1
    $exitCode = $LASTEXITCODE

    $ErrorActionPreference = $savedEAP

    if ($exitCode -ne 0) {
        Write-Host "  [FAIL] Upload failed for $($file.Name) (exit code $exitCode)" -ForegroundColor Red
        $output | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
    } else {
        Write-Host "  [OK] $($file.Name) -> $hdfsPath" -ForegroundColor Green
    }
}

# Verify upload.
Write-Host ""
Write-Host "Verifying HDFS contents:" -ForegroundColor Yellow
docker exec hadoop-namenode /opt/hadoop/bin/hdfs dfs -ls -h $HdfsDir

Write-Host ""
Write-Host "HDFS Storage Report:" -ForegroundColor Yellow
docker exec hadoop-namenode /opt/hadoop/bin/hdfs dfs -du -h -s $HdfsDir

Write-Host ""
Write-Host "========================================================" -ForegroundColor Green
Write-Host "  Upload Complete!                                       " -ForegroundColor Green
Write-Host "  HDFS path: $HdfsDir                                   " -ForegroundColor Green
Write-Host "========================================================" -ForegroundColor Green
Write-Host ""
