$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Uploads generated big data files to Hadoop HDFS.
.DESCRIPTION
    Copies the synthetic bioinformatics data from the local 'bigdata/' directory
    into HDFS at /wsh/gene2life/. Also verifies the upload.

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
try {
    docker exec hadoop-namenode /opt/hadoop/bin/hdfs dfs -ls / | Out-Null
} catch {
    Write-Host "ERROR: Hadoop NameNode is not running. Run setup-hadoop.ps1 first." -ForegroundColor Red
    exit 1
}

# Create HDFS directory.
Write-Host "Creating HDFS directory: $HdfsDir" -ForegroundColor Yellow
docker exec hadoop-namenode /opt/hadoop/bin/hdfs dfs -mkdir -p $HdfsDir

# Upload each file.
foreach ($file in $dataFiles) {
    $hdfsPath = "$HdfsDir/$($file.Name)"
    $localInContainer = "/tmp/$($file.Name)"
    $sizeMB = [math]::Round($file.Length / (1024 * 1024), 1)

    Write-Host "  Uploading: $($file.Name) ($sizeMB MB) ..." -ForegroundColor Yellow

    # Copy file into NameNode container first.
    docker cp $file.FullName "hadoop-namenode:$localInContainer"

    # Then put into HDFS.
    docker exec hadoop-namenode /opt/hadoop/bin/hdfs dfs -put -f $localInContainer $hdfsPath

    # Cleanup local copy in container.
    docker exec hadoop-namenode rm -f $localInContainer

    Write-Host "  [OK] $($file.Name) -> $hdfsPath" -ForegroundColor Green
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
