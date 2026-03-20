param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("single", "multi")]
    [string]$Phase = "single"
)

$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Sets up a Hadoop HDFS cluster using Docker containers.
.DESCRIPTION
    Creates a NameNode and DataNode(s) on the existing Docker network.
    For single-machine: 1 NameNode + 2 DataNodes on local Docker.
    For multi-machine: 1 NameNode (main PC) + 1 DataNode per remote PC.

.PARAMETER Phase
    'single' (default) or 'multi' for multi-machine deployment.
.EXAMPLE
    powershell -ExecutionPolicy Bypass -File setup-hadoop.ps1
    powershell -ExecutionPolicy Bypass -File setup-hadoop.ps1 -Phase multi
#>

$Root = Split-Path -Parent $PSScriptRoot
$HadoopImage = "apache/hadoop:3.3.6"
$NetworkName = "wsh-hadoop-net"

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Setting up Hadoop HDFS ($Phase mode)                  " -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

# Check Docker.
try { docker version | Out-Null } catch {
    Write-Host "ERROR: Docker is not running." -ForegroundColor Red
    exit 1
}

# Pull Hadoop image.
Write-Host "Pulling Hadoop image: $HadoopImage ..." -ForegroundColor Yellow
docker pull $HadoopImage

# Create network if it doesn't exist.
$existing = docker network ls --format "{{.Name}}" | Where-Object { $_ -eq $NetworkName }
if (-not $existing) {
    Write-Host "Creating Docker network: $NetworkName" -ForegroundColor Yellow
    docker network create $NetworkName
}

# Stop existing Hadoop containers.
@("hadoop-namenode", "hadoop-datanode1", "hadoop-datanode2", "hadoop-datanode3") | ForEach-Object {
    $c = docker ps -a --filter "name=^${_}$" --format "{{.Names}}" 2>$null
    if ($c) {
        Write-Host "  Removing existing: $_" -ForegroundColor Gray
        docker rm -f $_ 2>$null | Out-Null
    }
}

# Start NameNode.
Write-Host ""
Write-Host "Starting NameNode..." -ForegroundColor Yellow
docker run -d `
    --name hadoop-namenode `
    --hostname hadoop-namenode `
    --network $NetworkName `
    -p 9870:9870 `
    -p 8020:8020 `
    -e HADOOP_HOME=/opt/hadoop `
    -e CLUSTER_NAME=wsh-hdfs `
    $HadoopImage `
    bash -c "/opt/hadoop/bin/hdfs namenode -format -force && /opt/hadoop/bin/hdfs namenode"

Write-Host "[OK] NameNode started (Web UI: http://localhost:9870)" -ForegroundColor Green

# Start DataNodes.
if ($Phase -eq "single") {
    $dataNodeCount = 2
    for ($i = 1; $i -le $dataNodeCount; $i++) {
        $name = "hadoop-datanode$i"
        Write-Host "Starting $name..." -ForegroundColor Yellow
        docker run -d `
            --name $name `
            --hostname $name `
            --network $NetworkName `
            -e HADOOP_HOME=/opt/hadoop `
            $HadoopImage `
            bash -c "/opt/hadoop/bin/hdfs datanode"
        Write-Host "[OK] $name started" -ForegroundColor Green
    }
} else {
    # Multi-machine: read IPs from ip.txt.
    $ipFile = Join-Path $Root "config\ip.txt"
    if (-not (Test-Path $ipFile)) {
        Write-Host "ERROR: $ipFile not found. Create it with remote PC IPs." -ForegroundColor Red
        exit 1
    }
    $ips = Get-Content $ipFile | Where-Object { $_ -and $_ -notmatch "^\s*#" } | ForEach-Object { $_.Trim() }
    if ($ips.Count -eq 0) {
        Write-Host "ERROR: No IPs found in $ipFile" -ForegroundColor Red
        exit 1
    }
    $i = 1
    foreach ($ip in $ips) {
        $name = "hadoop-datanode$i"
        Write-Host "Starting $name on $ip..." -ForegroundColor Yellow
        docker -H "tcp://${ip}:2375" run -d `
            --name $name `
            --hostname $name `
            -e HADOOP_HOME=/opt/hadoop `
            $HadoopImage `
            bash -c "/opt/hadoop/bin/hdfs datanode"
        Write-Host "[OK] $name started on $ip" -ForegroundColor Green
        $i++
    }
}

# Wait for HDFS to stabilize.
Write-Host ""
Write-Host "Waiting 15 seconds for HDFS to stabilize..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# Verify HDFS status.
Write-Host ""
Write-Host "HDFS Status:" -ForegroundColor Green
docker exec hadoop-namenode /opt/hadoop/bin/hdfs dfsadmin -report 2>$null | Select-Object -First 20
Write-Host ""
Write-Host "========================================================" -ForegroundColor Green
Write-Host "  Hadoop HDFS Ready!                                    " -ForegroundColor Green
Write-Host "  NameNode Web UI: http://localhost:9870                " -ForegroundColor Green
Write-Host "========================================================" -ForegroundColor Green
Write-Host ""
