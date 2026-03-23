$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Sets up a Hadoop HDFS cluster across multiple machines.
.DESCRIPTION
    Creates a NameNode on the main PC and DataNodes on remote PCs.
    NameNode: PC1 (this machine)
    DataNodes: one per remote PC (from config/ip.txt)

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File setup-hadoop.ps1
#>

$Root = Split-Path -Parent $PSScriptRoot
$HadoopImage = "apache/hadoop:3.3.6"
$NetworkName = "wsh-hadoop-net"

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Setting up Hadoop HDFS (multi-machine)                " -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

# Check Docker.
try { docker version | Out-Null } catch {
    Write-Host "ERROR: Docker is not running." -ForegroundColor Red
    exit 1
}

# Read remote IPs.
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

# Detect this PC's IP matching the same subnet as the remote machines (e.g., 172.16.x.x).
$firstIpOctets = $ips[0].Split('.')
$subnetPrefix = "$($firstIpOctets[0]).$($firstIpOctets[1])."

$mainPcIp = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object {
    $_.InterfaceAlias -notmatch "Loopback" -and $_.IPAddress -match "^$([regex]::Escape($subnetPrefix))"

} | Select-Object -First 1).IPAddress

if (-not $mainPcIp) {
    # Fallback: use first non-loopback IPv4.
    $mainPcIp = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object {
        $_.InterfaceAlias -notmatch "Loopback" -and $_.IPAddress -ne "127.0.0.1"
    } | Select-Object -First 1).IPAddress
}
Write-Host "Main PC (NameNode) IP: $mainPcIp" -ForegroundColor White

# Pull Hadoop image on main PC.
Write-Host "Pulling Hadoop image: $HadoopImage ..." -ForegroundColor Yellow
docker pull $HadoopImage

# Create network if it doesn't exist (local only, for NameNode).
$existing = docker network ls --format "{{.Name}}" | Where-Object { $_ -eq $NetworkName }
if (-not $existing) {
    Write-Host "Creating Docker network: $NetworkName" -ForegroundColor Yellow
    docker network create $NetworkName
}

# ── Clean up existing Hadoop containers ────────────────────────────────
Write-Host ""
Write-Host "Cleaning up existing Hadoop containers..." -ForegroundColor Yellow

# Remove NameNode locally.
$c = docker ps -a --filter "name=^hadoop-namenode$" --format "{{.Names}}" 2>$null
if ($c) {
    Write-Host "  Removing local: hadoop-namenode" -ForegroundColor Gray
    docker rm -f hadoop-namenode 2>$null | Out-Null
}

# Remove DataNodes on REMOTE PCs.
$i = 1
foreach ($ip in $ips) {
    $name = "hadoop-datanode$i"
    try {
        $c = docker -H "tcp://${ip}:2375" ps -a --filter "name=^${name}$" --format "{{.Names}}" 2>$null
        if ($c) {
            Write-Host "  Removing remote: $name on $ip" -ForegroundColor Gray
            docker -H "tcp://${ip}:2375" rm -f $name 2>$null | Out-Null
        }
    } catch { }
    $i++
}

# ── Start NameNode ─────────────────────────────────────────────────────
Write-Host ""
Write-Host "Starting NameNode on this PC ($mainPcIp)..." -ForegroundColor Yellow

# Set up port proxy so remote DataNodes can reach the NameNode
Write-Host "Configuring Windows port proxy for NameNode (requires Admin)..." -ForegroundColor Yellow
cmd /c "netsh interface portproxy delete v4tov4 listenaddress=$mainPcIp listenport=8020" 2>$null | Out-Null
cmd /c "netsh interface portproxy delete v4tov4 listenaddress=$mainPcIp listenport=9870" 2>$null | Out-Null
cmd /c "netsh interface portproxy add v4tov4 listenaddress=$mainPcIp listenport=8020 connectaddress=127.0.0.1 connectport=8020"
cmd /c "netsh interface portproxy add v4tov4 listenaddress=$mainPcIp listenport=9870 connectaddress=127.0.0.1 connectport=9870"

# Build Hadoop config inline (core-site.xml + hdfs-site.xml).
$configScript = @"
mkdir -p /opt/hadoop/etc/hadoop;
cat > /opt/hadoop/etc/hadoop/core-site.xml << 'COREEOF'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${mainPcIp}:8020</value>
  </property>
</configuration>
COREEOF
cat > /opt/hadoop/etc/hadoop/hdfs-site.xml << 'HDFSEOF'
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/tmp/hadoop-namenode</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>true</value>
  </property>
</configuration>
HDFSEOF
/opt/hadoop/bin/hdfs namenode -format -force 2>/dev/null;
/opt/hadoop/bin/hdfs namenode
"@

docker run -d `
    --name hadoop-namenode `
    --hostname hadoop-namenode `
    --network $NetworkName `
    -p 9870:9870 `
    -p 8020:8020 `
    -e HADOOP_HOME=/opt/hadoop `
    -e CLUSTER_NAME=wsh-hdfs `
    $HadoopImage `
    bash -c $configScript

# Give NameNode time to format and start.
Write-Host "Waiting 20 seconds for NameNode to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 20

# Check if NameNode is running.
$nnStatus = docker ps --filter "name=hadoop-namenode" --format "{{.Status}}"
if (-not $nnStatus) {
    Write-Host "ERROR: NameNode failed to start. Check logs:" -ForegroundColor Red
    Write-Host "  docker logs hadoop-namenode" -ForegroundColor Yellow
    docker logs hadoop-namenode 2>&1 | Select-Object -Last 20
    exit 1
}
Write-Host "[OK] NameNode started (Web UI: http://localhost:9870)" -ForegroundColor Green

# ── Start DataNodes on remote PCs ──────────────────────────────────────

$i = 1
foreach ($ip in $ips) {
    $name = "hadoop-datanode$i"
    Write-Host "Starting $name on $ip..." -ForegroundColor Yellow

$datanodeScript = @"
mkdir -p /opt/hadoop/etc/hadoop;
cat > /opt/hadoop/etc/hadoop/core-site.xml << 'COREEOF'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${mainPcIp}:8020</value>
  </property>
</configuration>
COREEOF
cat > /opt/hadoop/etc/hadoop/hdfs-site.xml << 'HDFSEOF'
<configuration>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/tmp/hadoop-datanode</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.datanode.use.datanode.hostname</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.datanode.hostname</name>
    <value>${ip}</value>
  </property>
</configuration>
HDFSEOF
/opt/hadoop/bin/hdfs datanode
"@

    # Pull image on remote if needed.
    try {
        docker -H "tcp://${ip}:2375" pull $HadoopImage 2>$null | Out-Null
    } catch { }

    docker -H "tcp://${ip}:2375" run -d `
        --name $name `
        --hostname $name `
        -p 9866:9866 `
        -e HADOOP_HOME=/opt/hadoop `
        $HadoopImage `
        bash -c $datanodeScript

    Write-Host "[OK] $name started on $ip (connects to NameNode at ${mainPcIp}:8020)" -ForegroundColor Green
    $i++
}

# Wait for DataNodes to register.
Write-Host ""
Write-Host "Waiting 20 seconds for DataNodes to register with NameNode..." -ForegroundColor Yellow
Start-Sleep -Seconds 20

# Verify HDFS status.
Write-Host ""
Write-Host "HDFS Status:" -ForegroundColor Green
docker exec hadoop-namenode /opt/hadoop/bin/hdfs dfsadmin -report 2>$null | Select-Object -First 25
Write-Host ""
Write-Host "========================================================" -ForegroundColor Green
Write-Host "  Hadoop HDFS Ready!                                    " -ForegroundColor Green
Write-Host "  NameNode: $mainPcIp:8020                              " -ForegroundColor Green
Write-Host "  Web UI:   http://localhost:9870                       " -ForegroundColor Green
Write-Host "  DataNodes: $($ips.Count) (on remote PCs)               " -ForegroundColor Green
Write-Host "========================================================" -ForegroundColor Green
Write-Host ""
