# enable-docker-remote.ps1
param(
    [string]$ListenIP = "",
    [int]$Port = 2375,
    [string]$RuleName = "Docker Remote API 2375"
)

$ErrorActionPreference = "Stop"

function Assert-Admin {
    $currentUser = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
    if (-not $currentUser.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Run this script in an Administrator PowerShell window."
    }
}

function Get-PrimaryIPv4 {
    $ip = Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object {
            $_.IPAddress -notmatch '^127\.' -and
            $_.IPAddress -notmatch '^169\.254\.' -and
            $_.PrefixOrigin -ne 'WellKnown'
        } |
        Sort-Object InterfaceMetric, SkipAsSource |
        Select-Object -ExpandProperty IPAddress -First 1

    if (-not $ip) {
        throw "Could not detect a usable IPv4 address. Pass -ListenIP manually."
    }

    return $ip
}

Assert-Admin

if (-not $ListenIP) {
    $ListenIP = Get-PrimaryIPv4
}

Write-Host "Using ListenIP=$ListenIP Port=$Port"

# Optional check: Docker Desktop localhost listener
$dockerListening = netstat -ano | findstr ":$Port"
if (-not $dockerListening) {
    Write-Warning "No local listener detected on port $Port. Make sure Docker Desktop has 'Expose daemon on tcp://localhost:2375 without TLS' enabled."
}

# Remove old rule for same listen address/port if it exists
cmd /c "netsh interface portproxy delete v4tov4 listenaddress=$ListenIP listenport=$Port" | Out-Null

# Add port proxy: LAN_IP:2375 -> 127.0.0.1:2375
cmd /c "netsh interface portproxy add v4tov4 listenaddress=$ListenIP listenport=$Port connectaddress=127.0.0.1 connectport=$Port"

# Recreate firewall rule cleanly
cmd /c "netsh advfirewall firewall delete rule name=""$RuleName""" | Out-Null
cmd /c "netsh advfirewall firewall add rule name=""$RuleName"" dir=in action=allow protocol=TCP localport=$Port"

Write-Host ""
Write-Host "Portproxy status:"
cmd /c "netsh interface portproxy show all"

Write-Host ""
Write-Host "Firewall rule status:"
cmd /c "netsh advfirewall firewall show rule name=""$RuleName"""

Write-Host ""
Write-Host "Test from another machine with:"
Write-Host "  curl http://$ListenIP:$Port/version"
Write-Host "  docker -H tcp://$ListenIP:$Port version"
