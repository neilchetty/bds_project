$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Stops and removes all WSH worker containers on this machine.
.DESCRIPTION
    Stops all containers matching "worker-c*" pattern. Works for both
    single-machine and multi-machine modes.
#>

Write-Host ""
Write-Host "Stopping all WSH worker containers..." -ForegroundColor Yellow

$containers = docker ps -a --filter "name=worker-c" --format "{{.Names}}" 2>$null
if ($containers) {
    $containers | ForEach-Object {
        Write-Host "  Stopping: $_" -ForegroundColor Gray
        docker rm -f $_ 2>$null | Out-Null
    }
    Write-Host ""
    Write-Host "All containers stopped and removed." -ForegroundColor Green
} else {
    Write-Host "No WSH worker containers found." -ForegroundColor Yellow
}
Write-Host ""
