param(
    [Parameter(Mandatory=$false)]
    [ValidateRange(1, 100)]
    [int]$SizeGB = 30
)

$ErrorActionPreference = "Stop"

<#
.SYNOPSIS
    Generates synthetic bioinformatics data for the Gene2life big data benchmark.
.DESCRIPTION
    Creates synthetic FASTA-format genomic sequence files that match the Gene2life
    workflow structure. The generated data is split into chunks corresponding to
    each task in the workflow:
      - blast_input1.dat, blast_input2.dat    (sequence databases)
      - clustalw_ref.dat                       (reference alignment)
      - dnapars_matrix.dat, protpars_matrix.dat (distance matrices)

    Total data size is configurable (default: 30GB).

.PARAMETER SizeGB
    Target data size in gigabytes (30-100, default 30).
.PARAMETER Phase
    Deployment phase: 'single' or 'multi'.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File generate-bigdata.ps1 -SizeGB 30
    powershell -ExecutionPolicy Bypass -File generate-bigdata.ps1 -SizeGB 60 -Phase multi
#>

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "bigdata"

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Generating $SizeGB GB Synthetic Bioinformatics Data   " -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

# Create data directory.
if (-not (Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
}

# Calculate sizes for each data file (proportional to Gene2life workflow).
# blast inputs get ~40% each, clustalw gets ~15%, parsimony tools get ~2.5% each.
$totalBytes = [long]$SizeGB * 1024 * 1024 * 1024
$files = @(
    @{ Name = "blast_input1.dat"; Fraction = 0.40 },
    @{ Name = "blast_input2.dat"; Fraction = 0.40 },
    @{ Name = "clustalw_ref.dat"; Fraction = 0.15 },
    @{ Name = "dnapars_matrix.dat"; Fraction = 0.025 },
    @{ Name = "protpars_matrix.dat"; Fraction = 0.025 }
)

foreach ($file in $files) {
    $filePath = Join-Path $DataDir $file.Name
    $fileSize = [long]($totalBytes * $file.Fraction)
    $fileSizeMB = [math]::Ceiling($fileSize / (1024 * 1024))

    if (Test-Path $filePath) {
        $existingSize = (Get-Item $filePath).Length
        if ($existingSize -ge ($fileSize * 0.95)) {
            Write-Host "  [SKIP] $($file.Name) already exists ($([math]::Round($existingSize / 1GB, 2)) GB)" -ForegroundColor Gray
            continue
        }
    }

    Write-Host "  [GEN]  $($file.Name) ($fileSizeMB MB) ..." -ForegroundColor Yellow

    # Generate synthetic FASTA data using PowerShell.
    # Each line is a FASTA record: >seq_XXXX followed by a 80-char sequence line.
    $chunkSizeMB = 256
    $remainingMB = $fileSizeMB
    $first = $true

    while ($remainingMB -gt 0) {
        $currentChunk = [math]::Min($chunkSizeMB, $remainingMB)
        $remainingMB -= $currentChunk

        # Use docker to generate random data efficiently (faster than PowerShell).
        $container = "worker-c1-1"
        $tmpFile = "/tmp/wsh-bigdata-gen.bin"

        if ($first) {
            # Write FASTA header first.
            docker exec $container bash -c "echo '>synthetic_genome_sequence_database' > $tmpFile; dd if=/dev/urandom bs=1M count=$currentChunk status=none | base64 >> $tmpFile" 2>$null
            docker cp "${container}:${tmpFile}" $filePath 2>$null
            $first = $false
        } else {
            docker exec $container bash -c "dd if=/dev/urandom bs=1M count=$currentChunk status=none | base64 > $tmpFile" 2>$null
            # Append to existing file via docker cp to temp then append.
            $tempLocal = Join-Path $DataDir "temp_chunk.bin"
            docker cp "${container}:${tmpFile}" $tempLocal 2>$null
            Get-Content $tempLocal -Raw | Add-Content $filePath
            Remove-Item $tempLocal -ErrorAction SilentlyContinue
        }

        docker exec $container rm -f $tmpFile 2>$null
    }

    $finalSize = (Get-Item $filePath).Length
    Write-Host "  [OK]   $($file.Name) = $([math]::Round($finalSize / 1GB, 2)) GB" -ForegroundColor Green
}

Write-Host ""
Write-Host "========================================================" -ForegroundColor Green
Write-Host "  Data Generation Complete!                              " -ForegroundColor Green
Write-Host "========================================================" -ForegroundColor Green

$totalSize = 0
Get-ChildItem $DataDir -File | ForEach-Object {
    $totalSize += $_.Length
    Write-Host "  $($_.Name): $([math]::Round($_.Length / 1GB, 2)) GB" -ForegroundColor White
}
Write-Host ""
Write-Host "  Total: $([math]::Round($totalSize / 1GB, 2)) GB" -ForegroundColor Cyan
Write-Host "  Location: $DataDir" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next step: Upload to HDFS using upload-to-hdfs.ps1" -ForegroundColor Yellow
Write-Host ""
