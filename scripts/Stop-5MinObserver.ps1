$ErrorActionPreference = 'Stop'
$demoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$demoBase = [System.IO.Path]::GetFullPath((Join-Path $demoRoot 'outputs/demo_5min'))
$demoLatest = Get-Content -LiteralPath (Join-Path $demoBase 'latest.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$demoStopPath = [System.IO.Path]::GetFullPath($demoLatest.stop_file)
if (-not $demoStopPath.StartsWith($demoBase + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -or [IO.Path]::GetFileName($demoStopPath) -ne 'STOP') {
    throw 'Unexpected observer stop-file path.'
}
New-Item -ItemType File -Path $demoStopPath -Force | Out-Null
Write-Host 'Stop requested for the observation application. HDFS, Kafka, Airflow and the dashboard remain available.'
