# Create a local, ignored configuration file for a fresh installation.
$ErrorActionPreference = 'Stop'
$configRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$configPath = Join-Path $configRoot '.env'
if (Test-Path -LiteralPath $configPath) {
    Write-Host 'Existing .env preserved. No values changed.'
    return
}
function New-LocalValue {
    $bytes = New-Object byte[] 24
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    return ([BitConverter]::ToString($bytes)).Replace('-', '').ToLowerInvariant()
}
$configText = "AIRFLOW_DB_PASSWORD=$(New-LocalValue)`nAIRFLOW_ADMIN_PASSWORD=$(New-LocalValue)`nAIRFLOW_SECRET_KEY=$(New-LocalValue)`n"
[IO.File]::WriteAllText($configPath, $configText)
Write-Host 'Created local .env. Read AIRFLOW_ADMIN_PASSWORD there to log in as airflow. Do not share or commit this file.'
