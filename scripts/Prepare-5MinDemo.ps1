param(
    [switch]$OpenOnly,
    [switch]$CheckOnly,
    [switch]$RunPipeline,
    [int]$HoldSeconds = 7200
)
$ErrorActionPreference = 'Stop'
$demoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
Set-Location -LiteralPath $demoRoot

function Open-DemoSource {
    $demoCode = Get-Command code -ErrorAction SilentlyContinue
    if ($demoCode) {
        $demoEditorArgs = @('--new-window', (Join-Path $demoRoot 'HuynhQuangSang.code-workspace'), '--goto',
            (Join-Path $demoRoot 'scripts/bootstrap_hdfs.py:16'),
            (Join-Path $demoRoot 'src/review_analytics/spark_ops.py:21'),
            (Join-Path $demoRoot 'jobs/batch_pipeline.py:45'),
            (Join-Path $demoRoot 'jobs/stream_reviews.py:31'),
            (Join-Path $demoRoot 'dags/ecommerce_pipeline.py:19'),
            (Join-Path $demoRoot 'outputs/data_quality.json'),
            (Join-Path $demoRoot 'outputs/cluster_metadata.json'),
            (Join-Path $demoRoot 'outputs/persisted_validation.json'),
            (Join-Path $demoRoot 'docs/DEMO_5_PHUT.md'))
        & $demoCode.Source @demoEditorArgs
    } else { Write-Warning 'VS Code CLI was not found. Open the files listed in docs/DEMO_5_PHUT.md.' }
}
if (-not $CheckOnly) { Open-DemoSource }
if ($OpenOnly) { Write-Host 'Opened source and existing results only; no runtime jobs were executed.'; return }

$demoDocker = (Get-Command docker -ErrorAction Stop).Source
$demoTag = 'demo5_' + [DateTime]::UtcNow.ToString('yyyyMMdd_HHmmss')
$demoSession = Join-Path $demoRoot ('outputs/demo_5min/' + $demoTag)
New-Item -ItemType Directory -Path $demoSession -Force | Out-Null
# Windows PowerShell 5.1 may return a null ExitCode for Start-Process -PassThru.
# Own the Process handle and drain both streams asynchronously instead.
$demoEngineStart = New-Object System.Diagnostics.ProcessStartInfo
$demoEngineStart.FileName = $demoDocker
$demoEngineStart.Arguments = 'info --format "{{.ServerVersion}}"'
$demoEngineStart.UseShellExecute = $false
$demoEngineStart.CreateNoWindow = $true
$demoEngineStart.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Hidden
$demoEngineStart.RedirectStandardOutput = $true
$demoEngineStart.RedirectStandardError = $true
$demoEngineProbe = New-Object System.Diagnostics.Process
$demoEngineProbe.StartInfo = $demoEngineStart
try {
    if (-not $demoEngineProbe.Start()) { throw 'Could not start the Docker Engine probe.' }
    $demoEngineOutTask = $demoEngineProbe.StandardOutput.ReadToEndAsync()
    $demoEngineErrTask = $demoEngineProbe.StandardError.ReadToEndAsync()
    if (-not $demoEngineProbe.WaitForExit(10000)) {
        try { $demoEngineProbe.Kill() } catch { }
        [void]$demoEngineProbe.WaitForExit(2000)
        throw 'Docker Engine did not answer within 10 seconds. Fix docker info before preparing runtime.'
    }
    $demoEngineProbe.WaitForExit()
    $demoEngineOut = $demoEngineOutTask.GetAwaiter().GetResult()
    $demoEngineErr = $demoEngineErrTask.GetAwaiter().GetResult()
    $demoEngineExitCode = $demoEngineProbe.ExitCode
    [IO.File]::WriteAllText((Join-Path $demoSession 'engine.out'), $demoEngineOut)
    [IO.File]::WriteAllText((Join-Path $demoSession 'engine.err'), $demoEngineErr)
    [ordered]@{ powershell_version=$PSVersionTable.PSVersion.ToString(); exit_code=$demoEngineExitCode;
        server_version=$demoEngineOut.Trim(); checked_utc=[DateTime]::UtcNow.ToString('o') } |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $demoSession 'engine_probe.json') -Encoding UTF8
    if ($demoEngineExitCode -ne 0) {
        throw ('Docker Engine probe failed with exit code ' + $demoEngineExitCode + '. ' + $demoEngineErr.Trim())
    }
    if ([string]::IsNullOrWhiteSpace($demoEngineOut)) { throw 'Docker did not return a Server version.' }
} finally { $demoEngineProbe.Dispose() }
if ($CheckOnly) { Write-Host ('Docker Engine ready: ' + $demoEngineOut.Trim()); return }

function Invoke-DemoDocker {
    & $script:demoDocker @args
    if ($LASTEXITCODE -ne 0) { throw ('Docker command failed: ' + ($args -join ' ')) }
}
function Wait-DemoHttp {
    param([string]$Uri, [int]$Seconds = 120)
    $demoUntil = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        try { return Invoke-RestMethod -Uri $Uri -TimeoutSec 4 } catch { Start-Sleep -Seconds 2 }
    } while ([DateTime]::UtcNow -lt $demoUntil)
    throw ('Service did not become ready: ' + $Uri)
}

# Docker may publish 4040 even when no driver is listening inside spark-client.
# Check the Spark API rather than treating the published TCP port as an active driver.
$demoExistingApps = $null
try { $demoExistingApps = Invoke-RestMethod -Uri 'http://localhost:4040/api/v1/applications' -TimeoutSec 3 } catch { }
if ($demoExistingApps -and @($demoExistingApps).Count -gt 0) {
    throw 'Port 4040 is already in use. Stop the previous Spark demo/application before preparing a new one.'
}
Invoke-DemoDocker compose up -d namenode datanode spark-master spark-worker-1 spark-worker-2 spark-client
Wait-DemoHttp -Uri 'http://localhost:9870/jmx' | Out-Null
$demoCluster = Wait-DemoHttp -Uri 'http://localhost:8081/json/'
$demoWorkersUntil = [DateTime]::UtcNow.AddSeconds(90)
while ($demoCluster.aliveworkers -lt 2 -and [DateTime]::UtcNow -lt $demoWorkersUntil) {
    Start-Sleep -Seconds 2
    $demoCluster = Invoke-RestMethod -Uri 'http://localhost:8081/json/' -TimeoutSec 5
}
if ($demoCluster.aliveworkers -lt 2) { throw 'Spark does not have two live workers yet.' }
Invoke-DemoDocker compose exec -T spark-client hdfs dfsadmin -safemode wait
Invoke-DemoDocker compose exec -T spark-client hdfs dfsadmin -report |
    Out-File -LiteralPath (Join-Path $demoSession '01_hdfs_report.txt') -Encoding utf8
Invoke-DemoDocker compose exec -T spark-client python scripts/bootstrap_hdfs.py
Invoke-DemoDocker compose exec -T spark-client hdfs fsck /ecommerce/raw/reviews.csv -files -blocks -locations |
    Out-File -LiteralPath (Join-Path $demoSession '01_hdfs_blocks.txt') -Encoding utf8

Invoke-DemoDocker compose --profile orchestration up -d postgres kafka
Invoke-DemoDocker compose --profile orchestration run --rm airflow-init
Invoke-DemoDocker compose --profile orchestration up -d airflow-webserver airflow-scheduler
Invoke-DemoDocker compose exec -T airflow-scheduler airflow dags list-import-errors

$demoRunId = $null
if ($RunPipeline) {
    $demoQuality = Join-Path $demoRoot 'outputs/data_quality.json'
    if (Test-Path -LiteralPath $demoQuality) {
        Copy-Item -LiteralPath $demoQuality -Destination (Join-Path $demoRoot 'outputs/quality_before_airflow.json')
    }
    Invoke-DemoDocker compose exec -T airflow-scheduler airflow dags unpause ecommerce_dissatisfaction_pipeline
    Invoke-DemoDocker compose exec -T airflow-scheduler airflow dags trigger -r $demoTag ecommerce_dissatisfaction_pipeline
    $demoRunId = $demoTag
    $demoDagUntil = [DateTime]::UtcNow.AddMinutes(60)
    do {
        Start-Sleep -Seconds 10
        $demoRunsText = (Invoke-DemoDocker compose exec -T airflow-scheduler airflow dags list-runs -d ecommerce_dissatisfaction_pipeline -o json) -join "`n"
        $demoRunsMatch = [regex]::Match($demoRunsText, '(?ms)^\[\s*\{.*\]\s*$')
        if (-not $demoRunsMatch.Success) { throw 'Cannot parse Airflow run list; inspect the Airflow UI.' }
        $demoRuns = $demoRunsMatch.Value | ConvertFrom-Json
        $demoRun = $demoRuns | Where-Object { $_.run_id -eq $demoRunId } | Select-Object -First 1
        if ($demoRun -and $demoRun.state -eq 'failed') { throw 'The preparation DAG failed. Inspect its task log.' }
    } while ((-not $demoRun -or $demoRun.state -ne 'success') -and [DateTime]::UtcNow -lt $demoDagUntil)
    if (-not $demoRun -or $demoRun.state -ne 'success') { throw 'DAG preparation has not completed within the setup time limit.' }
    Invoke-DemoDocker compose exec -T airflow-scheduler airflow tasks states-for-dag-run ecommerce_dissatisfaction_pipeline $demoRunId |
        Out-File -LiteralPath (Join-Path $demoSession '07_airflow_tasks.txt') -Encoding utf8
} else {
    Invoke-DemoDocker compose exec -T spark-client hdfs dfs -test -e /ecommerce/processed/reviews
    Invoke-DemoDocker compose exec -T spark-client python jobs/replay_kafka.py |
        Out-File -LiteralPath (Join-Path $demoSession '06_producer.txt') -Encoding utf8
    Invoke-DemoDocker compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 jobs/stream_reviews.py
    Invoke-DemoDocker compose exec -T spark-client spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 1g jobs/validate_persisted.py
}
Invoke-DemoDocker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic ecommerce-reviews --time latest |
    Out-File -LiteralPath (Join-Path $demoSession '06_kafka_offsets.txt') -Encoding utf8
Invoke-DemoDocker compose --profile dashboard up -d dashboard
Wait-DemoHttp -Uri 'http://localhost:8090/api/health' | Out-Null
Wait-DemoHttp -Uri 'http://localhost:8080/health' | Out-Null

$demoObserverArgs = @('compose','exec','-T','spark-client','spark-submit','--master','spark://spark-master:7077',
    '--driver-memory','2g','--executor-memory','1g','jobs/demo_observe.py',
    '--exports',('/exports/demo_5min/' + $demoTag),'--hold-seconds',([string]$HoldSeconds))
$demoObserver = Start-Process -FilePath $demoDocker -ArgumentList $demoObserverArgs -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $demoSession 'observer.out') -RedirectStandardError (Join-Path $demoSession 'observer.err')
$demoReadyPath = Join-Path $demoSession 'READY.json'
$demoObserveUntil = [DateTime]::UtcNow.AddMinutes(12)
while (-not (Test-Path -LiteralPath $demoReadyPath)) {
    if ($demoObserver.HasExited) { throw 'Observer exited before READY. Read observer.err in the session directory.' }
    if ([DateTime]::UtcNow -ge $demoObserveUntil) { throw 'Observer is still preparing. Check its log before presenting.' }
    Start-Sleep -Seconds 2
}
$demoReady = Get-Content -LiteralPath $demoReadyPath -Raw -Encoding UTF8 | ConvertFrom-Json
$demoManifest = [ordered]@{ state='ready'; session_dir=$demoSession; tag=$demoTag;
    prepared_utc=[DateTime]::UtcNow.ToString('o'); application_id=$demoReady.application_id;
    airflow_run_id=$demoRunId; full_dag_rerun=[bool]$RunPipeline; observer_pid=$demoObserver.Id;
    stop_file=(Join-Path $demoSession 'STOP') }
$demoManifest | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $demoRoot 'outputs/demo_5min/latest.json') -Encoding UTF8
Write-Host ('DEMO READY: ' + $demoSession)
Write-Host ('Spark application: ' + $demoReady.application_id)
Write-Host 'Keep services running. Do not trigger another DAG while the observation application holds Spark resources.'
Write-Host 'Open localhost:9870, localhost:4040/SQL/, localhost:8080 and localhost:8090.'
Write-Host 'To release Spark resources, create the STOP file listed in outputs/demo_5min/latest.json.'
if (Get-Command code -ErrorAction SilentlyContinue) { & code --reuse-window $demoReadyPath (Join-Path $demoSession '03_quality.json') (Join-Path $demoSession '04_sql.json') (Join-Path $demoSession '06_stream.json') }
