# run_experiments.ps1
# Runs every Task-4 experiment automatically, one after another.
# For EACH run: stop Kafka/Zookeeper -> delete C:\kafka\data -> start Zookeeper + Kafka
#               -> create topics with the right partition count -> run pipeline -> save results.
#
# Usage (from the P2 project folder):
#   powershell -ExecutionPolicy Bypass -File .\run_experiments.ps1                 # all runs
#   powershell -ExecutionPolicy Bypass -File .\run_experiments.ps1 -Only P1,P2     # selected runs
#   powershell -ExecutionPolicy Bypass -File .\run_experiments.ps1 -Partitions 3 -Consumers 2   # any custom combo
#       (optional: -Rate 500 -Key none -RunId MyRun; results\experiment_summary.csv shows the measured counts)
#
# Before starting: close any Kafka / Zookeeper windows you opened manually.

param([string[]]$Only, [int]$Partitions = 0, [int]$Consumers = 0,
      [int]$Rate = 500, [string]$Key = "pickup_zone", [string]$RunId = "")

$JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
$JAVA      = "$JAVA_HOME\bin\java.exe"
$KAFKA     = "C:\kafka"
$LOG4J     = "-Dlog4j.configuration=file:C:/kafka/config/log4j.properties"
$LOGDIR    = "-Dkafka.logs.dir=C:/kafka/logs"

$runs = @(
    @{ id = "P1";        parts = 1; cons = 1; rate = 500;    key = "pickup_zone" },
    @{ id = "P2";        parts = 4; cons = 1; rate = 500;    key = "pickup_zone" },
    @{ id = "P3";        parts = 4; cons = 4; rate = 500;    key = "pickup_zone" },
    @{ id = "P4";        parts = 4; cons = 6; rate = 500;    key = "pickup_zone" },
    @{ id = "K2";        parts = 4; cons = 4; rate = 500;    key = "none" },
    @{ id = "Rate-Low";  parts = 4; cons = 4; rate = 200;    key = "pickup_zone" },
    @{ id = "Rate-High"; parts = 4; cons = 4; rate = 100000; key = "pickup_zone" }
)
if ($Partitions -gt 0 -or $Consumers -gt 0) {
    # Custom run: one experiment with the given partition / consumer counts
    if ($Partitions -le 0) { $Partitions = 4 }
    if ($Consumers -le 0)  { $Consumers = 4 }
    if (-not $RunId) { $RunId = "P${Partitions}C${Consumers}" }
    $runs = @(@{ id = $RunId; parts = $Partitions; cons = $Consumers; rate = $Rate; key = $Key })
    $Only = @($RunId)   # keeps the existing experiment_summary.csv (rows are appended)
}
elseif ($Only) {
    # "-Only P2,P3" may arrive as one string when started with "powershell -File"; split it
    $Only = @($Only | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    $runs = @($runs | Where-Object { $Only -contains $_.id })
}
if ($runs.Count -eq 0) {
    Write-Host "No matching runs for -Only '$($Only -join ',')'. Valid ids: P1,P2,P3,P4,K2,Rate-Low,Rate-High"
    exit 1
}

function Wait-Port([int]$port, [int]$timeoutSec = 90) {
    $t = 0
    while ($t -lt $timeoutSec) {
        try {
            $c = New-Object System.Net.Sockets.TcpClient
            $c.Connect("127.0.0.1", $port)
            $c.Close()
            return $true
        } catch {
            Start-Sleep -Seconds 2
            $t += 2
        }
    }
    return $false
}

function Stop-Kafka {
    # Stops only the Kafka broker and Zookeeper java processes (no wmic needed)
    Get-CimInstance Win32_Process -Filter "Name='java.exe'" |
        Where-Object { $_.CommandLine -match 'kafka\.Kafka|QuorumPeerMain' } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Seconds 5
}

function Start-Kafka {
    if (Test-Path "$KAFKA\data") { Remove-Item -Recurse -Force "$KAFKA\data" }
    New-Item -ItemType Directory -Force "$KAFKA\logs" | Out-Null

    Write-Host "  Starting Zookeeper ..."
    Start-Process -FilePath $JAVA -WindowStyle Minimized -ArgumentList @(
        "-Xmx512M", $LOG4J, $LOGDIR, "-cp", "$KAFKA\libs\*",
        "org.apache.zookeeper.server.quorum.QuorumPeerMain", "$KAFKA\config\zookeeper.properties")
    if (-not (Wait-Port 2181)) { throw "Zookeeper did not start (port 2181). See C:\kafka\logs" }

    Write-Host "  Starting Kafka broker ..."
    Start-Process -FilePath $JAVA -WindowStyle Minimized -ArgumentList @(
        "-Xmx1G", "-Xms1G", $LOG4J, $LOGDIR, "-cp", "$KAFKA\libs\*",
        "kafka.Kafka", "$KAFKA\config\server.properties")
    if (-not (Wait-Port 9092)) { throw "Kafka did not start (port 9092). See C:\kafka\logs\server.log" }
    Start-Sleep -Seconds 8   # let the broker register with Zookeeper
}

# Fresh summary CSV for this batch (old one is kept as a backup)
if (-not $Only -and (Test-Path "results\experiment_summary.csv")) {
    Move-Item -Force "results\experiment_summary.csv" "results\experiment_summary_old.csv"
}

foreach ($r in $runs) {
    $id = $r.id
    Write-Host ""
    Write-Host "==================== RUN $id  (partitions=$($r.parts) consumers=$($r.cons) rate=$($r.rate) key=$($r.key)) ===================="

    Write-Host "  Resetting Kafka (stop -> delete data -> start) ..."
    Stop-Kafka
    Start-Kafka

    Write-Host "  Creating topics with $($r.parts) partition(s) ..."
    $setupOut = cmd /c "python setup/setup_kafka.py --skip-start --partitions $($r.parts) 2>&1"
    $setupOut | ForEach-Object { Write-Host "    $_" }
    if (-not ($setupOut -match "taxi.raw' created with $($r.parts) partition")) {
        throw "taxi.raw was not created with $($r.parts) partition(s) - stopping so results are not wrong."
    }

    Remove-Item "results\results.jsonl" -ErrorAction SilentlyContinue

    Write-Host "  Running pipeline (log -> results\log_$id.txt) ..."
    cmd /c "python run_pipeline.py --run-id $id --rate $($r.rate) --partitions $($r.parts) --consumers $($r.cons) --key $($r.key) 2>&1" |
        Tee-Object -FilePath "results\log_$id.txt"

    if (Test-Path "results\results.jsonl") {
        Copy-Item -Force "results\results.jsonl" "results\results_$id.jsonl"
        Write-Host "  [SAVED] results\results_$id.jsonl"
    } else {
        Write-Host "  [WARN] results.jsonl not found for $id"
    }
}

Write-Host ""
Write-Host "Stopping Kafka and Zookeeper ..."
Stop-Kafka
Write-Host "ALL DONE. Files: results\results_<RUN>.jsonl, results\log_<RUN>.txt, results\experiment_summary.csv"