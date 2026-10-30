# start_kafka_local.ps1
$JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
$JAVA      = "$JAVA_HOME\bin\java.exe"
$KAFKA     = "C:\kafka"
$LOG4J     = "-Dlog4j.configuration=file:C:/kafka/config/log4j.properties"
$LOGDIR    = "-Dkafka.logs.dir=C:/kafka/logs"

if (Test-Path "$KAFKA\data") {
    Remove-Item -Recurse -Force "$KAFKA\data"
}
New-Item -ItemType Directory -Force "$KAFKA\logs" | Out-Null

Write-Host "Starting Zookeeper..."
Start-Process -FilePath $JAVA -WindowStyle Minimized -ArgumentList @(
    "-Xmx512M", $LOG4J, $LOGDIR, "-cp", "$KAFKA\libs\*",
    "org.apache.zookeeper.server.quorum.QuorumPeerMain", "$KAFKA\config\zookeeper.properties")

$t = 0
$zkOk = $false
while ($t -lt 30) {
    try {
        $c = New-Object System.Net.Sockets.TcpClient
        $c.Connect("127.0.0.1", 2181)
        $c.Close()
        $zkOk = $true
        break
    } catch {
        Start-Sleep -Seconds 2
        $t += 2
    }
}
Write-Host "Zookeeper listening on 2181: $zkOk"

if ($zkOk) {
    Write-Host "Starting Kafka broker..."
    Start-Process -FilePath $JAVA -WindowStyle Minimized -ArgumentList @(
        "-Xmx1G", "-Xms1G", $LOG4J, $LOGDIR, "-cp", "$KAFKA\libs\*",
        "kafka.Kafka", "$KAFKA\config\server.properties")

    $t = 0
    $kOk = $false
    while ($t -lt 45) {
        try {
            $c = New-Object System.Net.Sockets.TcpClient
            $c.Connect("127.0.0.1", 9092)
            $c.Close()
            $kOk = $true
            break
        } catch {
            Start-Sleep -Seconds 2
            $t += 2
        }
    }
    Write-Host "Kafka broker listening on 9092: $kOk"
}
