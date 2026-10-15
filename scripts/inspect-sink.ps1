param(
    [ValidateSet("auto","kafka","postgres")]
    [string]$SinkType = "auto",

    [int]$Tail = 20,

    [string]$Namespace = "vm-watcher"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Step { param([string]$Msg) Write-Host "`n==> $Msg" -ForegroundColor Cyan }
function Ok   { param([string]$Msg) Write-Host "    $Msg" -ForegroundColor Green }
function Fail { param([string]$Msg) Write-Host "    ERROR: $Msg" -ForegroundColor Red; exit 1 }

if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Fail "kubectl not found on PATH"
}

if ($SinkType -eq "auto") {
    Step "Detecting sink type from deployment/vm-watcher"
    $SinkType = kubectl get deploy vm-watcher -n $Namespace -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name=='SINK_TYPE')].value}"
    if ([string]::IsNullOrWhiteSpace($SinkType)) {
        $SinkType = "kafka"
    }
    Ok "Detected sink: $SinkType"
}

switch ($SinkType) {
    "postgres" {
        Step "Reading the latest VM states from PostgreSQL (vm_state)"
        $pod = kubectl get pod -n $Namespace -l app=postgres -o jsonpath="{.items[0].metadata.name}"
        if ([string]::IsNullOrWhiteSpace($pod)) {
            Fail "PostgreSQL pod not found"
        }
        kubectl exec -n $Namespace $pod -- psql -U vmwatcher -d vmwatcher -c "select event_key, last_event_type as type, last_status as status, last_resource_version as resource_version, last_seen_at from vm_state order by last_seen_at desc limit $Tail;"
        break
    }
    "kafka" {
        Step "Reading VM events from Kafka topic vm-events"
        $pod = kubectl get pod -n $Namespace -l strimzi.io/name=vm-audit-cluster-kafka -o jsonpath="{.items[0].metadata.name}"
        if ([string]::IsNullOrWhiteSpace($pod)) {
            Fail "Kafka broker pod not found"
        }
        kubectl exec -n $Namespace $pod -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vm-events --from-beginning --max-messages $Tail --timeout-ms 15000 --property print.key=true
        break
    }
    default {
        Fail "Unsupported sink type: $SinkType"
    }
}
