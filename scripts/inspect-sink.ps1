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
        Step "Reading latest VM events from PostgreSQL"
        $pod = kubectl get pod -n $Namespace -l app=postgres -o jsonpath="{.items[0].metadata.name}"
        if ([string]::IsNullOrWhiteSpace($pod)) {
            Fail "PostgreSQL pod not found"
        }
        kubectl exec -n $Namespace $pod -- psql -U vmwatcher -d vmwatcher -c "select id, event_key, payload->>'type' as type, payload->>'timestamp' as timestamp from vm_events order by id desc limit $Tail;"
        break
    }
    "kafka" {
        Step "Reading latest VM events from Kafka"
        $pod = kubectl get pod -n $Namespace -l app=kafka -o jsonpath="{.items[0].metadata.name}"
        if ([string]::IsNullOrWhiteSpace($pod)) {
            Fail "Kafka pod not found"
        }
        kubectl exec -n $Namespace $pod -- bash -lc "timeout 15 kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic vm-events --from-beginning --max-messages $Tail"
        break
    }
    default {
        Fail "Unsupported sink type: $SinkType"
    }
}
