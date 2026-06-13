<#
.SYNOPSIS
    Bootstrap a local vm-watcher dev cluster with kind.

.DESCRIPTION
    1. Creates (or reuses) a kind cluster from kind-config.yaml
    2. Installs KubeVirt using the kind quickstart flow
    3. Builds the vm-watcher Docker image
    4. Loads the image into the kind cluster (no registry needed)
    5. Patches the image reference in the deployment manifest
    6. Applies all manifests in deployment/ via kustomize
    7. Optionally creates an example VirtualMachine in a watched namespace

.PARAMETER SinkType
    Which sink to activate. This setup is Postgres-only by default.

.PARAMETER KubevirtVersion
    KubeVirt version tag to use. Defaults to the stable quickstart version.

.PARAMETER CreateExampleVM
    Apply deployment/05-example-vm.yaml after the watcher is ready.

.EXAMPLE
    .\scripts\setup.ps1
    .\scripts\setup.ps1 -SinkType postgres
#>
param(
    [ValidateSet("postgres")]
    [string]$SinkType = "postgres",

    [string]$KubevirtVersion = "",

    [bool]$CreateExampleVM = $true,

    [string]$ImageTag = "vm-watcher:dev"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot

function Step { param([string]$Msg) Write-Host "`n==> $Msg" -ForegroundColor Cyan }
function Ok   { param([string]$Msg) Write-Host "    $Msg" -ForegroundColor Green }
function Fail { param([string]$Msg) Write-Host "    ERROR: $Msg" -ForegroundColor Red; exit 1 }

function Wait-ForKubeVirtPhase {
    param(
        [string]$ExpectedPhase = "Deployed",
        [int]$TimeoutSeconds = 300
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $phase = kubectl get kubevirt.kubevirt.io/kubevirt -n kubevirt kubevirt -o jsonpath="{.status.phase}" 2>$null
        if ($phase -eq $ExpectedPhase) {
            return
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)

    Fail "KubeVirt did not reach phase '$ExpectedPhase' within ${TimeoutSeconds}s"
}

function Is-AllNamespaceWatch {
    param([string]$Value)
    $v = "$Value".Trim().ToLowerInvariant()
    return ($v -eq "*" -or $v -eq "all")
}

# ── 0. Prerequisite check ────────────────────────────────────────────────────
Step "Checking prerequisites"
foreach ($tool in "kind","kubectl","docker","kustomize") {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        Fail "$tool not found on PATH. Please install it first."
    }
    Ok "$tool found"
}

if ([string]::IsNullOrWhiteSpace($KubevirtVersion)) {
    Step "Resolving stable KubeVirt version from quickstart channel"
    $KubevirtVersion = (Invoke-RestMethod -Uri "https://storage.googleapis.com/kubevirt-prow/release/kubevirt/kubevirt/stable.txt").Trim()
    Ok "Using KubeVirt $KubevirtVersion"
}

# ── 1. Create kind cluster ───────────────────────────────────────────────────
Step "Creating kind cluster 'vm-watcher-dev'"
$existing = kind get clusters 2>&1 | Select-String "vm-watcher-dev"
if ($existing) {
    Ok "Cluster already exists — skipping create"
} else {
    kind create cluster --config "$Root\kind-config.yaml"
    Ok "Cluster created"
}

# ── 2. Install KubeVirt using the quickstart-kind flow ──────────────────────
Step "Installing KubeVirt operator ($KubevirtVersion)"
$operatorUrl = "https://github.com/kubevirt/kubevirt/releases/download/$KubevirtVersion/kubevirt-operator.yaml"
kubectl apply -f $operatorUrl
Ok "KubeVirt operator applied"

Step "Installing KubeVirt custom resource"
$crUrl = "https://github.com/kubevirt/kubevirt/releases/download/$KubevirtVersion/kubevirt-cr.yaml"
kubectl apply -f $crUrl
Ok "KubeVirt CR applied"

Step "Enabling emulation for kind"
kubectl -n kubevirt patch kubevirt kubevirt --type=merge --patch '{"spec":{"configuration":{"developerConfiguration":{"useEmulation":true}}}}'
Ok "KubeVirt emulation enabled"

Step "Waiting for KubeVirt to reach phase Deployed"
Wait-ForKubeVirtPhase -ExpectedPhase "Deployed" -TimeoutSeconds 600
Ok "KubeVirt is deployed"

Step "Verifying KubeVirt components"
kubectl get all -n kubevirt

# ── 3. Build Docker image ────────────────────────────────────────────────────
Step "Building Docker image $ImageTag"
Push-Location $Root
docker build -t $ImageTag .
Pop-Location
Ok "Image built"

# ── 4. Load image into kind ──────────────────────────────────────────────────
Step "Loading $ImageTag into kind cluster"
kind load docker-image $ImageTag --name vm-watcher-dev
Ok "Image loaded"

# ── 5. Patch image in deployment manifest ───────────────────────────────────
Step "Patching image reference in deployment/04-vm-watcher.yaml"
$deployFile = "$Root\deployment\04-vm-watcher.yaml"
(Get-Content $deployFile) -replace 'registry\.example\.com/vm-watcher:latest', $ImageTag |
    Set-Content $deployFile
Ok "Image patched to $ImageTag"

# ── 6. Patch SINK_TYPE ───────────────────────────────────────────────────────
Step "Setting SINK_TYPE to '$SinkType'"
(Get-Content $deployFile) -replace '(value: )"postgres"', "`$1`"$SinkType`"" |
    Set-Content $deployFile
Ok "SINK_TYPE set"

# ── 7. Install nginx ingress controller ─────────────────────────────────────
Step "Installing nginx ingress controller (kind provider)"
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
Ok "nginx ingress applied"

Step "Waiting for nginx ingress controller to be ready"
kubectl wait --namespace ingress-nginx `
    --for=condition=ready pod `
    --selector=app.kubernetes.io/component=controller `
    --timeout=120s
Ok "nginx ingress controller is ready"

# ── 8. Apply manifests ───────────────────────────────────────────────────────
Step "Applying manifests via kustomize"
kubectl apply -k "$Root\deployment"
Ok "Manifests applied"

# ── 9. Wait for rollout ───────────────────────────────────────────────────────
Step "Waiting for deployments to become ready"
foreach ($deploy in "postgres","vm-watcher","prometheus","grafana") {
    kubectl rollout status deployment/$deploy -n vm-watcher --timeout=120s
    Ok "$deploy is ready"
}

# ── 9b. Optional cluster RBAC for all-namespace mode ────────────────────────
$watchNamespaces = kubectl get deploy vm-watcher -n vm-watcher -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name=='WATCH_NAMESPACES')].value}"
if (Is-AllNamespaceWatch -Value $watchNamespaces) {
    Step "Applying cluster-wide RBAC for all-namespace watch mode"
    kubectl apply -f "$Root\deployment\01-rbac-cluster.yaml"
    Ok "Cluster RBAC applied"
} else {
    Ok "Namespace-list mode detected (WATCH_NAMESPACES='$watchNamespaces') - cluster RBAC not needed"
}

# ── 10. Create an example VirtualMachine in a watched namespace ─────────────
if ($CreateExampleVM) {
    Step "Applying example VirtualMachine manifest"
    kubectl apply -f "$Root\deployment\05-example-vm.yaml"
    Ok "Example VirtualMachine applied in namespace team-a"
}

Write-Host "`nDone! Cluster summary:" -ForegroundColor Yellow
kubectl get pods -n vm-watcher
kubectl get ingress -n vm-watcher
Write-Host "`nAccess URLs (add to C:\Windows\System32\drivers\etc\hosts if needed):" -ForegroundColor Yellow
Write-Host "  echo '127.0.0.1 grafana.local prometheus.local' | sudo tee -a /etc/hosts" -ForegroundColor DarkYellow
Write-Host "`n  Grafana    -> http://grafana.local    (admin / admin)" -ForegroundColor Green
Write-Host "  Prometheus -> http://prometheus.local" -ForegroundColor Green
Write-Host "  Healthz    -> http://localhost:8080/healthz" -ForegroundColor Green
Write-Host "  PostgreSQL -> localhost:5432  db=vmwatcher  user=vmwatcher  pass=changeme" -ForegroundColor Green
Write-Host "`nUseful commands:" -ForegroundColor Yellow
Write-Host "kubectl logs -n vm-watcher deploy/vm-watcher -f" -ForegroundColor Yellow
Write-Host "kubectl get vm -A" -ForegroundColor Yellow
Write-Host ".\scripts\inspect-sink.ps1 -SinkType postgres -Tail 20" -ForegroundColor Yellow
Write-Host "kubectl patch vm fedora-testvm -n team-a --type=merge -p '{\"spec\":{\"runStrategy\":\"Always\"}}'" -ForegroundColor Yellow
Write-Host "kubectl patch vm fedora-testvm -n team-a --type=merge -p '{\"spec\":{\"runStrategy\":\"Halted\"}}'" -ForegroundColor Yellow
