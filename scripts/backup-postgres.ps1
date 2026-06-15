<#
.SYNOPSIS
  Creates a PostgreSQL backup dump from the vm-watcher postgres pod.

.DESCRIPTION
  Uses pg_dump inside the postgres pod and writes a compressed custom dump
  to a local file so it can be stored externally.

.EXAMPLE
  .\scripts\backup-postgres.ps1
  .\scripts\backup-postgres.ps1 -OutputDir .\backups
#>
param(
  [string]$Namespace = "vm-watcher",
  [string]$PodSelector = "app=postgres",
  [string]$Database = "vmwatcher",
  [string]$Username = "vmwatcher",
  [string]$OutputDir = ".\backups"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path $OutputDir)) {
  New-Item -ItemType Directory -Path $OutputDir | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$outFile = Join-Path $OutputDir "vmwatcher-backup-$timestamp.dump"

$pod = kubectl get pod -n $Namespace -l $PodSelector -o jsonpath="{.items[0].metadata.name}"
if ([string]::IsNullOrWhiteSpace($pod)) {
  throw "Postgres pod not found in namespace '$Namespace' with selector '$PodSelector'."
}

Write-Host "Creating backup from pod '$pod'..." -ForegroundColor Cyan

# Custom format (-Fc) is preferred for reliable restore with pg_restore.
kubectl exec -n $Namespace $pod -- pg_dump -U $Username -d $Database -Fc | Set-Content -Path $outFile -Encoding Byte

Write-Host "Backup created: $outFile" -ForegroundColor Green
Write-Host "Tip: copy this file to external storage (NAS/object storage/secure file share)." -ForegroundColor Yellow
