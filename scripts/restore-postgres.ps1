<#
.SYNOPSIS
  Restores a PostgreSQL backup dump into vm-watcher postgres pod.

.DESCRIPTION
  Restores a pg_dump custom-format backup using pg_restore.
  Existing database objects are dropped and recreated.

.EXAMPLE
  .\scripts\restore-postgres.ps1 -BackupFile .\backups\vmwatcher-backup-20260615-101500.dump
#>
param(
  [Parameter(Mandatory = $true)]
  [string]$BackupFile,

  [string]$Namespace = "vm-watcher",
  [string]$PodSelector = "app=postgres",
  [string]$Database = "vmwatcher",
  [string]$Username = "vmwatcher"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path $BackupFile)) {
  throw "Backup file not found: $BackupFile"
}

$pod = kubectl get pod -n $Namespace -l $PodSelector -o jsonpath="{.items[0].metadata.name}"
if ([string]::IsNullOrWhiteSpace($pod)) {
  throw "Postgres pod not found in namespace '$Namespace' with selector '$PodSelector'."
}

Write-Host "Restoring backup '$BackupFile' into pod '$pod'..." -ForegroundColor Cyan

# --clean + --if-exists makes restore idempotent over existing objects.
Get-Content -Path $BackupFile -Encoding Byte | kubectl exec -i -n $Namespace $pod -- pg_restore -U $Username -d $Database --clean --if-exists --no-owner --no-privileges

Write-Host "Restore completed successfully." -ForegroundColor Green
