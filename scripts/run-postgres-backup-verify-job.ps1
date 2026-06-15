<#
.SYNOPSIS
  Triggers a PostgreSQL backup integrity verification job.

.DESCRIPTION
  Runs a one-off job that validates the newest backup dump on the backup PVC
  with pg_restore --list.

.EXAMPLE
  .\scripts\run-postgres-backup-verify-job.ps1
#>
param(
  [string]$Namespace = "vm-watcher",
  [string]$CronJobName = "postgres-backup-verify"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ts = Get-Date -Format "yyyyMMddHHmmss"
$jobName = "$CronJobName-manual-$ts"

kubectl create job --from=cronjob/$CronJobName $jobName -n $Namespace | Out-Host
kubectl wait --for=condition=Complete -n $Namespace job/$jobName --timeout=180s | Out-Host
kubectl logs -n $Namespace job/$jobName | Out-Host

Write-Host "Verification job completed: $jobName" -ForegroundColor Green
