<#
.SYNOPSIS
  Triggers an immediate PostgreSQL backup using the postgres-backup CronJob template.

.EXAMPLE
  .\scripts\run-postgres-backup-job.ps1
#>
param(
  [string]$Namespace = "vm-watcher",
  [string]$CronJobName = "postgres-backup"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ts = Get-Date -Format "yyyyMMddHHmmss"
$jobName = "$CronJobName-manual-$ts"

kubectl create job --from=cronjob/$CronJobName $jobName -n $Namespace | Out-Host
kubectl wait --for=condition=Complete -n $Namespace job/$jobName --timeout=180s | Out-Host
kubectl logs -n $Namespace job/$jobName | Out-Host

Write-Host "Manual backup job completed: $jobName" -ForegroundColor Green
