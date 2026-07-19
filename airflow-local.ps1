param(
    [ValidateSet("start", "stop", "logs", "status")]
    [string]$Action = "start"
)

$ErrorActionPreference = "Stop"
$project = Split-Path -Parent $MyInvocation.MyCommand.Path
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
$docker = if ($dockerCommand) {
    $dockerCommand.Source
} else {
    $candidates = @(
        "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin\docker.exe",
        "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
    )
    $candidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
}
if (-not $docker) {
    throw "Docker Desktop is required. Install it, start it, then rerun this command."
}
$dockerBin = Split-Path -Parent $docker
$env:PATH = "$dockerBin;$env:PATH"

Push-Location $project
try {
    switch ($Action) {
        "start" {
            & $docker compose up --build --detach
            Write-Host "Airflow is starting at http://localhost:8080"
            Write-Host "Run .\airflow-local.ps1 logs to view the generated admin password."
        }
        "stop" { & $docker compose down }
        "logs" { & $docker compose logs --follow airflow }
        "status" { & $docker compose ps }
    }
} finally {
    Pop-Location
}
