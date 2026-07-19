$ErrorActionPreference = "Stop"

$docker = Get-Command docker -ErrorAction SilentlyContinue
if (-not $docker) {
    $desktopDocker = Join-Path $env:LOCALAPPDATA "Programs\DockerDesktop\resources\bin\docker.exe"
    if (-not (Test-Path -LiteralPath $desktopDocker)) {
        throw "Docker Desktop is required and docker.exe was not found."
    }
    $docker = $desktopDocker
}

& $docker compose up -d airflow
if ($LASTEXITCODE -ne 0) { throw "Could not start the local stack." }

& $docker compose exec -T airflow dbt build `
    --project-dir /opt/project `
    --profiles-dir /opt/project `
    --vars "{output_uri: 's3://hybrid/delta'}"
if ($LASTEXITCODE -ne 0) { throw "dbt build failed; documentation was not generated." }

& $docker compose exec -T airflow dbt docs generate `
    --project-dir /opt/project `
    --profiles-dir /opt/project `
    --vars "{output_uri: 's3://hybrid/delta'}"
if ($LASTEXITCODE -ne 0) { throw "dbt docs generate failed." }

Write-Host "dbt documentation generated at target\index.html"
