param(
    [Parameter(Mandatory=$true)][string]$Date,
    [Parameter(Mandatory=$true)][ValidateSet("customer", "account", "transactions")][string]$Stage,
    [string]$OutputUri = $env:OUTPUT_URI
)

$ErrorActionPreference = "Stop"
$project = Split-Path -Parent $MyInvocation.MyCommand.Path
& python "$project/tools/run_partition.py" --date $Date --stage $Stage --output-uri $OutputUri
