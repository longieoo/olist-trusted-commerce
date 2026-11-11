param([ValidateSet('corrections','cdc','dbt','verify','ui','down')][string]$Action='corrections')
$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$compose=@('compose','-p','olist-final','-f','compose.yaml','-f','infra/compose.clean.yaml','-f','infra/compose.cdc.yaml')
function Invoke-Stack {
    & docker @compose @args
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed: exit $LASTEXITCODE" }
}
if (!(Test-Path .env)) { throw 'Run ./scripts/final.ps1 setup first.' }
switch ($Action) {
    'corrections' { Invoke-Stack run --rm --no-deps --entrypoint python3 job scripts/demo_corrections.py }
    'cdc' {
        Invoke-Stack --profile stream --profile cdc up -d --wait postgres kafka connect
        Invoke-Stack run --rm --no-deps job cdc-setup
        Invoke-Stack run --rm --no-deps --entrypoint python3 job scripts/verify_cdc.py
    }
    'dbt' {
        Invoke-Stack build dbt-check
        Invoke-Stack run --rm --no-deps dbt-check
    }
    'verify' {
        Invoke-Stack run --rm --no-deps job test
        Invoke-Stack run --rm --no-deps --entrypoint python3 job scripts/verify_local_source.py
        Invoke-Stack run --rm --no-deps --entrypoint python3 job scripts/verify_dashboard.py
        Invoke-Stack run --rm --no-deps --entrypoint python3 job scripts/check_project.py
        Invoke-Stack run --rm --no-deps --entrypoint python3 job scripts/verify_upgrade.py
    }
    'ui' { Invoke-Stack --profile ui up -d dashboard }
    'down' { Invoke-Stack --profile ui --profile quality --profile stream --profile cdc --profile orchestration down }
}
