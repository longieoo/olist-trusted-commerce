param([ValidateSet('setup','demo-fixture','demo-real','test','integration','stream-test','ui','down')][string]$Action='demo-fixture')
$ErrorActionPreference='Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$compose=@('compose','-p','olist-final','-f','compose.yaml','-f','infra/compose.clean.yaml','-f','infra/compose.cdc.yaml')
function Invoke-Docker {
    & docker @compose @args
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed: exit $LASTEXITCODE" }
}
function Initialize-Local {
    if (!(Test-Path .env)) {
        $rng=[System.Security.Cryptography.RandomNumberGenerator]::Create()
        $bytes=New-Object byte[] 32; $rng.GetBytes($bytes)
        $password=[Convert]::ToBase64String($bytes)
        $text=[IO.File]::ReadAllText((Join-Path (Get-Location) '.env.example'))
        $text=$text.Replace('replace-with-random-local-password',$password)
        [IO.File]::WriteAllText((Join-Path (Get-Location) '.env'),$text)
        $rng.Dispose()
    }
    New-Item -ItemType Directory -Force evidence | Out-Null
}
Initialize-Local
switch ($Action) {
    'setup' { Invoke-Docker build job; Invoke-Docker up -d --wait postgres minio }
    'demo-fixture' {
        Invoke-Docker up -d --wait postgres minio
        Invoke-Docker run --rm job smoke
        $bid=(Get-Content evidence/smoke.json -Raw | ConvertFrom-Json).batch_id
        Invoke-Docker run --rm job local-publish --batch-id $bid --root data/local-fixture --max-reject-rate 1
        Write-Host 'Fixture published to data/local-fixture. It deliberately contains invalid records.'
    }
    'demo-real' {
        Invoke-Docker up -d --wait postgres minio
        Invoke-Docker run --rm job seed --directory data/real --kind real_olist
        Invoke-Docker run --rm job batch --directory data/real
        Invoke-Docker run --rm job local-publish --directory data/real
        Invoke-Docker --profile ui up -d dashboard
        Write-Host 'Dashboard: http://localhost:8510'
    }
    'test' { Invoke-Docker run --rm --no-deps job test }
    'integration' { Invoke-Docker run --rm job test --integration }
    'stream-test' {
        Invoke-Docker --profile stream up -d --wait kafka
        Invoke-Docker run --rm job stream-smoke
    }
    'ui' { Invoke-Docker --profile ui up -d dashboard }
    'down' { Invoke-Docker --profile ui --profile quality --profile stream --profile cdc --profile orchestration down }
}
