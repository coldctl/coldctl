# Requires the emulator and coldctl-test bucket from compose.gcs.yml.
$ErrorActionPreference = 'Stop'
$previousEndpoint = $env:COLDCTL_TEST_GCS_ENDPOINT
$fixture = $null
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    $fixture = Start-Process -FilePath (Get-Command python).Source -ArgumentList 'scripts/gcs-emulator-proxy.py' -WindowStyle Hidden -PassThru
    Start-Sleep -Seconds 1
    if ($fixture.HasExited) { throw 'GCS fixture failed to start; check port 14445.' }
    $null = Invoke-RestMethod http://127.0.0.1:14445/__fixture_stats
    $env:COLDCTL_TEST_GCS_ENDPOINT = 'http://127.0.0.1:14445'
    cargo test --locked -p coldctl-core --lib gcs_ -- --ignored
    if ($LASTEXITCODE -ne 0) { throw 'GCS archive tests failed.' }
    $stats = Invoke-RestMethod http://127.0.0.1:14445/__fixture_stats
    if ($stats.precondition_failures -lt 1) { throw 'Create-only conflict path was not exercised.' }
    $stats
} finally {
    if ($fixture -and -not $fixture.HasExited) { Stop-Process -Id $fixture.Id }
    $env:COLDCTL_TEST_GCS_ENDPOINT = $previousEndpoint
    Pop-Location
}
