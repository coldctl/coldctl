# First registry only. Never prints private keys or uploads them anywhere.
$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent $PSScriptRoot
$privateDirectory = Join-Path $repository 'secrets/connector-signing'
$publicDirectory = Join-Path $repository 'release'
$publicRoot = Join-Path $publicDirectory 'root.json'
if ((Test-Path -LiteralPath $privateDirectory) -or (Test-Path -LiteralPath $publicRoot)) {
    throw 'Signing material already exists. Do not regenerate an established trust root.'
}
New-Item -ItemType Directory -Path $privateDirectory | Out-Null
$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$owner = (Get-Acl -LiteralPath $repository).Owner
$ownerSid = ([System.Security.Principal.NTAccount]::new($owner)).Translate([System.Security.Principal.SecurityIdentifier]).Value
& icacls.exe $privateDirectory /inheritance:r /grant:r "*${sid}:(OI)(CI)F" "*${ownerSid}:(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Unable to restrict private-key directory permissions.' }
$keys = Join-Path $privateDirectory 'keys'
$expires = (Get-Date).ToUniversalTime().AddYears(2).ToString("yyyy-MM-ddTHH:mm:ssZ")
Push-Location $repository
try {
    cargo run --locked -p coldctl-core --example bootstrap_connector_registry -- $keys $expires
    if ($LASTEXITCODE -ne 0) { throw 'Signing bootstrap failed. Inspect the private directory before retrying.' }
    New-Item -ItemType Directory -Force -Path $publicDirectory | Out-Null
    Copy-Item -LiteralPath (Join-Path $keys 'root.json') -Destination $publicRoot
    Write-Output "Public root: $publicRoot"
    Write-Output "Private keys: $keys (gitignored, operator/repository-owner/SYSTEM access only)"
    Write-Output 'Back up root.private.pem offline. Add online.private.pem as the COLDCTL_TUF_ONLINE_KEY GitHub Actions secret.'
} finally { Pop-Location }
