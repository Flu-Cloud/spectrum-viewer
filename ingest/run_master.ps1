# Run the full multi-sensor ingest detached, with a crash-retry loop.
# Resolves the repo root from this script's location, so run it from anywhere.
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
while ($true) {
  py ingest\master_ingest.py *>> master.log
  if ($LASTEXITCODE -eq 0) { break }
  "[$(Get-Date -Format HH:mm:ss)] master exited $LASTEXITCODE, retrying in 30s" | Out-File -Append master.log
  Start-Sleep -Seconds 30
}
