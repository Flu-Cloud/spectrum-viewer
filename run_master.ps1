Set-Location 'C:\Users\pipyt\spectrum-viewer'
while ($true) {
  py master_ingest.py *>> master.log
  if ($LASTEXITCODE -eq 0) { break }
  "[$(Get-Date -Format HH:mm:ss)] master exited $LASTEXITCODE, retrying in 30s" | Out-File -Append master.log
  Start-Sleep -Seconds 30
}
