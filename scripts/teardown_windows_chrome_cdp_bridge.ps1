param(
  [int]$ChromeDebugPort = 9222,
  [int]$BridgePort = 9223,
  [string]$ChromeUserDataDir = 'C:\chrome-debug-profile'
)

$ErrorActionPreference = 'Stop'

function Ensure-Admin {
  $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = New-Object Security.Principal.WindowsPrincipal($identity)
  if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Run this script in an Administrator PowerShell window.'
  }
}

Ensure-Admin

Write-Host "1. Stop dedicated debug Chrome on port $ChromeDebugPort ..."
$targets = Get-CimInstance Win32_Process -Filter "Name = 'chrome.exe'" | Where-Object {
  $_.CommandLine -and (
    $_.CommandLine -like "*--remote-debugging-port=$ChromeDebugPort*" -or
    $_.CommandLine -like "*--user-data-dir=$ChromeUserDataDir*"
  )
}
foreach ($process in $targets) {
  try {
    Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
    Write-Host "  stopped chrome pid=$($process.ProcessId)"
  }
  catch {
    Write-Warning "  failed to stop chrome pid=$($process.ProcessId): $($_.Exception.Message)"
  }
}

Write-Host "2. Remove portproxy rule 0.0.0.0:$BridgePort ..."
cmd /c "netsh interface portproxy delete v4tov4 listenaddress=0.0.0.0 listenport=$BridgePort" | Out-Null

Write-Host "3. Remove Windows Firewall rule Chrome-CDP-Bridge-$BridgePort ..."
cmd /c "netsh advfirewall firewall delete rule name=Chrome-CDP-Bridge-$BridgePort" | Out-Null

Write-Host 'Done.'
