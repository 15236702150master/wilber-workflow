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

function Get-ChromePath {
  $candidates = @(
    (Join-Path $env:ProgramFiles 'Google\Chrome\Application\chrome.exe'),
    (Join-Path ${env:ProgramFiles(x86)} 'Google\Chrome\Application\chrome.exe'),
    (Join-Path $env:LocalAppData 'Google\Chrome\Application\chrome.exe')
  )

  foreach ($path in $candidates) {
    if ($path -and (Test-Path $path)) {
      return $path
    }
  }

  throw 'chrome.exe not found. Update Get-ChromePath in this script.'
}

function Get-DebugEndpointJson {
  param(
    [string]$Url
  )

  try {
    $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 3
    if ($response.StatusCode -ne 200 -or -not $response.Content) {
      return $null
    }
    return $response.Content | ConvertFrom-Json
  }
  catch {
    return $null
  }
}

Ensure-Admin

$chromePath = Get-ChromePath

Write-Host '1. Remove old portproxy rule (if any)...'
cmd /c "netsh interface portproxy delete v4tov4 listenaddress=0.0.0.0 listenport=$BridgePort" | Out-Null

Write-Host "2. Create stable bridge 0.0.0.0:$BridgePort -> 127.0.0.1:$ChromeDebugPort ..."
cmd /c "netsh interface portproxy add v4tov4 listenaddress=0.0.0.0 listenport=$BridgePort connectaddress=127.0.0.1 connectport=$ChromeDebugPort"

Write-Host "3. Open Windows Firewall for TCP $BridgePort ..."
cmd /c "netsh advfirewall firewall delete rule name=Chrome-CDP-Bridge-$BridgePort" | Out-Null
cmd /c "netsh advfirewall firewall add rule name=Chrome-CDP-Bridge-$BridgePort dir=in action=allow protocol=TCP localport=$BridgePort" | Out-Null

Write-Host '4. Launch dedicated debug Chrome ...'
Start-Process -FilePath $chromePath -ArgumentList @(
  "--remote-debugging-port=$ChromeDebugPort",
  '--remote-debugging-address=127.0.0.1',
  "--user-data-dir=$ChromeUserDataDir",
  '--no-first-run',
  '--no-default-browser-check'
)

Start-Sleep -Seconds 2

$debugVersionUrl = "http://127.0.0.1:$ChromeDebugPort/json/version"
$debugInfo = Get-DebugEndpointJson -Url $debugVersionUrl

Write-Host ''
if ($debugInfo -and $debugInfo.Browser) {
  Write-Host 'Chrome DevTools endpoint is ready.'
  Write-Host "  Browser: $($debugInfo.Browser)"
  Write-Host "  WebSocket: $($debugInfo.webSocketDebuggerUrl)"
}
else {
  Write-Warning "Chrome debug endpoint did not return DevTools JSON at $debugVersionUrl"
  Write-Host 'Check whether another process is occupying 9222:'
  Write-Host "  netstat -ano | findstr :$ChromeDebugPort"
  Write-Host 'Then inspect the owning PID:'
  Write-Host '  tasklist /FI "PID eq <PID_FROM_NETSTAT>"'
}

Write-Host ''
Write-Host 'Done. Stable setup:'
Write-Host "  Chrome debug endpoint: 127.0.0.1:$ChromeDebugPort"
Write-Host "  WSL bridge endpoint:   0.0.0.0:$BridgePort"
Write-Host ''
Write-Host 'Check with:'
Write-Host "  netstat -ano | findstr :$BridgePort"
Write-Host "  Invoke-WebRequest http://127.0.0.1:$ChromeDebugPort/json/version"
