param(
  [Parameter(Mandatory = $true)]
  [string]$ScriptPath,

  [switch]$WaitForExit,

  [switch]$NoWindow
)

$ErrorActionPreference = 'Stop'

$arguments = @(
  '-NoProfile',
  '-ExecutionPolicy', 'Bypass',
  '-File', $ScriptPath
)

$options = @{
  FilePath     = 'powershell.exe'
  Verb         = 'RunAs'
  ArgumentList = $arguments
  PassThru     = $true
}

if ($WaitForExit) {
  $options['Wait'] = $true
}

$options['WindowStyle'] = if ($NoWindow) { 'Hidden' } else { 'Normal' }

$process = Start-Process @options

if ($WaitForExit) {
  exit $process.ExitCode
}
