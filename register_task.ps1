$taskName  = "TwixBot"
$botDir    = "C:\Users\ojeku\OneDrive\Documents\GitHub\Twix0514"
$python    = (Get-Command python).Source
$logFile   = "$botDir\bot_stderr.log"

# Remove old task if exists
Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue

$action  = New-ScheduledTaskAction `
    -Execute $python `
    -Argument "bot.py" `
    -WorkingDirectory $botDir

# Trigger: at log-on + repeat every 5 min (auto-restart if crashed)
$trigger = @(
    (New-ScheduledTaskTrigger -AtLogOn),
    (New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes 5) -Once -At (Get-Date))
)

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Days 365) `
    -RestartCount 99 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -RunOnlyIfNetworkAvailable `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew

$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Highest

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "Twix Polymarket bot — runs on login, restarts on crash" | Out-Null

Write-Host "Task '$taskName' registered. Bot will start automatically at every login."
Write-Host "To start now: Start-ScheduledTask -TaskName '$taskName'"
Write-Host "To stop:      Stop-ScheduledTask  -TaskName '$taskName'"
Write-Host "To remove:    Unregister-ScheduledTask -TaskName TwixBot -Confirm:false"
