<#
.SYNOPSIS
    Validates that your machine and Azure subscription are ready to deploy the
    MX Prism Data Quality solution to Microsoft Fabric.

.DESCRIPTION
    Runs a series of read-only checks and prints a PASS / WARN / FAIL summary:

      1. Required command-line tools are installed and meet minimum versions
      2. You are signed in to the Azure CLI and the target subscription resolves
      3. The Microsoft.Fabric resource provider is registered on the subscription
      4. The requested Fabric capacity SKU is available in the requested region
      5. Existing Fabric capacities in the subscription (so you can reuse one)
      6. Your role assignments at subscription scope

    Nothing is created or modified. Run this before `azd up`.

.PARAMETER Location
    Azure region to deploy the Fabric capacity into, as an Azure region code
    (for example 'eastus2') or display name (for example 'East US 2').
    Defaults to eastus2.

.PARAMETER Sku
    Fabric capacity SKU to check availability for. Defaults to F64.

.PARAMETER Subscription
    Azure subscription ID or name. Defaults to the Azure CLI's active subscription.

.EXAMPLE
    ./Test-Prerequisites.ps1

.EXAMPLE
    ./Test-Prerequisites.ps1 -Location westus2 -Sku F64

.NOTES
    Requires PowerShell 7+ and the Azure CLI.
    Safe to run repeatedly; performs no writes.
#>

[CmdletBinding()]
param(
    [string]$Location = "eastus2",
    [string]$Sku = "F64",
    [string]$Subscription
)

$ErrorActionPreference = "Stop"
$script:Results = [System.Collections.Generic.List[object]]::new()

function Add-Result {
    param(
        [Parameter(Mandatory)][string]$Check,
        [Parameter(Mandatory)][ValidateSet("PASS", "WARN", "FAIL")][string]$Status,
        [string]$Detail = "",
        [string]$Fix = ""
    )
    $script:Results.Add([pscustomobject]@{
            Check  = $Check
            Status = $Status
            Detail = $Detail
            Fix    = $Fix
        })
    $color = switch ($Status) { "PASS" { "Green" } "WARN" { "Yellow" } "FAIL" { "Red" } }
    $icon = switch ($Status) { "PASS" { "[ OK ]" } "WARN" { "[WARN]" } "FAIL" { "[FAIL]" } }
    Write-Host ("{0,-6} {1,-42} {2}" -f $icon, $Check, $Detail) -ForegroundColor $color
}

function Get-CommandVersion {
    <#  Returns the first version-looking token from a tool's version output. #>
    param([string]$Command, [string[]]$VersionArgs)
    try {
        $raw = & $Command @VersionArgs 2>&1 | Out-String
    }
    catch { return $null }
    $m = [regex]::Match($raw, '(\d+)\.(\d+)\.(\d+)')
    if ($m.Success) { return [version]$m.Value }
    return $null
}

function Test-Tool {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Command,
        [string[]]$VersionArgs = @("--version"),
        [version]$Minimum,
        [string]$InstallHint,
        [switch]$Optional
    )
    $cmd = Get-Command $Command -ErrorAction SilentlyContinue
    if (-not $cmd) {
        Add-Result -Check $Name -Status ($Optional ? "WARN" : "FAIL") `
            -Detail "not found on PATH" -Fix $InstallHint
        return
    }
    $ver = Get-CommandVersion -Command $Command -VersionArgs $VersionArgs
    if ($null -eq $ver) {
        Add-Result -Check $Name -Status "WARN" -Detail "installed (version not detected)"
        return
    }
    if ($Minimum -and $ver -lt $Minimum) {
        Add-Result -Check $Name -Status "FAIL" -Detail "v$ver (need >= $Minimum)" -Fix $InstallHint
        return
    }
    Add-Result -Check $Name -Status "PASS" -Detail "v$ver"
}

Write-Host ""
Write-Host "MX Prism Data Quality on Microsoft Fabric - prerequisite check" -ForegroundColor Cyan
Write-Host "=============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Command-line tools" -ForegroundColor White

Test-Tool -Name "PowerShell 7+" -Command "pwsh" -VersionArgs @("--version") -Minimum ([version]"7.0.0") `
    -InstallHint "https://learn.microsoft.com/powershell/scripting/install/installing-powershell"
Test-Tool -Name "Azure CLI (az)" -Command "az" -VersionArgs @("version", "--output", "tsv", "--query", "\`"azure-cli\`"") -Minimum ([version]"2.60.0") `
    -InstallHint "https://learn.microsoft.com/cli/azure/install-azure-cli"
Test-Tool -Name "Azure Developer CLI (azd)" -Command "azd" -VersionArgs @("version") -Minimum ([version]"1.17.2") `
    -InstallHint "https://learn.microsoft.com/azure/developer/azure-developer-cli/install-azd"
Test-Tool -Name "Python 3.10+ (optional)" -Command "python" -VersionArgs @("--version") -Minimum ([version]"3.10.0") `
    -InstallHint "Only needed to regenerate the sample corpus; https://www.python.org/downloads/" -Optional
Test-Tool -Name "Git (optional)" -Command "git" -VersionArgs @("--version") -Minimum ([version]"2.30.0") `
    -InstallHint "Only needed to clone or contribute; https://git-scm.com/downloads" -Optional

# azd 1.23.9 is explicitly excluded by azure.yaml requiredVersions
$azdVer = Get-CommandVersion -Command "azd" -VersionArgs @("version")
if ($azdVer -and $azdVer.ToString() -eq "1.23.9") {
    Add-Result -Check "azd version not 1.23.9" -Status "FAIL" `
        -Detail "azd 1.23.9 is blocked by azure.yaml" -Fix "Upgrade or downgrade azd"
}

Write-Host ""
Write-Host "2. Azure sign-in and subscription" -ForegroundColor White

$account = $null
try {
    $args = @("account", "show", "--output", "json")
    if ($Subscription) { $args += @("--subscription", $Subscription) }
    $account = (& az @args 2>$null) | ConvertFrom-Json
}
catch { $account = $null }

if (-not $account) {
    Add-Result -Check "Azure CLI signed in" -Status "FAIL" -Detail "not signed in" -Fix "Run: az login"
    Write-Host ""
    Write-Host "Cannot continue with Azure checks until you sign in." -ForegroundColor Red
    exit 1
}

Add-Result -Check "Azure CLI signed in" -Status "PASS" -Detail "$($account.user.name)"
Add-Result -Check "Target subscription" -Status "PASS" -Detail "$($account.name) ($($account.id))"
$subId = $account.id

Write-Host ""
Write-Host "3. Resource provider registration" -ForegroundColor White

$rpState = (& az provider show --namespace Microsoft.Fabric --subscription $subId --query registrationState -o tsv 2>$null)
if ($rpState -eq "Registered") {
    Add-Result -Check "Microsoft.Fabric provider" -Status "PASS" -Detail "Registered"
}
else {
    Add-Result -Check "Microsoft.Fabric provider" -Status "FAIL" -Detail "$rpState" `
        -Fix "az provider register --namespace Microsoft.Fabric --subscription $subId"
}

Write-Host ""
Write-Host "4. Fabric capacity SKU availability" -ForegroundColor White

# Resolve the requested location to its Azure display name, because the
# Microsoft.Fabric SKUs API reports locations using display names.
$locDisplay = (& az account list-locations --query "[?name=='$($Location.ToLower())'].displayName | [0]" -o tsv 2>$null)
if (-not $locDisplay) { $locDisplay = $Location }

$skuJson = (& az rest --method get `
        --url "https://management.azure.com/subscriptions/$subId/providers/Microsoft.Fabric/skus?api-version=2023-11-01" `
        --output json 2>$null)

if (-not $skuJson) {
    Add-Result -Check "$Sku availability in $locDisplay" -Status "WARN" `
        -Detail "could not query the Fabric SKUs API" `
        -Fix "Verify manually: https://learn.microsoft.com/fabric/admin/region-availability"
}
else {
    $skus = ($skuJson | ConvertFrom-Json).value
    $match = $skus | Where-Object { $_.name -eq $Sku -and ($_.locations -contains $locDisplay) }
    if ($match) {
        Add-Result -Check "$Sku availability in $locDisplay" -Status "PASS" -Detail "available"
    }
    else {
        $available = ($skus | Where-Object { $_.locations -contains $locDisplay } |
            Select-Object -ExpandProperty name -Unique | Sort-Object) -join ", "
        if ($available) {
            Add-Result -Check "$Sku availability in $locDisplay" -Status "FAIL" `
                -Detail "$Sku not offered in $locDisplay" -Fix "SKUs available there: $available"
        }
        else {
            Add-Result -Check "$Sku availability in $locDisplay" -Status "FAIL" `
                -Detail "no Fabric SKUs offered in '$locDisplay'" `
                -Fix "Check the region name, or pick one from: https://learn.microsoft.com/fabric/admin/region-availability"
        }
    }
}

Write-Host ""
Write-Host "5. Existing Fabric capacities" -ForegroundColor White

$capJson = (& az rest --method get `
        --url "https://management.azure.com/subscriptions/$subId/providers/Microsoft.Fabric/capacities?api-version=2023-11-01" `
        --output json 2>$null)
if ($capJson) {
    $caps = ($capJson | ConvertFrom-Json).value
    if ($caps -and $caps.Count -gt 0) {
        $summary = ($caps | ForEach-Object { "$($_.name) ($($_.sku.name), $($_.location), $($_.properties.state))" }) -join "; "
        Add-Result -Check "Existing capacities" -Status "PASS" -Detail $summary `
            -Fix "To reuse one instead of creating a capacity: azd env set AZURE_EXISTING_FABRIC_CAPACITY_NAME <name>"
    }
    else {
        Add-Result -Check "Existing capacities" -Status "PASS" -Detail "none found - a new one will be created"
    }
}
else {
    Add-Result -Check "Existing capacities" -Status "WARN" -Detail "could not list capacities"
}

Write-Host ""
Write-Host "6. Subscription role assignments" -ForegroundColor White

$principalId = (& az ad signed-in-user show --query id -o tsv 2>$null)
if (-not $principalId) {
    Add-Result -Check "Role assignments" -Status "WARN" `
        -Detail "could not resolve signed-in user (service principal login?)" `
        -Fix "Verify manually in the Azure portal under Subscription > Access control (IAM)"
}
else {
    $roles = (& az role assignment list --assignee $principalId --subscription $subId `
            --include-inherited --query "[].roleDefinitionName" -o tsv 2>$null)
    $roleList = @($roles) | Where-Object { $_ }
    if (-not $roleList) {
        Add-Result -Check "Role assignments" -Status "WARN" -Detail "none found at subscription scope" `
            -Fix "You need Contributor + User Access Administrator, or Owner"
    }
    else {
        $joined = ($roleList | Sort-Object -Unique) -join ", "
        $canWrite = $roleList -contains "Owner" -or $roleList -contains "Contributor"
        $canAssign = $roleList -contains "Owner" -or $roleList -contains "User Access Administrator" `
            -or $roleList -contains "Role Based Access Control Administrator"
        if ($canWrite -and $canAssign) {
            Add-Result -Check "Role assignments" -Status "PASS" -Detail $joined
        }
        else {
            Add-Result -Check "Role assignments" -Status "WARN" -Detail $joined `
                -Fix "Deployment needs resource-write AND role-assignment rights (Owner, or Contributor + User Access Administrator)"
        }
    }
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

$fail = @($script:Results | Where-Object Status -eq "FAIL")
$warn = @($script:Results | Where-Object Status -eq "WARN")

Write-Host ""
Write-Host "=============================================================" -ForegroundColor Cyan
Write-Host ("Summary: {0} passed, {1} warning(s), {2} failure(s)" -f `
    @($script:Results | Where-Object Status -eq "PASS").Count, $warn.Count, $fail.Count) -ForegroundColor Cyan
Write-Host "=============================================================" -ForegroundColor Cyan

if ($fail.Count -or $warn.Count) {
    Write-Host ""
    Write-Host "Action needed:" -ForegroundColor Yellow
    foreach ($r in ($fail + $warn)) {
        if ($r.Fix) {
            Write-Host ("  - {0}: {1}" -f $r.Check, $r.Fix) -ForegroundColor Yellow
        }
    }
}

Write-Host ""
Write-Host "Note: Fabric tenant settings cannot be validated from the Azure CLI." -ForegroundColor DarkGray
Write-Host "      Have a Fabric administrator confirm the settings listed in" -ForegroundColor DarkGray
Write-Host "      docs/Deploy.md before deploying." -ForegroundColor DarkGray
Write-Host ""

if ($fail.Count -gt 0) { exit 1 }
exit 0
