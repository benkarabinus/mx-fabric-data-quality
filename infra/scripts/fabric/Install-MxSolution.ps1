<#
.SYNOPSIS
    Deploy the MX Prism Data Quality solution to Microsoft Fabric.

.DESCRIPTION
    Bootstraps the solution with three steps:

      1. create or reuse the Fabric workspace and put it on the capacity
      2. add workspace administrators
      3. invoke Deploy-MxContent.ps1, which creates the lakehouses, uploads the
         notebooks, both pipelines and semantic model, and uploads sample files
         without running any notebook or pipeline

    Authentication delegates to the Azure CLI, so no additional credential
    store, runtime or package install is required. PowerShell 7 and the Azure
    CLI are the only dependencies, and both are already needed by azd.

    An existing workspace is reused, item definitions are updated in place,
    and administrators that already hold the role are skipped. Publication
    replaces remote definitions and uploaded files, but does not reseed tables.

.PARAMETER CapacityName
    Fabric capacity to host the workspace. Defaults to AZURE_FABRIC_CAPACITY_NAME,
    which azd sets from the Bicep output.

.PARAMETER WorkspaceName
    Workspace to create or reuse. Defaults to FABRIC_WORKSPACE_NAME, or
    "mx-fabric-dq-poc-<SOLUTION_SUFFIX>".

.PARAMETER SkipNotebookRun
    Legacy compatibility switch. Redundant: deployment never runs notebooks
    or pipelines. Run setup explicitly for a new workspace, then processing.

.EXAMPLE
    ./Install-MxSolution.ps1

.EXAMPLE
    ./Install-MxSolution.ps1 -WorkspaceName "MX Prism DQ" -WhatIf

.NOTES
    Requires PowerShell 7+ and the Azure CLI. Nothing is installed on your machine.
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [string]$CapacityName = $env:AZURE_FABRIC_CAPACITY_NAME,
    [string]$WorkspaceName = $env:FABRIC_WORKSPACE_NAME,
    [string]$SolutionSuffix = $env:SOLUTION_SUFFIX,
    [string]$CapacityAdministrators = $env:AZURE_FABRIC_CAPACITY_ADMINISTRATORS,
    [string]$WorkspaceAdministrators = $env:FABRIC_WORKSPACE_ADMINISTRATORS,
    [switch]$SkipNotebookRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Import-Module (Join-Path $PSScriptRoot "FabricApi.psm1") -Force

$SolutionName = "MX Prism Data Quality"
$WorkspaceNamePrefix = "mx-fabric-dq-poc"

# $PSScriptRoot is infra/scripts/fabric, so three levels up is the repository root.
$repoRoot = Split-Path (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent) -Parent
$deployScript = Join-Path $PSScriptRoot "Deploy-MxContent.ps1"


# ---------------------------------------------------------------------------
# Validate inputs
# ---------------------------------------------------------------------------

if (-not $CapacityName) {
    throw ("No Fabric capacity specified. Pass -CapacityName, or set " +
           "AZURE_FABRIC_CAPACITY_NAME. When running through azd this is set " +
           "automatically from the Bicep output - check 'azd env get-values'.")
}

if (-not $WorkspaceName) {
    if (-not $SolutionSuffix) {
        throw ("No workspace name available. Pass -WorkspaceName, or set " +
               "FABRIC_WORKSPACE_NAME or SOLUTION_SUFFIX.")
    }
    $WorkspaceName = "$WorkspaceNamePrefix-$SolutionSuffix"
}

if (-not (Test-Path $deployScript)) {
    throw "Content deployment script not found at $deployScript"
}

$administrators = ConvertTo-AdministratorList `
    -CapacityAdministratorsJson $CapacityAdministrators `
    -WorkspaceAdministratorsCsv $WorkspaceAdministrators


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

$started = Get-Date

Write-Host ""
Write-Host ("=" * 60) -ForegroundColor Cyan
Write-Host "  $SolutionName - solution installer" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Cyan
Write-Host "  Capacity  : $CapacityName"
Write-Host "  Workspace : $WorkspaceName"
if ($SolutionSuffix) { Write-Host "  Suffix    : $SolutionSuffix" }
Write-Host "  Source    : $repoRoot"
if ($administrators) {
    Write-Host "  Admins    : $($administrators -join ', ')"
}
Write-Host ""

# Fail fast with a clear message rather than deep inside the first API call.
try {
    $account = az account show --output json 2>&1 | ConvertFrom-Json
    Write-Host "  Signed in : $($account.user.name)"
    Write-Host "  Subscript.: $($account.name)"
}
catch {
    throw "Not signed in to the Azure CLI. Run 'az login' and try again."
}


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

$completed = [System.Collections.Generic.List[string]]::new()

try {
    Write-Step 1 3 "Set up the Fabric workspace"
    $workspaceId = Initialize-FabricWorkspace -CapacityName $CapacityName `
        -WorkspaceName $WorkspaceName
    $completed.Add("workspace")

    Write-Step 2 3 "Add workspace administrators"
    Set-FabricWorkspaceAdmins -WorkspaceId $workspaceId -Administrators $administrators
    $completed.Add("administrators")

    Write-Step 3 3 "Deploy the solution content"
    Write-Info "   Lakehouses, notebooks, both pipelines, semantic model, any report and sample files,"
    Write-Info "   uploaded directly from this repository. Nothing is downloaded."

    $deployArgs = @{
        WorkspaceId = $workspaceId
        RepoRoot    = $repoRoot
    }
    if ($SkipNotebookRun) {
        Write-Info "   -SkipNotebookRun is retained for compatibility; deployment never runs notebooks or pipelines."
    }

    & $deployScript @deployArgs
    if ($LASTEXITCODE -ne 0) { throw "Solution content deployment failed (exit $LASTEXITCODE)." }
    $completed.Add("solution content")
}
catch {
    Write-Host ""
    Write-Err ("=" * 60)
    Write-Err "  Installation failed"
    Write-Err ("=" * 60)
    Write-Err "  $(Get-ExceptionChain -Exception $_.Exception)"
    if ($completed.Count -gt 0) {
        Write-Host "  Completed before the failure: $($completed -join ', ')" -ForegroundColor DarkGray
    }
    Write-Host ""
    Write-Host "  The script is safe to re-run; completed steps are skipped." -ForegroundColor DarkGray

    if ($_.Exception.Message -match 'SSL|TLS|forcibly closed|connection was closed|timed out') {
        Write-Host ""
        Write-Host "  This looks like a transient network fault rather than a configuration" -ForegroundColor DarkGray
        Write-Host "  problem. Each call already retries, so if it persists check for a VPN," -ForegroundColor DarkGray
        Write-Host "  proxy or TLS-inspecting firewall between you and api.fabric.microsoft.com." -ForegroundColor DarkGray
    }
    exit 1
}


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

$elapsed = [int]((Get-Date) - $started).TotalSeconds
$url = "https://app.fabric.microsoft.com/groups/$workspaceId"

Write-Host ""
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host "  Installation complete" -ForegroundColor Green
Write-Host ("=" * 60) -ForegroundColor Green
Write-Host "  Workspace : $WorkspaceName"
Write-Host "  URL       : $url"
Write-Host "  Duration  : $([TimeSpan]::FromSeconds($elapsed).ToString('hh\:mm\:ss'))"
Write-Host ""
Write-Host "  Next: open the workspace and confirm mx_bronze, mx_silver and mx_gold exist."
Write-Host "  New workspace: run mx_setup_pipeline once, then mx_dq_pipeline."
Write-Host "  Initialized workspace: run mx_dq_pipeline for routine processing."
Write-Host "  No notebooks or pipelines were run by deployment."
Write-Warn "  Setup overwrites mapping tables from uploaded CSVs; review custom table edits before reseeding."
Write-Host ""
