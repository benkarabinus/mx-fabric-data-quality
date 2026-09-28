<#
.SYNOPSIS
    Remove the MX Prism Data Quality solution from Microsoft Fabric.

.DESCRIPTION
    Deletes the Fabric workspace created by Install-MxSolution.ps1, which
    removes every item inside it.

    Run by azd as a predown hook, before the Azure resource group and Fabric
    capacity are deleted.

.PARAMETER WorkspaceName
    Workspace to delete. Defaults to FABRIC_WORKSPACE_NAME, or
    "mx-fabric-dq-poc-<SOLUTION_SUFFIX>". Must match what the installer used.

.PARAMETER WorkspaceId
    Delete by ID instead of by name. Takes precedence over the name.

.PARAMETER Force
    Skip the confirmation prompt. azd sets this automatically when running
    non-interactively.

.EXAMPLE
    ./Remove-MxSolution.ps1 -WhatIf

.NOTES
    Deleting a workspace permanently removes every item in it, including all
    lakehouse data. Requires PowerShell 7+ and the Azure CLI.
#>

[CmdletBinding(SupportsShouldProcess, ConfirmImpact = "High")]
param(
    [string]$WorkspaceName = $env:FABRIC_WORKSPACE_NAME,
    [string]$WorkspaceId = $env:FABRIC_WORKSPACE_ID,
    [string]$SolutionSuffix = $env:SOLUTION_SUFFIX,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Import-Module (Join-Path $PSScriptRoot "FabricApi.psm1") -Force

# Must match WorkspaceNamePrefix in Install-MxSolution.ps1, otherwise this looks
# for a workspace the installer never created.
$WorkspaceNamePrefix = "mx-fabric-dq-poc"

if (-not $WorkspaceId -and -not $WorkspaceName) {
    if (-not $SolutionSuffix) {
        Write-Warn "No workspace name, ID or solution suffix supplied; nothing to remove."
        exit 0
    }
    $WorkspaceName = "$WorkspaceNamePrefix-$SolutionSuffix"
}

Write-Host ""
Write-Host ("=" * 60) -ForegroundColor Cyan
Write-Host "  MX Prism Data Quality - remove solution" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Cyan

try {
    if ($WorkspaceId) {
        Write-Info "  Target workspace ID: $WorkspaceId"
        $target = $WorkspaceId
        $displayName = $WorkspaceId
    }
    else {
        Write-Info "  Looking up workspace: $WorkspaceName"
        $workspace = Get-FabricWorkspace -Name $WorkspaceName
        if (-not $workspace) {
            # Nothing to do is a success, not a failure - azd down must not
            # break because the workspace was already removed by hand.
            Write-Warn "  Workspace '$WorkspaceName' not found; nothing to remove."
            exit 0
        }
        $target = $workspace.id
        $displayName = $workspace.displayName
        Write-Ok "  Found workspace: $displayName ($target)"
    }

    Write-Host ""
    Write-Warn "  Deleting a workspace permanently removes every item in it,"
    Write-Warn "  including all lakehouse data. This cannot be undone."
    Write-Host ""

    if (-not $Force -and -not $PSCmdlet.ShouldProcess($displayName, "Delete Fabric workspace")) {
        Write-Info "  Cancelled; nothing was deleted."
        exit 0
    }

    Remove-FabricWorkspace -WorkspaceId $target -Confirm:$false
    Write-Ok "  Deleted workspace: $displayName"
}
catch {
    Write-Err "  Failed to remove the workspace: $($_.Exception.Message)"
    Write-Host ""
    Write-Host "  Delete it manually in the Fabric portal if needed:" -ForegroundColor DarkGray
    Write-Host "  Workspace settings > General > Remove this workspace" -ForegroundColor DarkGray
    exit 1
}

Write-Host ""
