<#
.SYNOPSIS
    Microsoft Fabric and Microsoft Graph REST helpers for the MX Prism Data
    Quality solution.

.DESCRIPTION
    Provides just enough of the Fabric and Graph REST surface to bootstrap the
    solution: find a capacity, create and configure a workspace, add workspace
    administrators, upload the installer notebook, and run it.

    Authentication delegates to the Azure CLI, which is already a prerequisite
    for deployment. That removes any need for a separate credential library,
    a Python runtime, or a package install at deployment time.

.NOTES
    Requires PowerShell 7+ and the Azure CLI. No other dependencies.
#>

Set-StrictMode -Version Latest

$script:FabricApiRoot = "https://api.fabric.microsoft.com/v1"
$script:GraphApiRoot = "https://graph.microsoft.com/v1.0"

# A few semantic model settings, storage format among them, are still only
# exposed on the older Power BI REST surface rather than the Fabric one.
$script:PowerBIApiRoot = "https://api.powerbi.com/v1.0/myorg"

$script:TokenCache = @{}


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

function Write-Info { param([string]$Message) Write-Host $Message -ForegroundColor Cyan }
function Write-Ok { param([string]$Message) Write-Host $Message -ForegroundColor Green }
function Write-Warn { param([string]$Message) Write-Host $Message -ForegroundColor Yellow }
function Write-Err { param([string]$Message) Write-Host $Message -ForegroundColor Red }

function Write-Step {
    param([int]$Number, [int]$Total, [string]$Title)
    Write-Host ""
    Write-Host ("[{0}/{1}] {2}" -f $Number, $Total, $Title) -ForegroundColor White
    Write-Host ("-" * 60) -ForegroundColor DarkGray
}

function Get-ExceptionChain {
    <#
    .SYNOPSIS
        Flatten an exception and its inner exceptions into one readable line.

    .DESCRIPTION
        .NET reports a failed TLS handshake as "The SSL connection could not be
        established, see inner exception." - the useful cause is only in
        InnerException. Printing just the outer Message therefore tells the
        operator nothing actionable, so walk the whole chain.
    #>
    param([System.Exception]$Exception)

    $messages = [System.Collections.Generic.List[string]]::new()
    $current = $Exception
    $guard = 0

    while ($current -and $guard -lt 10) {
        $text = $current.Message.Trim()
        # Skip the placeholder that only points at the inner exception.
        if ($text -and $text -notmatch 'see inner exception' -and -not $messages.Contains($text)) {
            $messages.Add($text)
        }
        $current = $current.InnerException
        $guard++
    }

    if ($messages.Count -eq 0) { return $Exception.Message }
    return ($messages -join " -> ")
}


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

function Get-FabricAccessToken {
    <#
    .SYNOPSIS
        Acquire an access token for a resource through the Azure CLI.

    .DESCRIPTION
        Tokens are cached in-process and refreshed a few minutes before expiry,
        so a long deployment does not fail part-way through on an expired token.
    #>
    [CmdletBinding()]
    param(
        [ValidateSet("Fabric", "Graph", "Storage", "PowerBI")]
        [string]$Resource = "Fabric"
    )

    $resourceUrl = switch ($Resource) {
        "Fabric" { "https://api.fabric.microsoft.com" }
        "Graph" { "https://graph.microsoft.com" }
        # OneLake is addressed through the ADLS Gen2 DFS endpoint, which takes a
        # storage-audience token rather than a Fabric one.
        "Storage" { "https://storage.azure.com" }
        # The Power BI REST API predates Fabric and keeps its own audience.
        "PowerBI" { "https://analysis.windows.net/powerbi/api" }
    }

    $cached = $script:TokenCache[$Resource]
    if ($cached -and $cached.Expires -gt (Get-Date).AddMinutes(5)) {
        return $cached.Token
    }

    $json = az account get-access-token --resource $resourceUrl --output json 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw ("Could not acquire a $Resource token from the Azure CLI. " +
               "Run 'az login' and confirm the correct subscription is selected.`n$json")
    }

    $parsed = $json | ConvertFrom-Json
    $expires = if ($parsed.PSObject.Properties.Name -contains "expires_on") {
        [DateTimeOffset]::FromUnixTimeSeconds([int64]$parsed.expires_on).LocalDateTime
    }
    else {
        (Get-Date).AddMinutes(45)
    }

    $script:TokenCache[$Resource] = @{ Token = $parsed.accessToken; Expires = $expires }
    return $parsed.accessToken
}


# ---------------------------------------------------------------------------
# Request plumbing
# ---------------------------------------------------------------------------

function Invoke-FabricApi {
    <#
    .SYNOPSIS
        Call a Fabric, Graph, or Power BI REST endpoint.

    .DESCRIPTION
        Handles bearer auth, JSON serialization, HTTP 429 rate limiting with
        Retry-After, and long-running operations returned as HTTP 202 with a
        Location header.

    .PARAMETER Uri
        Relative to the API root, or an absolute URL for LRO polling.

    .PARAMETER RawResponse
        Return the full response object (status code and headers) rather than
        just the parsed body. Needed by callers that must inspect a 202.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Uri,
        [ValidateSet("GET", "POST", "PATCH", "PUT", "DELETE")][string]$Method = "GET",
        [object]$Body,
        [ValidateSet("Fabric", "Graph", "PowerBI")][string]$Resource = "Fabric",
        [switch]$RawResponse,
        [switch]$NoWaitForLro,
        [int]$MaxRetries = 3,
        [int]$TimeoutSec = 300
    )

    $root = switch ($Resource) {
        "Graph" { $script:GraphApiRoot }
        "PowerBI" { $script:PowerBIApiRoot }
        default { $script:FabricApiRoot }
    }
    $url = if ($Uri -match '^https?://') { $Uri } else { "$root/$($Uri.TrimStart('/'))" }

    $params = @{
        Uri                     = $url
        Method                  = $Method
        Headers                 = @{ Authorization = "Bearer $(Get-FabricAccessToken -Resource $Resource)" }
        ContentType             = "application/json"
        TimeoutSec              = $TimeoutSec
        SkipHttpErrorCheck      = $true
        ResponseHeadersVariable = "responseHeaders"
        StatusCodeVariable      = "statusCode"
        ErrorAction             = "Stop"
    }
    if ($null -ne $Body) {
        $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 20 -Compress }
    }

    for ($attempt = 0; $attempt -le $MaxRetries; $attempt++) {
        try {
            $result = Invoke-RestMethod @params
        }
        catch {
            # Transport-level failure: TLS handshake, connection reset, timeout
            # or DNS. HTTP status errors never reach here because
            # SkipHttpErrorCheck returns them through $statusCode instead, so
            # anything caught here is a network fault and worth retrying.
            #
            # These are observed intermittently against the Fabric endpoint, and
            # without a retry a single dropped handshake fails the whole
            # deployment part-way through.
            $detail = Get-ExceptionChain -Exception $_.Exception

            if ($attempt -ge $MaxRetries) {
                throw ("$Method $url failed after $($attempt + 1) attempt(s). $detail")
            }

            $wait = [math]::Pow(2, $attempt) + (Get-Random -Minimum 0.0 -Maximum 1.0)
            Write-Warn ("      network error, retrying in {0:N1}s ({1})" -f $wait, $detail)
            Start-Sleep -Seconds $wait
            continue
        }

        # Rate limited - honour Retry-After and try again.
        if ($statusCode -eq 429) {
            $wait = 30
            if ($responseHeaders.ContainsKey("Retry-After")) {
                $wait = [int]($responseHeaders["Retry-After"] | Select-Object -First 1)
            }
            Write-Warn ("      rate limited, retrying in {0}s" -f $wait)
            Start-Sleep -Seconds $wait
            continue
        }

        if ($statusCode -ge 400) {
            $detail = if ($result) { ($result | ConvertTo-Json -Depth 6 -Compress) } else { "" }
            throw "$Method $url failed: HTTP $statusCode $detail"
        }

        # Long-running operation: poll the Location header until it settles.
        if ($statusCode -eq 202 -and -not $NoWaitForLro) {
            $location = $null
            if ($responseHeaders -and $responseHeaders.ContainsKey("Location")) {
                $location = $responseHeaders["Location"] | Select-Object -First 1
            }
            if ($location) {
                return Wait-FabricLongRunningOperation -Location $location -Resource $Resource
            }
        }

        if ($RawResponse) {
            return [pscustomobject]@{
                StatusCode = $statusCode
                Headers    = $responseHeaders
                Body       = $result
            }
        }
        return $result
    }

    throw "$Method $url failed after $MaxRetries retries (rate limited)."
}


function Wait-FabricLongRunningOperation {
    <#
    .SYNOPSIS
        Poll a Fabric long-running operation until it succeeds or fails.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Location,
        [string]$Resource = "Fabric",
        [int]$IntervalSeconds = 5,
        [int]$TimeoutSeconds = 1800
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds $IntervalSeconds

        $response = Invoke-FabricApi -Uri $Location -Resource $Resource `
            -RawResponse -NoWaitForLro
        $body = $response.Body

        # A 200 with no status field means the operation finished and the body
        # is the result itself.
        if (-not $body -or -not ($body.PSObject.Properties.Name -contains "status")) {
            if ($response.StatusCode -eq 200) { return $body }
            continue
        }

        switch ($body.status) {
            "Succeeded" { return $body }
            "Completed" { return $body }
            "Failed" {
                $err = if ($body.PSObject.Properties.Name -contains "error") {
                    $body.error | ConvertTo-Json -Depth 6 -Compress
                }
                else { "no error detail" }
                throw "Long-running operation failed: $err"
            }
            default { }   # Running / NotStarted - keep polling
        }
    }

    throw "Long-running operation did not complete within $TimeoutSeconds seconds."
}


# ---------------------------------------------------------------------------
# Capacities and workspaces
# ---------------------------------------------------------------------------

function Get-FabricCapacity {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Name)

    $response = Invoke-FabricApi -Uri "capacities"
    return $response.value | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
}


function Get-FabricWorkspace {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Name)

    $response = Invoke-FabricApi -Uri "workspaces"
    return $response.value | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
}


function New-FabricWorkspace {
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$Name,
        [string]$CapacityId
    )

    $body = @{ displayName = $Name }
    if ($CapacityId) { $body.capacityId = $CapacityId }

    if (-not $PSCmdlet.ShouldProcess($Name, "Create Fabric workspace")) { return }
    $created = Invoke-FabricApi -Uri "workspaces" -Method POST -Body $body
    return $created.id
}


function Set-FabricWorkspaceCapacity {
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$CapacityId
    )

    if (-not $PSCmdlet.ShouldProcess($WorkspaceId, "Assign to capacity $CapacityId")) { return }
    Invoke-FabricApi -Uri "workspaces/$WorkspaceId/assignToCapacity" -Method POST `
        -Body @{ capacityId = $CapacityId } | Out-Null
}


function Initialize-FabricWorkspace {
    <#
    .SYNOPSIS
        Find or create the workspace and make sure it sits on the target capacity.

    .DESCRIPTION
        Safe to re-run. An existing workspace already on the right capacity is
        left alone; one on a different capacity is reassigned.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$CapacityName,
        [Parameter(Mandatory)][string]$WorkspaceName
    )

    Write-Info "   Looking up capacity: $CapacityName"
    $capacity = Get-FabricCapacity -Name $CapacityName
    if (-not $capacity) {
        throw ("Capacity '$CapacityName' not found. Confirm it exists and that you " +
               "have access to it.")
    }
    Write-Ok "   Found capacity: $CapacityName ($($capacity.id))"

    Write-Info "   Checking for workspace: $WorkspaceName"
    $workspace = Get-FabricWorkspace -Name $WorkspaceName

    if ($workspace) {
        $workspaceId = $workspace.id
        Write-Info "   Workspace already exists ($workspaceId)"

        $currentCapacity = if ($workspace.PSObject.Properties.Name -contains "capacityId") {
            $workspace.capacityId
        }
        else { $null }

        if ($currentCapacity -eq $capacity.id) {
            Write-Ok "   Already assigned to the target capacity"
        }
        else {
            Write-Info "   Assigning workspace to capacity"
            try {
                Set-FabricWorkspaceCapacity -WorkspaceId $workspaceId -CapacityId $capacity.id
                Write-Ok "   Assigned to capacity"
            }
            catch {
                # The assignment call can report failure after it has actually
                # taken effect, so verify before treating it as fatal.
                $refreshed = Get-FabricWorkspace -Name $WorkspaceName
                if ($refreshed -and $refreshed.capacityId -eq $capacity.id) {
                    Write-Warn "   Assignment reported an error but the capacity is correct; continuing"
                }
                else {
                    throw
                }
            }
        }
    }
    else {
        Write-Info "   Creating workspace: $WorkspaceName"
        $workspaceId = New-FabricWorkspace -Name $WorkspaceName
        Write-Ok "   Created workspace ($workspaceId)"

        Write-Info "   Assigning workspace to capacity"
        Set-FabricWorkspaceCapacity -WorkspaceId $workspaceId -CapacityId $capacity.id
        Write-Ok "   Assigned to capacity"
    }

    return $workspaceId
}


# ---------------------------------------------------------------------------
# Workspace administrators
# ---------------------------------------------------------------------------

function Get-FabricRoleAssignment {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$WorkspaceId)

    $response = Invoke-FabricApi -Uri "workspaces/$WorkspaceId/roleAssignments"
    return $response.value
}


function Resolve-GraphPrincipal {
    <#
    .SYNOPSIS
        Resolve an identity to its object ID and principal type.

    .DESCRIPTION
        Accepts a user principal name, an object ID, or an application ID.
        Returns $null when Graph cannot resolve it, which is not fatal: an
        object ID can still be used directly, and tenants often restrict Graph.
    #>
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Identity)

    $isGuid = [guid]::TryParse($Identity, [ref]([guid]::Empty))

    # A user principal name
    if ($Identity -like "*@*") {
        try {
            $user = Invoke-FabricApi -Uri "users/$Identity" -Resource Graph
            return [pscustomobject]@{
                Type = "User"; ObjectId = $user.id
                DisplayName = $user.displayName; UserPrincipalName = $user.userPrincipalName
                AppId = $null
            }
        }
        catch { return $null }
    }

    if (-not $isGuid) { return $null }

    # A GUID could be a user object ID, a service principal object ID, or an appId.
    foreach ($probe in @(
            @{ Uri = "users/$Identity"; Type = "User" },
            @{ Uri = "servicePrincipals/$Identity"; Type = "ServicePrincipal" },
            @{ Uri = "servicePrincipals(appId='$Identity')"; Type = "ServicePrincipal" }
        )) {
        try {
            $found = Invoke-FabricApi -Uri $probe.Uri -Resource Graph
            return [pscustomobject]@{
                Type              = $probe.Type
                ObjectId          = $found.id
                DisplayName       = $found.displayName
                UserPrincipalName = if ($found.PSObject.Properties.Name -contains "userPrincipalName") { $found.userPrincipalName } else { $null }
                AppId             = if ($found.PSObject.Properties.Name -contains "appId") { $found.appId } else { $null }
            }
        }
        catch { continue }
    }

    return $null
}


function Add-FabricWorkspaceAdmin {
    <#
    .SYNOPSIS
        Grant one identity the Admin role on a workspace.

    .DESCRIPTION
        Already-assigned identities are skipped. When Graph cannot determine the
        principal type, a ServicePrincipal assignment is attempted first and a
        User assignment second, matching the behaviour of the original installer.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$Identity,
        [System.Collections.Generic.HashSet[string]]$ExistingPrincipals
    )

    if ($ExistingPrincipals -and $ExistingPrincipals.Contains($Identity.ToLowerInvariant())) {
        Write-Info "      skipped (already an administrator): $Identity"
        return "Skipped"
    }

    $principal = Resolve-GraphPrincipal -Identity $Identity
    $objectId = if ($principal) { $principal.ObjectId } else { $Identity }

    if ($ExistingPrincipals -and $ExistingPrincipals.Contains($objectId.ToLowerInvariant())) {
        Write-Info "      skipped (already an administrator): $Identity"
        return "Skipped"
    }

    if (-not [guid]::TryParse($objectId, [ref]([guid]::Empty))) {
        Write-Warn ("      cannot resolve '$Identity' to an object ID. Supply the Entra " +
                    "object ID instead if Graph access is restricted.")
        return "Failed"
    }

    # Candidate shapes to try, most likely first.
    $candidates = @()
    if ($principal -and $principal.Type -eq "User") {
        $candidates += @{ type = "User"; upn = $principal.UserPrincipalName }
    }
    elseif ($principal -and $principal.Type -eq "ServicePrincipal") {
        $candidates += @{ type = "ServicePrincipal"; appId = $principal.AppId }
    }
    else {
        $candidates += @{ type = "ServicePrincipal"; appId = $null }
        $candidates += @{ type = "User"; upn = $(if ($Identity -like "*@*") { $Identity } else { $null }) }
    }

    foreach ($candidate in $candidates) {
        $body = @{
            principal = @{ id = $objectId; type = $candidate.type }
            role      = "Admin"
        }
        if ($principal -and $principal.DisplayName) {
            $body.principal.displayName = $principal.DisplayName
        }
        if ($candidate.type -eq "User" -and $candidate.upn) {
            $body.principal.userDetails = @{ userPrincipalName = $candidate.upn }
        }
        if ($candidate.type -eq "ServicePrincipal" -and $candidate.appId) {
            $body.principal.servicePrincipalDetails = @{ aadAppId = $candidate.appId }
        }

        try {
            Invoke-FabricApi -Uri "workspaces/$WorkspaceId/roleAssignments" -Method POST `
                -Body $body | Out-Null
            Write-Ok "      added as $($candidate.type): $Identity"
            if ($ExistingPrincipals) {
                [void]$ExistingPrincipals.Add($objectId.ToLowerInvariant())
                [void]$ExistingPrincipals.Add($Identity.ToLowerInvariant())
            }
            return "Added"
        }
        catch {
            # A conflict means somebody else already holds the role.
            if ($_.Exception.Message -match "409|already exists|PrincipalAlreadyHasRole") {
                Write-Info "      skipped (already an administrator): $Identity"
                return "Skipped"
            }
            continue
        }
    }

    Write-Warn "      could not add '$Identity' as an administrator"
    return "Failed"
}


function Set-FabricWorkspaceAdmins {
    <#
    .SYNOPSIS
        Grant the Admin role to each supplied identity, skipping duplicates.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [string[]]$Administrators
    )

    if (-not $Administrators -or $Administrators.Count -eq 0) {
        Write-Info "   No additional administrators requested"
        return
    }

    $existing = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase)

    try {
        foreach ($assignment in (Get-FabricRoleAssignment -WorkspaceId $WorkspaceId)) {
            if ($assignment.role -ne "Admin") { continue }
            if ($assignment.principal.id) {
                [void]$existing.Add($assignment.principal.id.ToLowerInvariant())
            }
            $details = $assignment.principal.PSObject.Properties.Name
            if ($details -contains "userDetails" -and $assignment.principal.userDetails.userPrincipalName) {
                [void]$existing.Add($assignment.principal.userDetails.userPrincipalName.ToLowerInvariant())
            }
        }
        Write-Info "   $($existing.Count) existing administrator identity/identities found"
    }
    catch {
        Write-Warn "   Could not list existing role assignments; duplicates may be attempted"
    }

    $added = 0; $skipped = 0; $failed = 0
    foreach ($identity in $Administrators) {
        switch (Add-FabricWorkspaceAdmin -WorkspaceId $WorkspaceId -Identity $identity `
                -ExistingPrincipals $existing) {
            "Added" { $added++ }
            "Skipped" { $skipped++ }
            default { $failed++ }
        }
    }

    Write-Info "   administrators: $added added, $skipped already present, $failed failed"
    if ($failed -gt 0) {
        Write-Warn "   Workspace creation succeeded; add the failed identities manually if needed."
    }
}


# ---------------------------------------------------------------------------
# Lakehouses
# ---------------------------------------------------------------------------

function Get-FabricLakehouse {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$Name
    )

    $items = @((Invoke-FabricApi -Uri "workspaces/$WorkspaceId/lakehouses").value)
    return $items | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
}

function New-FabricLakehouse {
    <#
    .SYNOPSIS
        Create a lakehouse, or return the existing one of the same name.

    .PARAMETER EnableSchemas
        Create a schema-enabled lakehouse. Schemas are NOT enabled by default
        through the REST API, and the MX notebooks organise every table under a
        schema (bronze, hl7, ccda, mapping, dq). Without this, the first
        CREATE SCHEMA fails and the whole pipeline dies with a generic
        "System cancelled the Spark session" error.

        This cannot be changed after creation.
    #>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$Name,
        [switch]$EnableSchemas
    )

    $existing = Get-FabricLakehouse -WorkspaceId $WorkspaceId -Name $Name
    if ($existing) {
        if ($EnableSchemas) {
            # defaultSchema is only present on schema-enabled lakehouses.
            $detail = Invoke-FabricApi -Uri "workspaces/$WorkspaceId/lakehouses/$($existing.id)"
            $hasSchemas = $detail.PSObject.Properties.Name -contains "properties" -and
                          $detail.properties.PSObject.Properties.Name -contains "defaultSchema"
            if (-not $hasSchemas) {
                throw ("Lakehouse '$Name' already exists but is not schema-enabled, and that " +
                       "cannot be changed after creation. The MX notebooks organise tables " +
                       "under schemas, so delete it in the workspace and re-run to have it " +
                       "recreated correctly.")
            }
        }
        Write-Ok "      $Name already exists ($($existing.id))"
        return $existing.id
    }

    if (-not $PSCmdlet.ShouldProcess($Name, "Create lakehouse")) { return }

    $body = @{ displayName = $Name }
    if ($EnableSchemas) { $body.creationPayload = @{ enableSchemas = $true } }

    Invoke-FabricApi -Uri "workspaces/$WorkspaceId/lakehouses" -Method POST -Body $body | Out-Null

    $created = Get-FabricLakehouse -WorkspaceId $WorkspaceId -Name $Name
    if (-not $created) { throw "Lakehouse '$Name' was not found after creation" }

    $suffix = if ($EnableSchemas) { " (schema-enabled)" } else { "" }
    Write-Ok "      created $Name$suffix ($($created.id))"
    return $created.id
}

# ---------------------------------------------------------------------------
# Item definitions
# ---------------------------------------------------------------------------

function New-FabricItemPart {
    <#
    .SYNOPSIS
        Build one InlineBase64 definition part from text content.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][AllowEmptyString()][string]$Content
    )

    return @{
        path        = $Path
        payload     = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Content))
        payloadType = "InlineBase64"
    }
}

function Get-FabricItemByName {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Type
    )

    $items = @((Invoke-FabricApi -Uri "workspaces/$WorkspaceId/items?type=$Type").value)
    return $items | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
}

function Publish-FabricItemDefinition {
    <#
    .SYNOPSIS
        Create or update a Fabric item from a set of definition parts.

    .DESCRIPTION
        Creates the item when it does not exist, and replaces its definition
        when it does, so the whole deployment is re-runnable.

    .PARAMETER Format
        Definition format. Notebooks use "fabricGitSource"; semantic models,
        reports and data pipelines take no format and are omitted.
    #>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][ValidateSet("Notebook", "SemanticModel", "Report", "DataPipeline")][string]$Type,
        [Parameter(Mandatory)][array]$Parts,
        [string]$Format
    )

    $definition = @{ parts = $Parts }
    if ($Format) { $definition.format = $Format }

    $existing = Get-FabricItemByName -WorkspaceId $WorkspaceId -Name $Name -Type $Type

    if ($existing) {
        if (-not $PSCmdlet.ShouldProcess($Name, "Update $Type definition")) { return $existing.id }

        # updateMetadata=true is rejected unless a .platform part is supplied,
        # so only ask for it when one is actually present.
        $hasPlatform = @($Parts | Where-Object { $_.path -eq ".platform" }).Count -gt 0
        $uri = "workspaces/$WorkspaceId/items/$($existing.id)/updateDefinition"
        if ($hasPlatform) { $uri += "?updateMetadata=true" }

        Invoke-FabricApi -Uri $uri -Method POST -Body @{ definition = $definition } | Out-Null
        Write-Ok "      updated $Name"
        return $existing.id
    }

    if (-not $PSCmdlet.ShouldProcess($Name, "Create $Type")) { return }

    Invoke-FabricApi -Uri "workspaces/$WorkspaceId/items" -Method POST -Body @{
        displayName = $Name
        type        = $Type
        definition  = $definition
    } | Out-Null

    $created = Get-FabricItemByName -WorkspaceId $WorkspaceId -Name $Name -Type $Type
    if (-not $created) { throw "$Type '$Name' was not found after creation" }

    Write-Ok "      created $Name"
    return $created.id
}


# ---------------------------------------------------------------------------
# OneLake file upload
# ---------------------------------------------------------------------------

function Send-OneLakeFile {
    <#
    .SYNOPSIS
        Upload one file into a lakehouse's Files area.

    .DESCRIPTION
        OneLake exposes the ADLS Gen2 DFS API, so an upload is create, append,
        then flush. Content is sent as raw bytes: HL7 v2 uses bare CR segment
        terminators, which any text-mode handling would corrupt.
    #>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId,
        [Parameter(Mandatory)][string]$RelativePath,
        [Parameter(Mandatory)][byte[]]$Content
    )

    $encoded = ($RelativePath -split '/' | ForEach-Object { [Uri]::EscapeDataString($_) }) -join '/'
    $base = "https://onelake.dfs.fabric.microsoft.com/$WorkspaceId/$LakehouseId/Files/$encoded"

    if (-not $PSCmdlet.ShouldProcess($RelativePath, "Upload to OneLake")) { return }

    $headers = @{ Authorization = "Bearer $(Get-FabricAccessToken -Resource Storage)" }

    # Create (truncates any existing file), append, then commit. The create
    # call carries no body; Invoke-WebRequest sets Content-Length: 0 itself.
    Invoke-WebRequest -Uri "${base}?resource=file" -Method PUT -Headers $headers `
        -ErrorAction Stop | Out-Null

    if ($Content.Length -gt 0) {
        Invoke-WebRequest -Uri "${base}?action=append&position=0" -Method PATCH -Headers $headers `
            -Body $Content -ContentType "application/octet-stream" -ErrorAction Stop | Out-Null
    }

    Invoke-WebRequest -Uri "${base}?action=flush&position=$($Content.Length)" -Method PATCH `
        -Headers $headers -ErrorAction Stop | Out-Null
}


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

function Start-FabricItemJob {
    <#
    .SYNOPSIS
        Run a Fabric item as an on-demand job and wait for it to finish.

    .DESCRIPTION
        Transient Spark and managed-VNet errors are retried with exponential
        back-off, matching the original installer's behaviour. Those failures
        are common on a capacity's first Spark session and are not worth
        failing a deployment over.

    .PARAMETER JobType
        Fabric job type. Notebooks use "RunNotebook"; data pipelines use
        "Pipeline".

    .PARAMETER Label
        Noun used in progress messages, e.g. "Notebook" or "Pipeline".
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$ItemId,
        [string]$JobType = "RunNotebook",
        [string]$Label = "Notebook",
        [int]$PollSeconds = 20,
        [int]$TimeoutSeconds = 5400,
        [int]$MaxAttempts = 3,
        [int]$InitialBackoffSeconds = 30
    )

    $transient = @("GetManagedVnetTimeout", "Please retry", "TooManyRequests",
                   "Throttled", "ServiceUnavailable")

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        Write-Info "   Starting $($Label.ToLower()) job (attempt $attempt of $MaxAttempts)"
        try {
            $response = Invoke-FabricApi `
                -Uri "workspaces/$WorkspaceId/items/$ItemId/jobs/instances?jobType=$JobType" `
                -Method POST -Body @{} -RawResponse -NoWaitForLro

            if ($response.StatusCode -notin 200, 201, 202) {
                throw "Unexpected status starting the job: HTTP $($response.StatusCode)"
            }

            $location = $null
            if ($response.Headers -and $response.Headers.ContainsKey("Location")) {
                $location = $response.Headers["Location"] | Select-Object -First 1
            }
            if (-not $location) {
                Write-Ok "   $Label job completed"
                return
            }

            $started = Get-Date
            $deadline = $started.AddSeconds($TimeoutSeconds)
            $lastStatus = ""

            while ((Get-Date) -lt $deadline) {
                Start-Sleep -Seconds $PollSeconds
                $poll = Invoke-FabricApi -Uri $location -RawResponse -NoWaitForLro
                $status = if ($poll.Body -and ($poll.Body.PSObject.Properties.Name -contains "status")) {
                    $poll.Body.status
                }
                else { "Unknown" }

                if ($status -ne $lastStatus) {
                    $elapsed = [int]((Get-Date) - $started).TotalSeconds
                    Write-Info "      $status (${elapsed}s elapsed)"
                    $lastStatus = $status
                }

                if ($status -in "Completed", "Succeeded") {
                    $elapsed = [int]((Get-Date) - $started).TotalSeconds
                    Write-Ok "   $Label job completed in ${elapsed}s"
                    return
                }

                if ($status -in "Failed", "Cancelled", "Deduped") {
                    $detail = if ($poll.Body.PSObject.Properties.Name -contains "failureReason") {
                        $poll.Body.failureReason | ConvertTo-Json -Depth 6 -Compress
                    }
                    else { "no detail available" }
                    throw "$Label job $status`: $detail"
                }
            }

            throw "$Label job did not finish within $TimeoutSeconds seconds."
        }
        catch {
            $message = $_.Exception.Message
            $isTransient = $transient | Where-Object { $message -match [regex]::Escape($_) }

            if ($attempt -lt $MaxAttempts -and $isTransient) {
                $backoff = $InitialBackoffSeconds * [math]::Pow(2, $attempt - 1)
                Write-Warn "   Transient error: $message"
                Write-Warn "   Retrying in $backoff seconds"
                Start-Sleep -Seconds $backoff
                continue
            }
            throw
        }
    }
}


function Start-FabricPipelineJob {
    <#
    .SYNOPSIS
        Run a data pipeline and wait for it to finish.

    .DESCRIPTION
        A pipeline runs every notebook in the chain, so the default timeout is
        longer than a single notebook's.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$PipelineId,
        [int]$PollSeconds = 30,
        [int]$TimeoutSeconds = 10800,
        [int]$MaxAttempts = 2,
        [int]$InitialBackoffSeconds = 30
    )

    Start-FabricItemJob -WorkspaceId $WorkspaceId -ItemId $PipelineId `
        -JobType "Pipeline" -Label "Pipeline" `
        -PollSeconds $PollSeconds -TimeoutSeconds $TimeoutSeconds `
        -MaxAttempts $MaxAttempts -InitialBackoffSeconds $InitialBackoffSeconds
}


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------

function Remove-FabricWorkspace {
    [CmdletBinding(SupportsShouldProcess)]
    param([Parameter(Mandatory)][string]$WorkspaceId)

    if (-not $PSCmdlet.ShouldProcess($WorkspaceId, "Delete Fabric workspace")) { return }
    Invoke-FabricApi -Uri "workspaces/$WorkspaceId" -Method DELETE | Out-Null
}


function ConvertTo-AdministratorList {
    <#
    .SYNOPSIS
        Combine the two administrator environment variables into one list.

    .PARAMETER CapacityAdministratorsJson
        JSON array from AZURE_FABRIC_CAPACITY_ADMINISTRATORS, set by Bicep.

    .PARAMETER WorkspaceAdministratorsCsv
        Comma-separated list from FABRIC_WORKSPACE_ADMINISTRATORS.
    #>
    [CmdletBinding()]
    param(
        [string]$CapacityAdministratorsJson,
        [string]$WorkspaceAdministratorsCsv
    )

    $admins = [System.Collections.Generic.List[string]]::new()

    if ($CapacityAdministratorsJson) {
        try {
            foreach ($a in ($CapacityAdministratorsJson | ConvertFrom-Json)) {
                if ($a) { $admins.Add([string]$a) }
            }
        }
        catch {
            Write-Warn "   AZURE_FABRIC_CAPACITY_ADMINISTRATORS is not valid JSON; ignoring"
        }
    }

    if ($WorkspaceAdministratorsCsv) {
        foreach ($a in ($WorkspaceAdministratorsCsv -split ",")) {
            $trimmed = $a.Trim()
            if ($trimmed) { $admins.Add($trimmed) }
        }
    }

    # Always return an array. PowerShell unrolls a single-element array on
    # return, so a lone administrator would come back as a bare string and
    # callers doing .Count or foreach would get the wrong shape. The unary
    # comma prevents that unrolling.
    return , @($admins | Select-Object -Unique)
}


# ---------------------------------------------------------------------------
# Semantic models
# ---------------------------------------------------------------------------

function Set-FabricSemanticModelStorageFormat {
    <#
    .SYNOPSIS
        Put a semantic model into large semantic model storage format.

    .DESCRIPTION
        Direct Lake framing requires large semantic model storage format
        (targetStorageMode "PremiumFiles"). A model published through the
        Fabric item-definition API does not get it, so the first refresh is
        rejected during validation - which produces no refresh-history entry
        and the unhelpful message "Unable to load a query that produces no
        tables".

        Opening the model in Editing mode in the browser converts it silently,
        which is why the fault disappears as soon as anyone investigates it.
        Setting it here keeps the deployment self-contained instead.

        The call is idempotent, so re-running a deployment is safe.

    .OUTPUTS
        An object carrying the storage format before and after the call, so the
        caller can report whether it actually had to change anything.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$SemanticModelId
    )

    # Only exposed on the Power BI REST surface, not the Fabric one.
    $datasetUri = "groups/$WorkspaceId/datasets/$SemanticModelId"

    $before = $null
    try {
        $dataset = Invoke-FabricApi -Uri $datasetUri -Resource "PowerBI"
        if ($dataset -and $dataset.PSObject.Properties.Name -contains "targetStorageMode") {
            $before = $dataset.targetStorageMode
        }
    }
    catch {
        # Reading it back is diagnostic, not required. Fall through and set it.
        Write-Warn ("      could not read the current storage format: " +
                    (Get-ExceptionChain -Exception $_.Exception))
    }

    if ($before -eq "PremiumFiles") {
        return [pscustomobject]@{ Before = $before; After = $before; Changed = $false }
    }

    Invoke-FabricApi -Uri $datasetUri -Method "PATCH" -Resource "PowerBI" `
        -Body @{ targetStorageMode = "PremiumFiles" } | Out-Null

    # The conversion is asynchronous and reports no operation to poll, so read
    # the value back rather than assuming it took.
    $after = $null
    try {
        $after = (Invoke-FabricApi -Uri $datasetUri -Resource "PowerBI").targetStorageMode
    }
    catch {
        Write-Warn ("      could not confirm the new storage format: " +
                    (Get-ExceptionChain -Exception $_.Exception))
    }

    return [pscustomobject]@{ Before = $before; After = $after; Changed = $true }
}


Export-ModuleMember -Function @(
    "Get-FabricAccessToken", "Invoke-FabricApi", "Wait-FabricLongRunningOperation",
    "Get-FabricCapacity", "Get-FabricWorkspace", "New-FabricWorkspace",
    "Set-FabricWorkspaceCapacity", "Initialize-FabricWorkspace",
    "Get-FabricRoleAssignment", "Resolve-GraphPrincipal",
    "Add-FabricWorkspaceAdmin", "Set-FabricWorkspaceAdmins",
    "Start-FabricItemJob", "Start-FabricPipelineJob",
    "Get-FabricLakehouse", "New-FabricLakehouse",
    "New-FabricItemPart", "Get-FabricItemByName", "Publish-FabricItemDefinition",
    "Set-FabricSemanticModelStorageFormat",
    "Send-OneLakeFile",
    "Remove-FabricWorkspace", "ConvertTo-AdministratorList",
    "Write-Info", "Write-Ok", "Write-Warn", "Write-Err", "Write-Step",
    "Get-ExceptionChain"
)
