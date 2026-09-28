<#
.SYNOPSIS
    Deploy the MX Prism Data Quality solution content into a Fabric workspace,
    directly from this repository.

.DESCRIPTION
    Creates the three medallion lakehouses, uploads every notebook, both
    pipelines, the semantic model and any report, and loads the sample files
    into OneLake. Does not run notebooks or pipelines.

    Everything is read from the local working tree and pushed over the Fabric
    REST API. Nothing is downloaded from GitHub, so the solution never has to
    be published anywhere, and no package is installed on the machine or in
    Fabric.

    Placeholder IDs in the notebook metadata and the semantic model are
    rewritten to the IDs of the items that were just created.

    Existing items are updated in place rather than duplicated. Publication
    replaces remote item definitions and uploaded files, but does not reseed
    mapping tables. Run setup explicitly for a new workspace, then processing.

.PARAMETER WorkspaceId
    Target Fabric workspace.

.PARAMETER RepoRoot
    Repository root. Defaults to three levels above this script.

.PARAMETER SkipSampleData
    Do not upload the sample HL7 v2, CCDA and configuration files.

.PARAMETER SkipPipeline
    Legacy compatibility switch. Redundant: deployment never runs a pipeline.

.NOTES
    Requires PowerShell 7+ and the Azure CLI.
#>
[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)][string]$WorkspaceId,
    [string]$RepoRoot,
    [switch]$SkipSampleData,
    [switch]$SkipPipeline
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

Import-Module (Join-Path $PSScriptRoot "FabricApi.psm1") -Force

if (-not $RepoRoot) {
    $RepoRoot = Split-Path (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent) -Parent
}

$workspaceRoot = Join-Path $RepoRoot "fabric_workspace"
$samplesRoot = Join-Path $RepoRoot "infra/data/samples_mx"

if (-not (Test-Path $workspaceRoot)) {
    throw "fabric_workspace not found under '$RepoRoot'. Pass -RepoRoot explicitly."
}

# Placeholder IDs baked into the committed items, rebound below.
$PLACEHOLDER_WORKSPACE = "1a2b3c4d-0000-4000-8000-000000000000"
$PLACEHOLDER_LAKEHOUSE = [ordered]@{
    mx_bronze = "1a2b3c4d-0001-4000-8000-000000000001"
    mx_silver = "1a2b3c4d-0002-4000-8000-000000000002"
    mx_gold   = "1a2b3c4d-0003-4000-8000-000000000003"
}

# The setup and processing pipelines reference these notebooks by placeholder
# IDs, rebound below to the IDs returned when the notebooks are published.
# Keep the original numbering; it no longer represents one execution order.
#
# Never chain notebooks with mssparkutils.notebook.run(): a nested run shares
# the parent's Spark session, and when a child fails the session is cancelled
# outright rather than raising, so the failure cannot be caught, attributed or
# retried. Separate pipeline activities give each notebook a clean session and
# name the failing step.
$PLACEHOLDER_NOTEBOOK = [ordered]@{
    schema_model_bronze = "1a2b3c4d-1001-4000-8000-000000001001"
    ingest_raw_files    = "1a2b3c4d-1002-4000-8000-000000001002"
    schema_model_silver = "1a2b3c4d-1003-4000-8000-000000001003"
    seed_mapping_tables = "1a2b3c4d-1004-4000-8000-000000001004"
    silver_parse_hl7    = "1a2b3c4d-1005-4000-8000-000000001005"
    silver_parse_ccda   = "1a2b3c4d-1006-4000-8000-000000001006"
    schema_model_gold   = "1a2b3c4d-1007-4000-8000-000000001007"
    gold_score_rules    = "1a2b3c4d-1008-4000-8000-000000001008"
    refresh_semantic_model = "1a2b3c4d-1009-4000-8000-000000001009"
}


function Get-ItemDisplayName {
    <# Read the display name from an item's .platform file. #>
    param([string]$ItemFolder)

    $platformPath = Join-Path $ItemFolder ".platform"
    if (Test-Path $platformPath) {
        $platform = Get-Content $platformPath -Raw | ConvertFrom-Json
        if ($platform.metadata.displayName) { return $platform.metadata.displayName }
    }
    # Fall back to the folder name with its type suffix removed.
    return [IO.Path]::GetFileNameWithoutExtension((Split-Path $ItemFolder -Leaf))
}

function Get-ItemParts {
    <#
        Build definition parts for every file in an item folder, applying a
        text replacement map. .platform is included so the item keeps its
        logical ID and display name.
    #>
    param(
        [string]$ItemFolder,
        [hashtable]$Replacements = @{},
        [string[]]$Exclude = @()
    )

    $parts = @()
    $prefix = (Resolve-Path $ItemFolder).Path.TrimEnd('\', '/')

    foreach ($file in Get-ChildItem $ItemFolder -Recurse -File) {
        $relative = $file.FullName.Substring($prefix.Length + 1) -replace '\\', '/'
        if ($Exclude -contains $relative) { continue }

        # Power BI Desktop and the report preview drop a local cache in .pbi/.
        # It is machine-local, is git-ignored, and is not part of the item, so
        # uploading it would fail the import. .platform is the one dot-file the
        # item does need.
        if ($relative -like ".pbi/*") { continue }

        $content = Get-Content $file.FullName -Raw -Encoding UTF8
        if ($null -eq $content) { $content = "" }

        foreach ($key in $Replacements.Keys) {
            $content = $content.Replace($key, $Replacements[$key])
        }

        $parts += New-FabricItemPart -Path $relative -Content $content
    }

    return $parts
}


$started = Get-Date
$script:reportFailed = $false
Write-Host ""
Write-Host "Deploying MX solution content from the local repository" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Cyan
Write-Host "  source    : $RepoRoot"
Write-Host "  workspace : $WorkspaceId"

# ---------------------------------------------------------------------------
Write-Step 1 6 "Create the medallion lakehouses"

$lakehouseIds = [ordered]@{}
foreach ($name in $PLACEHOLDER_LAKEHOUSE.Keys) {
    # The notebooks organise every table under a schema, so the lakehouses must
    # be schema-enabled. fabric_workspace/lakehouses/*/lakehouse.metadata.json
    # records this as {"defaultSchema": "dbo"}.
    $lakehouseIds[$name] = New-FabricLakehouse -WorkspaceId $WorkspaceId -Name $name -EnableSchemas
}

# Notebook metadata, the pipelines' workspace references and the semantic model's
# OneLake path all bind to lakehouses and the workspace by ID.
$idReplacements = @{ $PLACEHOLDER_WORKSPACE = $WorkspaceId }
foreach ($name in $PLACEHOLDER_LAKEHOUSE.Keys) {
    $idReplacements[$PLACEHOLDER_LAKEHOUSE[$name]] = $lakehouseIds[$name]
}

# ---------------------------------------------------------------------------
Write-Step 2 6 "Upload the notebooks"

$notebookFolders = @(Get-ChildItem (Join-Path $workspaceRoot "notebooks") -Directory -Recurse |
    Where-Object { $_.Name -like "*.Notebook" } | Sort-Object Name)

$notebookIds = @{}
Write-Info "   $($notebookFolders.Count) notebook(s)"
foreach ($folder in $notebookFolders) {
    $name = Get-ItemDisplayName -ItemFolder $folder.FullName
    if ($notebookIds.ContainsKey($name)) {
        throw "Duplicate notebook display name '$name'. Each pipeline reference must resolve to one notebook."
    }
    $parts = Get-ItemParts -ItemFolder $folder.FullName -Replacements $idReplacements
    $notebookIds[$name] = Publish-FabricItemDefinition -WorkspaceId $WorkspaceId -Name $name `
        -Type "Notebook" -Parts $parts -Format "fabricGitSource"
}

# ---------------------------------------------------------------------------
Write-Step 3 6 "Upload the setup and processing pipelines"

# The pipeline names its notebooks by ID, so it can only be published after
# them. A placeholder that survived to this point would produce a pipeline
# that imports cleanly and then fails at run time against an item that does
# not exist, so an unresolved name is a hard error.
$pipelineReplacements = @{ $PLACEHOLDER_WORKSPACE = $WorkspaceId }
foreach ($name in $PLACEHOLDER_NOTEBOOK.Keys) {
    if (-not $notebookIds[$name]) {
        throw ("The pipeline references notebook '$name', which was not published. " +
               "Expected fabric_workspace/notebooks/**/$name.Notebook.")
    }
    $pipelineReplacements[$PLACEHOLDER_NOTEBOOK[$name]] = $notebookIds[$name]
}

$pipelineFolders = @(Get-ChildItem (Join-Path $workspaceRoot "pipelines") -Directory |
    Where-Object { $_.Name -like "*.DataPipeline" } | Sort-Object Name)
$pipelines = [ordered]@{}
foreach ($pipelineFolder in $pipelineFolders) {
    $pipelineName = Get-ItemDisplayName -ItemFolder $pipelineFolder.FullName
    if ($pipelines.Contains($pipelineName)) {
        throw "Duplicate pipeline display name '$pipelineName'. Each pipeline must have a unique name."
    }
    $pipelineParts = @(Get-ItemParts -ItemFolder $pipelineFolder.FullName `
        -Replacements $pipelineReplacements)
    $contentParts = @($pipelineParts | Where-Object path -eq "pipeline-content.json")
    if ($contentParts.Count -ne 1) {
        throw "Pipeline '$pipelineName' must contain exactly one pipeline-content.json."
    }
    $content = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($contentParts[0].payload))
    if ($content -match '1a2b3c4d-') {
        throw "Pipeline '$pipelineName' contains an unresolved placeholder ID."
    }
    $pipeline = $content | ConvertFrom-Json
    foreach ($activity in $pipeline.properties.activities) {
        if ($activity.type -eq "TridentNotebook" -and
            ($activity.typeProperties.workspaceId -ne $WorkspaceId -or
             $activity.typeProperties.notebookId -notin $notebookIds.Values)) {
            throw "Pipeline '$pipelineName' activity '$($activity.name)' references an unpublished notebook or a different workspace."
        }
    }
    $pipelines[$pipelineName] = $pipelineParts
}
foreach ($requiredName in @("mx_setup_pipeline", "mx_dq_pipeline")) {
    if (-not $pipelines.Contains($requiredName)) {
        throw "Required pipeline '$requiredName' not found under fabric_workspace/pipelines."
    }
}

foreach ($pipelineName in $pipelines.Keys) {
    # A data pipeline definition carries no format, unlike a notebook or report.
    Publish-FabricItemDefinition -WorkspaceId $WorkspaceId -Name $pipelineName `
        -Type "DataPipeline" -Parts $pipelines[$pipelineName] | Out-Null
    Write-Ok "      $pipelineName"
}

# ---------------------------------------------------------------------------
Write-Step 4 6 "Upload the semantic model and report"

$modelFolder = Get-ChildItem (Join-Path $workspaceRoot "reports") -Directory |
    Where-Object { $_.Name -like "*.SemanticModel" } | Select-Object -First 1
$reportFolder = Get-ChildItem (Join-Path $workspaceRoot "reports") -Directory |
    Where-Object { $_.Name -like "*.Report" } | Select-Object -First 1

$semanticModelId = $null

if ($modelFolder) {
    # The model is Direct Lake: definition/expressions.tmdl reaches the Delta
    # files in mx_gold over OneLake, so there is no SQL analytics endpoint to
    # wait for. It addresses them by workspace and lakehouse ID, which are the
    # same placeholders the notebooks use.
    $modelName = Get-ItemDisplayName -ItemFolder $modelFolder.FullName
    $modelParts = Get-ItemParts -ItemFolder $modelFolder.FullName `
        -Replacements $idReplacements

    $semanticModelId = Publish-FabricItemDefinition -WorkspaceId $WorkspaceId `
        -Name $modelName -Type "SemanticModel" -Parts $modelParts
    Write-Ok "      $modelName"

    # Direct Lake framing requires large semantic model storage format. A model
    # published over REST does not get it, and the first refresh is then
    # rejected in validation with "Unable to load a query that produces no
    # tables" - and no refresh-history entry to explain why. Opening the model
    # in Editing mode in the browser converts it silently, which is what makes
    # this so easy to misdiagnose.
    try {
        $storage = Set-FabricSemanticModelStorageFormat -WorkspaceId $WorkspaceId `
            -SemanticModelId $semanticModelId

        if (-not $storage.Changed) {
            Write-Info "      storage format already large ($($storage.After))"
        }
        else {
            Write-Ok ("      storage format set to large (was " +
                      "$(if ($storage.Before) { $storage.Before } else { 'unreported' }), " +
                      "now $(if ($storage.After) { $storage.After } else { 'unconfirmed' }))")
        }
    }
    catch {
        # Not fatal: the model is published and can be converted by hand.
        Write-Warn ("      could not set large semantic model storage format: " +
                    (Get-ExceptionChain -Exception $_.Exception))
        Write-Warn "      if the model will not refresh, open it in the workspace, choose"
        Write-Warn "      Settings, and enable Large semantic model storage format."
    }

    # Framing does not happen here, and cannot: gold tables do not exist until
    # mx_setup_pipeline runs, and are empty until mx_dq_pipeline runs. A model
    # published over REST has never been framed, and until it has been, the
    # refresh path the workspace Refresh button uses is rejected in validation
    # with "Unable to load a query that produces no tables" and writes no
    # refresh-history entry at all.
    #
    # The last activity in mx_dq_pipeline frames it, on every run, once the
    # gold tables it reads are written. See the refresh_semantic_model
    # notebook for the detail.
    Write-Info "      mx_dq_pipeline frames this model after it populates mx_gold"
}

if ($reportFolder -and $semanticModelId) {
    $reportName = Get-ItemDisplayName -ItemFolder $reportFolder.FullName

    # Git integration exports a byPath reference to the sibling model folder.
    # The REST API has no folder context and requires byConnection, so the
    # reference is rewritten to point at the model that was just created.
    #
    # The shape depends on the definitionProperties schema version the report
    # was authored against. This repository uses 1.0.0, which needs the full
    # XMLA-style property set rather than the bare connection string that
    # 2.0.0 accepts.
    $existingPbir = Get-Content (Join-Path $reportFolder.FullName "definition.pbir") -Raw | ConvertFrom-Json
    $schemaUrl = $existingPbir.'$schema'
    $pbirVersion = $existingPbir.version

    $workspaceName = (Invoke-FabricApi -Uri "workspaces/$WorkspaceId").displayName

    if ($schemaUrl -match "definitionProperties/2\.") {
        $reference = [ordered]@{ connectionString = "semanticmodelid=$semanticModelId" }
    }
    else {
        $reference = [ordered]@{
            connectionString          = ("Data Source=powerbi://api.powerbi.com/v1.0/myorg/$workspaceName;" +
                                         "Initial Catalog=$modelName;Integrated Security=ClaimsToken")
            pbiServiceModelId         = $null
            pbiModelVirtualServerName = "sobe_wowvirtualserver"
            pbiModelDatabaseName      = $semanticModelId
            connectionType            = "pbiServiceXmlaStyleLive"
            name                      = "EntityDataSource"
        }
    }

    $pbir = [ordered]@{
        '$schema'        = $schemaUrl
        version          = $pbirVersion
        datasetReference = @{ byConnection = $reference }
    } | ConvertTo-Json -Depth 10

    $reportParts = Get-ItemParts -ItemFolder $reportFolder.FullName -Exclude @("definition.pbir")
    $reportParts += New-FabricItemPart -Path "definition.pbir" -Content $pbir

    # A report is either PBIR (a definition/ folder of pages and visuals) or
    # PBIR-Legacy (a single report.json). The API defaults to PBIR, so a legacy
    # report must say so or the import fails looking for a folder that is not
    # there - reported only as "Report Workload failed to import the report".
    $reportFormat = if (Test-Path (Join-Path $reportFolder.FullName "report.json")) {
        "PBIR-Legacy"
    }
    else { "PBIR" }

    Write-Info "      report format: $reportFormat"

    # The report is presentation over the gold tables, so a failure here must
    # not strand the lakehouses, notebooks, model and data that the solution
    # actually runs on. Report it and carry on.
    try {
        Publish-FabricItemDefinition -WorkspaceId $WorkspaceId -Name $reportName `
            -Type "Report" -Parts $reportParts -Format $reportFormat | Out-Null
    }
    catch {
        Write-Warn "      report import failed: $(Get-ExceptionChain -Exception $_.Exception)"
        Write-Warn "      The semantic model deployed, so the report can be rebuilt against"
        Write-Warn "      it in Power BI, or redeployed once its definition is corrected."
        $script:reportFailed = $true
    }
}
elseif ($reportFolder) {
    Write-Warn "      Skipping the report: no semantic model was deployed to bind it to."
}
else {
    # Only reached if fabric_workspace/reports/*.Report has been removed.
    Write-Info "      no report to upload - see 'The report' in docs/Deploy.md"
}

# ---------------------------------------------------------------------------
Write-Step 5 6 "Load the sample data into mx_bronze"

if ($SkipSampleData) {
    Write-Warn "   Skipped (-SkipSampleData)."
}
elseif (-not (Test-Path $samplesRoot)) {
    Write-Warn "   No sample data found at $samplesRoot."
}
else {
    # Only material the pipeline actually reads is uploaded. Excluded:
    #   _generators/  the sample generator and its verifier (developer tools)
    #   _templates/   hand-authored source messages the generator expands
    #   full_catalog/ reference catalogues, if one is added locally - never seeded
    $files = Get-ChildItem $samplesRoot -Recurse -File |
        Where-Object {
            $_.FullName -notmatch '[\\/]_generators[\\/]' -and
            $_.FullName -notmatch '[\\/]_templates[\\/]' -and
            $_.FullName -notmatch '[\\/]full_catalog[\\/]'
        }

    $prefix = (Resolve-Path $samplesRoot).Path.TrimEnd('\', '/')
    Write-Info "   $($files.Count) file(s) to upload"
    $uploaded = 0

    foreach ($file in $files) {
        $relative = "samples_mx/" + ($file.FullName.Substring($prefix.Length + 1) -replace '\\', '/')
        # Raw bytes: HL7 v2 uses bare CR terminators that text mode would rewrite.
        $bytes = [IO.File]::ReadAllBytes($file.FullName)
        Send-OneLakeFile -WorkspaceId $WorkspaceId -LakehouseId $lakehouseIds["mx_bronze"] `
            -RelativePath $relative -Content $bytes
        $uploaded++
        if ($uploaded % 10 -eq 0) { Write-Info "      $uploaded/$($files.Count)" }
    }

    Write-Ok "   uploaded $uploaded file(s) to mx_bronze/Files/samples_mx"
}

# ---------------------------------------------------------------------------
Write-Step 6 6 "Next steps - run explicitly in Fabric"

if ($SkipPipeline) {
    Write-Info "   -SkipPipeline is retained for compatibility; deployment never runs either pipeline."
}
Write-Info "   No notebooks or pipelines were run by deployment."
Write-Info "   New workspace: run mx_setup_pipeline once, then mx_dq_pipeline."
Write-Info "   Initialized workspace: run mx_dq_pipeline for routine processing."
Write-Warn "   Setup OVERWRITES all four mapping tables from uploaded configuration CSVs."
Write-Warn "   Review table-only edits before reseeding. Never run setup and processing concurrently."

$elapsed = [int]((Get-Date) - $started).TotalSeconds
Write-Host ""
Write-Ok ("Solution content deployed in {0}m {1}s" -f [int]($elapsed / 60), ($elapsed % 60))

if ($script:reportFailed) {
    Write-Warn "The report did not import. Everything else deployed."
}
