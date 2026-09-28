# Deploying the solution

This guide takes you from an empty Azure subscription to a working Fabric
workspace with sample data scored against quality rules and a Power BI report
over the results.

Everything is pushed from your local working tree over the Fabric REST API.
Nothing is downloaded from GitHub, so the repository can stay private.

- [Prerequisites](#prerequisites)
- [What you end up with](#what-you-end-up-with)
- [Deploying with azd up](#deploying-with-azd-up)
- [Environment variables](#environment-variables)
- [What the installer does](#what-the-installer-does)
- [Verifying the deployment](#verifying-the-deployment)
- [The report](#the-report)
- [Connecting an Amazon S3 bucket](#connecting-an-amazon-s3-bucket)
- [Troubleshooting](#troubleshooting)
- [Tearing down](#tearing-down)

---

## Prerequisites

### Tools

| Tool | Minimum | Required | Install |
| --- | --- | --- | --- |
| PowerShell 7+ (`pwsh`) | 7.0.0 | Yes | <https://learn.microsoft.com/powershell/scripting/install/installing-powershell> |
| Azure CLI (`az`) | 2.60.0 | Yes | <https://learn.microsoft.com/cli/azure/install-azure-cli> |
| Azure Developer CLI (`azd`) | 1.17.2 | Yes | <https://learn.microsoft.com/azure/developer/azure-developer-cli/install-azd> |
| Python 3.10+ | 3.10.0 | No | Only needed to regenerate the sample corpus — <https://www.python.org/downloads/> |
| Git | 2.30.0 | No | Only needed to clone or contribute — <https://git-scm.com/downloads> |

> `azd` **1.23.9 specifically is blocked** by `requiredVersions` in
> [`azure.yaml`](../azure.yaml). Any other version above 1.17.1 is fine.

Nothing is installed from PyPI. Every Python file in this repository uses only
the standard library, and the notebooks add `pyspark` and `sempy`, both supplied
by the Fabric runtime.

### Permissions

| Where | What you need |
| --- | --- |
| Azure subscription | Permission to create a resource group and a Microsoft Fabric capacity, and to register the `Microsoft.Fabric` resource provider |
| Microsoft Fabric | A Fabric-enabled tenant, and permission to create workspaces |

### Pre-flight check

Run the read-only pre-flight before you deploy. It changes nothing and is safe
to re-run:

```powershell
./infra/scripts/utils/Test-Prerequisites.ps1
```

It checks six things:

1. The five tools above, and their versions
2. That you are signed in to the Azure CLI and a subscription resolves
3. That the `Microsoft.Fabric` resource provider is registered
4. That the capacity SKU is available in your region
5. Whether you already have Fabric capacities you could reuse
6. Your subscription-scope role assignments

Its parameters — `-Location` (default `eastus2`), `-Sku` (default `F64`) and
`-Subscription` — let you check a region or SKU other than the defaults.

---

## What you end up with

| Item | Name | Notes |
| --- | --- | --- |
| Resource group | `rg-<AZURE_ENV_NAME>` | Created by `azd` |
| Fabric capacity | `<generated>` | F64 by default; you can reuse an existing one |
| Fabric workspace | `mx-fabric-dq-poc-<suffix>` | Bound to the capacity |
| Lakehouses | `mx_bronze`, `mx_silver`, `mx_gold` | Schema-enabled |
| Notebooks | 9 | Setup: three schema notebooks and the mapping seed. Processing: ingest, two parsers, scoring and model refresh |
| Data pipelines | `mx_setup_pipeline`, `mx_dq_pipeline` | Setup once, then processing routinely |
| Semantic model | `mx_data_quality` | Direct Lake over `mx_gold`, large storage format |
| Report | `mx_data_quality` | Four pages over the semantic model |
| Sample files | 154 | 126 HL7, 24 CCDA, 4 configuration CSVs |

The workspace also gets two maintenance notebooks — `drop_all_tables` and
`truncate_all_tables` — that are deliberately **not** in the pipeline.

---

## Deploying with azd up

### 1. Sign in

```powershell
az login
azd auth login
```

Both are needed: `azd` provisions, and the Fabric installer calls the Azure CLI
to read your tenant and subscription.

### 2. Create an azd environment

```powershell
azd env new mx-poc
```

An `azd` environment is a named bag of settings and outputs, stored under
`.azure/mx-poc/`. **Create it before anything else** — every `azd env set` below
writes into the current environment, and with none selected those commands fail.
This command does not prompt for subscription or location; those come from the
Azure CLI/`azd` context or explicit environment settings.

Useful companions:

| Command | Purpose |
| --- | --- |
| `azd env list` | Show environments and which one is active |
| `azd env select <name>` | Switch the active environment |
| `azd env get-values` | Print everything the current environment holds |
| `azd env remove <name>` | Delete the environment's local settings (add `--force` to skip the prompt) |

`azd env remove` only deletes the folder under `.azure/`. It does **not** touch
anything in Azure — the capacity, resource group and Fabric workspace all
survive, and you lose the settings that would have let `azd down` find them.
Tear down first, then remove the environment. See
[Tearing down](#tearing-down).

### 3. Set any options you need

Skip this entirely to create a new F64 capacity with defaults.

To reuse a Fabric capacity you already own instead of creating one:

```powershell
azd env set AZURE_EXISTING_FABRIC_CAPACITY_NAME <capacity-name>
```

To choose a different SKU — **F2 is much cheaper than the F64 default and is
enough for this sample corpus:**

```powershell
azd env set AZURE_FABRIC_CAPACITY_SKU F2
```

Allowed values are `F2`, `F4`, `F8`, `F16`, `F32`, `F64`, `F128`, `F256`.

> **Cost.** Fabric capacity is billed per second with no commitment, but it bills
> whenever it is running — not only while you are using it. **Pause the capacity
> in the Azure portal when the solution is idle**, and resume it before a
> session. See
> [Pause and resume](https://learn.microsoft.com/fabric/enterprise/pause-resume).

### 4. Deploy

```powershell
azd up
```

This:

1. **Provisions Azure resources** from [`infra/main.bicep`](../infra/main.bicep) —
   a resource group and a Fabric capacity. Outputs include
   `AZURE_FABRIC_CAPACITY_NAME` and `SOLUTION_SUFFIX`, which the installer reads.
   They are written back into the environment, so `azd env get-values` shows them
   afterwards.
2. **Runs the `postprovision` hook**, which is
   [`infra/scripts/fabric/Install-MxSolution.ps1`](../infra/scripts/fabric/Install-MxSolution.ps1).

`azd up` is re-runnable. If the Fabric half fails, fix the cause and run it again
rather than starting over.

### Running the installer on its own

The Fabric half is a plain PowerShell script. If provisioning already succeeded
and you only want to redo the Fabric work:

```powershell
./infra/scripts/fabric/Install-MxSolution.ps1
```

It reads the same environment variables `azd` sets. Add `-WhatIf` to see what it
would do without doing it.

---

## Environment variables

`Install-MxSolution.ps1` takes five values, each with an environment-variable
default:

| Parameter | Environment variable | Meaning |
| --- | --- | --- |
| `-CapacityName` | `AZURE_FABRIC_CAPACITY_NAME` | Fabric capacity to bind the workspace to. **Required** |
| `-WorkspaceName` | `FABRIC_WORKSPACE_NAME` | Workspace name. Falls back to `mx-fabric-dq-poc-<SolutionSuffix>` |
| `-SolutionSuffix` | `SOLUTION_SUFFIX` | Used to build the workspace name |
| `-CapacityAdministrators` | `AZURE_FABRIC_CAPACITY_ADMINISTRATORS` | Added as capacity administrators |
| `-WorkspaceAdministrators` | `FABRIC_WORKSPACE_ADMINISTRATORS` | Added as workspace administrators |

You must supply either a workspace name or a suffix. If neither is present the
script stops rather than guessing.

If the capacity name is missing, the error tells you to check `azd env get-values` —
that is almost always where it went.

---

## What the installer does

### Install-MxSolution.ps1 — three steps

| Step | Action |
| --- | --- |
| 1/3 | Create the workspace and bind it to the capacity |
| 2/3 | Assign workspace administrators |
| 3/3 | Invoke `Deploy-MxContent.ps1` with the new workspace ID |

Deployment never runs a pipeline; you run setup and processing yourself in the
portal. (`-SkipNotebookRun` is still accepted for compatibility and does nothing.)

### Deploy-MxContent.ps1 — six steps

| Step | Action |
| --- | --- |
| 1/6 | Create the medallion lakehouses |
| 2/6 | Upload the notebooks |
| 3/6 | Upload both pipelines |
| 4/6 | Upload the semantic model (set to large storage format) and the report |
| 5/6 | Load the sample/config files into `mx_bronze` |
| 6/6 | Print next steps — which pipeline to run first. Nothing is run |

Deployment publishes items and files only; it does not run either pipeline.
Existing items are updated in place rather than duplicated. Mapping tables are
never reseeded by deployment, but a redeploy **does** overwrite notebooks and
uploaded files you edited in the workspace — make those edits in the repository.

You can run it directly against an existing workspace:

```powershell
./infra/scripts/fabric/Deploy-MxContent.ps1 -WorkspaceId <guid>
```

| Switch | Effect |
| --- | --- |
| `-SkipSampleData` | Do not upload the 154 sample/config files |
| `-SkipPipeline` | Accepted for compatibility; does nothing, because pipelines are never auto-run |

#### Lakehouses are created schema-enabled

Step 1 passes `creationPayload.enableSchemas = true`. This is **off by default**
over the REST API and **cannot be changed after creation**. Without it the first
`CREATE SCHEMA` in `schema_model_bronze` fails and there is no way to recover
except deleting the lakehouse and starting again.

#### Placeholder IDs are rewritten at publish time

Notebooks, both pipelines and the semantic model are committed with fixed
placeholder GUIDs where workspace, lakehouse and notebook IDs belong. Each
publish step substitutes the real IDs it just captured, so the same tree deploys
into any workspace without hand-editing.

If you add a notebook to the pipeline, add it to `$PLACEHOLDER_NOTEBOOK` in
`Deploy-MxContent.ps1` too. A pipeline activity referencing a notebook that is
not in that map is a hard failure at publish time, on purpose — it is much
cheaper to fail there than to debug it in a pipeline run.

Publishing also stops before anything is uploaded if a pipeline or notebook
name is duplicated, a pipeline references a notebook it cannot bind, or a
placeholder is left unresolved.

#### Why the pipeline, not a parent notebook

Each pipeline runs its notebooks as separate activities rather than calling
`mssparkutils.notebook.run()` from an orchestrator notebook. A nested run shares
the parent's Spark session, and when a child fails the session is cancelled
outright rather than raising — so the failure cannot be caught, attributed or
retried. Separate activities give you a per-notebook status in the run history.

---

## Verifying the deployment

When `azd up` finishes it prints the workspace URL:

```
https://app.fabric.microsoft.com/groups/<workspace-id>
```

### 1. The lakehouses exist

Open the workspace. You should see `mx_bronze`, `mx_silver` and `mx_gold`.

### 2. The pipeline succeeded

Run `mx_setup_pipeline` once on a new workspace and confirm its four activities
are green. Then run `mx_dq_pipeline` and confirm its five activities are green —
the last one, `refresh_semantic_model`, frames the report's model. After that,
only the processing pipeline needs to run. If an activity failed, note which one;
the Spark error message will not tell you.

### 3. The tables are populated

Open `mx_gold` → **Tables** → `dq` and check row counts:

| Table | Expected rows |
| --- | --- |
| `dq.dim_participant` | 3 |
| `dq.dim_period` | 2 |
| `dq.dim_rule` | 33 |
| `dq.fact_rule_result` | one row per message per rule written for its message type |

### 4. The numbers match the baseline

The sample corpus is authored to produce a known, deliberately uneven quality
spread. `dq.vw_rule_results` scores every participant × period × rule cell;
count its statuses in a notebook:

```python
display(spark.sql("""
    SELECT participant_id,
           SUM(CASE WHEN status = 'Pass'    THEN 1 ELSE 0 END) AS pass,
           SUM(CASE WHEN status = 'Warning' THEN 1 ELSE 0 END) AS warning,
           SUM(CASE WHEN status = 'Fail'    THEN 1 ELSE 0 END) AS fail
    FROM mx_gold.dq.vw_rule_results
    GROUP BY participant_id
    ORDER BY participant_id
"""))
```

The expected result:

| Participant | Pass | Warning | Fail | Cells passing |
| --- | --- | --- | --- | --- |
| ARRMC — Arroyo Regional Medical Center | 66 | 0 | 0 | 100.0% |
| BVCH — Bay Valley Community Hospital | 38 | 4 | 16 | 65.5% |
| PCMC — Pinecrest Community Medical Center | 16 | 4 | 42 | 25.8% |

Totals: **150 messages, 33 rules, 186 cells**, none N/A. The last column is
Pass ÷ cells — the report's *Pass Rate* — not message-level compliance.

> Matching these numbers confirms the plumbing works end to end. It is **not**
> evidence the rules are correct, because the same repository generated both the
> data and the expectation. Validating the rules against real messages is what
> the working sessions are for.

### Results that look wrong but are not

Two results are correct and should not be "fixed":

- `P4P-ADT-020` and `P4P-ADT-021` (`PD1-3.1`, `PD1-4`) score 0.0% for BVCH and
  PCMC, because neither sends a `PD1` segment. Both rules allow exceptions, so
  the status is **Warning**, not **Fail**.
- PCMC's `P4P-ADT-033` scores 16.7%. It is a `CONDITIONAL` rule, so only the
  messages that carry a Patient Class are counted.

---

## The report

`mx_data_quality` deploys as a four-page report over the semantic model. It has
data as soon as `mx_dq_pipeline` finishes.

| Page | Answers |
| --- | --- |
| **MX Overview** | Which participants fall short, and on which rules? |
| **Participant Scorecard** | How did one participant do on every rule? |
| **Failing Messages** | Which individual messages failed this rule, and where are their files? |
| **Rule Catalog** | What is measured today, and where is it configured? |

**Failing Messages is a drillthrough page**, hidden in view mode. Right-click a
rule row on Overview or Scorecard and choose **Drill through**; it carries the
participant and rule across and lists the failed messages by file path. The
measures and dimensions behind the pages are described in
[Extend.md](Extend.md#report-measures-and-dimensions).

### Changing the report

Most changes are not report changes. A new field or threshold is a row in
`mapping.quality_rule`, and every page picks it up on the next run — see
[Extend.md](Extend.md). When the layout itself needs to change:

1. Open `fabric_workspace/reports/mx_data_quality.Report/definition.pbir` in
   Power BI Desktop. It points at the sibling `mx_data_quality.SemanticModel`
   folder, so the report and model open together.
2. Edit and save, then commit the changed files under `definition/`. Each page
   and visual is its own file, so changes read as reviewable diffs.
3. Redeploy. Step 4 of `Deploy-MxContent.ps1` rewrites the model reference in
   `definition.pbir` to point at the deployed model, so you never edit it by
   hand.

Desktop also writes a `.pbi/` folder of local cache beside the report; it is
git-ignored and not deployed. If the report fails to import, the deploy script
warns and carries on — everything else is still published.

### Direct Lake framing

The model reads `mx_gold` in Direct Lake mode. A model published over the REST
API has never been *framed* — pointed at the current Delta files — and until it
is, the workspace **Refresh** button fails with *Unable to load a query that
produces no tables*. The last `mx_dq_pipeline` activity, `refresh_semantic_model`,
frames it through the enhanced refresh API on every run, so no manual step is
needed.

Two things to know:

- Power BI **suspends automatic updates after a refresh error**, and resumes
  only after a successful refresh. If the report looks stale after a failed
  run, re-run `mx_dq_pipeline` or refresh the model once.
- **Do not add `dq.vw_rule_results` to the model.** It is a Spark view, not a
  Delta table, so Direct Lake cannot read it. It exists as a cross-check you
  can query from a notebook when comparing against MX's existing report.

---

## Connecting an Amazon S3 bucket

The sample corpus lands in OneLake as part of deployment. To point the same
pipeline at real messages sitting in Amazon S3, create a **read-only OneLake
shortcut** from `mx_bronze` to the bucket and change one parameter.

```
Amazon S3 bucket                OneLake shortcut              Bronze
  hl7/<participant>/...   -->   mx_bronze/Files/s3/   -->   bronze.raw_message
  ccda/<participant>/...
```

Nothing is copied into Fabric by the shortcut itself. The ingest notebook reads
through it and writes the message text into `bronze.raw_message`, exactly as it
does for the sample files.

### 1. On the AWS side

#### 1.1 Bucket details you will need

| Value | Example |
| --- | --- |
| Bucket name | `mx-hie-messages` |
| Region code | `us-west-2` |
| Connection URL | `https://<bucket>.s3.<region>.amazonaws.com` |

#### 1.2 Prefix layout

Lay the objects out the way the ingest notebook expects:

```
hl7/<participant>/<yyyy>/<mm>/<id>.hl7
ccda/<participant>/<yyyy>/<mm>/<id>.xml
```

Participant and reporting period are read from the path, not the message body.
Use the same value for `<participant>` that appears in `mapping.participant` —
for MX, the *Acronym List* value from the P4P workbook — so the two line up
without a translation step.

#### 1.3 An IAM user, not a role

> **Fabric S3 shortcuts authenticate with an IAM user access key and secret.
> IAM role assumption is not supported.**

Create a dedicated IAM user — for example `fabric-mx-dq-readonly` — with:

- **No console access**
- An access key of type *Application running outside AWS*
- The secret recorded at creation; AWS shows it once

#### 1.4 IAM policy

Attach a policy granting read access to the two prefixes and nothing else:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::<bucket>"
    },
    {
      "Sid": "ReadMessages",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::<bucket>/hl7/*",
        "arn:aws:s3:::<bucket>/ccda/*"
      ]
    }
  ]
}
```

#### 1.5 Encryption

| Bucket encryption | What is needed |
| --- | --- |
| SSE-S3 (default) | Nothing extra |
| **SSE-KMS** | The IAM user also needs `kms:Decrypt` and `kms:GenerateDataKey` on the key, **or every request returns 403** |
| SSE-C | Not supported by Fabric shortcuts |

For SSE-KMS, add the user to the key policy:

```json
{
  "Sid": "AllowFabricRead",
  "Effect": "Allow",
  "Principal": { "AWS": "arn:aws:iam::<account-id>:user/fabric-mx-dq-readonly" },
  "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
  "Resource": "*"
}
```

#### 1.6 Network

- You do **not** need to disable S3 Block Public Access. The shortcut
  authenticates; it does not need anonymous access.
- If the bucket is reachable only from a VPC or behind a firewall, you need an
  on-premises data gateway.
- The bucket must **not** be configured as Requester Pays.

#### 1.7 Checklist for the AWS administrator

> 1. Bucket name and region code
> 2. Objects laid out under `hl7/` and `ccda/` with participant and period folders
> 3. An IAM **user** (not a role) with no console access
> 4. A read-only policy scoped to those two prefixes
> 5. If SSE-KMS: `kms:Decrypt` and `kms:GenerateDataKey` on the key
> 6. The access key ID and secret, delivered securely

### 2. On the Fabric side

#### 2.1 Enable the tenant setting

**Admin portal → Tenant settings → OneLake settings →
*Users can create shortcuts to external data sources*.**

Without this, **Amazon S3 does not appear** in the shortcut menu at all.

#### 2.2 Create the connection

**Settings (gear) → Manage connections and gateways → + New → Cloud**

| Field | Value |
| --- | --- |
| Connection name | `mx-s3-hie-messages` |
| Connection type | **Amazon S3** — *not* "Amazon S3 compatible" |
| URL | `https://<bucket>.s3.<region>.amazonaws.com` |
| Authentication | **Key** — access key ID and secret |
| Privacy level | Organizational |

#### 2.3 Create the shortcut

1. Open `mx_bronze` → **Explorer**
2. Right-click **Files** — *not* Tables — → **New shortcut**
3. **External sources → Amazon S3**, pick the connection
4. Select the `hl7/` and `ccda/` prefixes
5. Rename the shortcut to **`s3`**

The result is `mx_bronze/Files/s3/hl7/...` and `mx_bronze/Files/s3/ccda/...`.

#### 2.4 Point ingestion at it

`ingest_raw_files` reads one value, defined near the top of the notebook:

```python
SOURCE_ROOT = "Files/samples_mx"
```

Change it to `Files/s3` and save the notebook. It is a plain assignment in a
code cell, not a tagged parameters cell, so it is edited rather than passed in
at run time.

Nothing else changes. The parsers, mapping tables and rules are unaware of where
the bytes came from.

#### 2.5 Caching

Enable OneLake shortcut caching if the same files are read repeatedly. It cuts
AWS egress charges and read latency.

### 3. Verify the shortcut

In any notebook attached to `mx_bronze`:

```python
notebookutils.fs.ls("Files/s3")
notebookutils.fs.ls("Files/s3/hl7/<participant>/2026/01")

path = "Files/s3/hl7/<participant>/2026/01/<file>.hl7"
print(notebookutils.fs.head(path, 500))
```

The last call should print text beginning `MSH|`.

### Security considerations for PHI

| Area | Guidance |
| --- | --- |
| Agreements | Confirm a BAA covers both the AWS account and the Fabric tenant |
| Least privilege | The IAM policy grants read on two prefixes only |
| Key management | Rotate the access key on a schedule; store it only in the Fabric connection |
| In transit | HTTPS throughout |
| At rest | SSE-S3 or SSE-KMS in S3; OneLake encrypts by default |
| Workspace access | **Viewer is enough to read report data.** Grant it deliberately |
| Auditing | Enable CloudTrail **data events** on the bucket to see object-level reads |
| De-identification | For a demo or workshop, prefer de-identified extracts over live PHI |

### Limitations

| Limitation | Detail |
| --- | --- |
| Read-only | Shortcuts to S3 cannot be written to |
| Authentication | IAM user access key only — no role assumption |
| Nesting | A shortcut cannot point at another shortcut |
| Copy Blob API | Not supported against S3 shortcuts |
| Object names | Characters reserved by RFC 3986 may not resolve |
| Encryption | SSE-C is not supported |

### Troubleshooting the shortcut

| Symptom | Cause |
| --- | --- |
| Amazon S3 missing from the shortcut menu | Tenant setting not enabled (§2.1) |
| 403 when browsing the bucket | Missing `s3:ListBucket` or `s3:GetBucketLocation` |
| Browsing works, reading objects 403s | SSE-KMS without `kms:Decrypt` (§1.5) |
| "Endpoint does not match region" | Region code in the URL is wrong |
| Browse succeeds but shows nothing | Prefix is empty, or the policy scopes it out |
| Network error creating the connection | Bucket not publicly reachable — needs a gateway |
| Worked yesterday, 403 today | Access key rotated or disabled |
| Slow reads, rising AWS bill | Enable shortcut caching (§2.5) |
| Files visible in AWS but not Fabric | Uploaded outside `hl7/`/`ccda/`, or after the shortcut was scoped |

### Reference

- [Create an Amazon S3 shortcut](https://learn.microsoft.com/fabric/onelake/create-s3-shortcut)
- [OneLake shortcuts](https://learn.microsoft.com/fabric/onelake/onelake-shortcuts)
- [OneLake shortcuts REST API](https://learn.microsoft.com/fabric/onelake/onelake-shortcuts-rest-api)
- [On-premises data gateway shortcuts](https://learn.microsoft.com/fabric/onelake/create-on-premises-shortcut)
- [AWS access keys](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)

---

## Troubleshooting

### "System cancelled the Spark session due to statement execution failures"

Fabric reports **every** notebook failure with this message. No cell number, no
line, no traceback — the same text whether the cause was a typo, a missing
table or an out-of-memory error, and the same through the job API and the Livy
API.

To find the actual cause:

1. Open the pipeline run history and note **which** activity failed
2. Open that notebook in the portal and run it interactively, cell by cell

A pipeline is much better than an orchestrator notebook here precisely because
it tells you which activity died.

### "Invalid object name" from the SQL analytics endpoint

The SQL endpoint lags the lakehouse. A table can exist in Delta and not yet be
queryable through SQL.

Check the lakehouse **Tables** view first. If the table is there, wait and
retry — this is not evidence the pipeline failed.

The SQL endpoint also exposes long strings such as `raw_payload` as
`varchar(8000)`, so large CCDA documents appear truncated there. Read full
payloads from a notebook.

### The first CREATE SCHEMA fails

The lakehouse was created without schema support. This cannot be changed after
creation — delete the lakehouse and re-run `Deploy-MxContent.ps1`, which passes
`enableSchemas = true`.

### A notebook cannot see a table in another lakehouse

`spark.table("bronze.raw_message")` resolves inside the notebook's **own**
default lakehouse. Cross-lakehouse reads need three-part names:

```python
spark.table("mx_bronze.bronze.raw_message")
```

Every lakehouse a notebook touches must also be attached to it.

### TLS or connection errors during deployment

The installer detects these and suggests checking for a VPN, proxy or
TLS-inspecting firewall between your machine and `api.fabric.microsoft.com`.

The script is safe to re-run; existing items are updated in place.

### Deployment stopped partway

Every step is idempotent. Re-run:

```powershell
./infra/scripts/fabric/Install-MxSolution.ps1
```

The failure output lists which steps completed before the error.

### The capacity name is empty

```powershell
azd env get-values
```

`AZURE_FABRIC_CAPACITY_NAME` should be set by provisioning. If it is missing,
provisioning did not complete — run `azd provision` again.

---

## Tearing down

```powershell
azd down
```

This runs the `predown` hook —
[`Remove-MxSolution.ps1 -Force`](../infra/scripts/fabric/Remove-MxSolution.ps1) —
which deletes the Fabric workspace, and then deletes the resource group and the
Fabric capacity.

> **Deleting a workspace permanently removes every item in it, including all
> lakehouse data.** There is no recycle bin for lakehouse tables.

To remove only the Fabric workspace and keep the capacity:

```powershell
./infra/scripts/fabric/Remove-MxSolution.ps1 -WhatIf   # preview
./infra/scripts/fabric/Remove-MxSolution.ps1           # prompts
```

It resolves the workspace by ID if you pass `-WorkspaceId` (or set
`FABRIC_WORKSPACE_ID`) — that **takes precedence** — and otherwise by name from
`FABRIC_WORKSPACE_NAME`, falling back to `mx-fabric-dq-poc-<SOLUTION_SUFFIX>`.
Whichever you use has to match what the installer created.

---

## Next

- [Extending the solution](Extend.md) — adding fields, rules and participants
- [Working sessions](WorkingSessionPlaybook.md) — validating the rules with MX
