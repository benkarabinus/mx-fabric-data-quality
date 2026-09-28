# Dev container

Opens the repository with every deployment tool already installed, so nothing
needs to be set up on your machine beyond Docker and VS Code (or GitHub
Codespaces).

| Tool | Why |
|---|---|
| Azure CLI + Bicep | provisions the Fabric capacity, supplies API tokens |
| Azure Developer CLI (`azd`) | runs the deployment |
| PowerShell 7 | runs the Fabric deployment scripts |
| Git, GitHub CLI | source control |
| Python 3.11 (base image) | only to regenerate the sample corpus; not used to deploy |

Nothing is installed from PyPI.

## Use it

1. Open the repository in VS Code and run **Dev Containers: Reopen in Container**
   (or create a Codespace from GitHub).
2. In the container terminal:

   ```bash
   az login
   azd auth login
   azd env new mx-poc
   azd up
   ```

See [docs/Deploy.md](../docs/Deploy.md) for options and environment variables.

## Files

- `devcontainer.json` — base image, tool features, VS Code extensions
- `Dockerfile` — the Python 3.11 base image
- `post-create.sh` — prints tool versions and sets `core.autocrlf input`
