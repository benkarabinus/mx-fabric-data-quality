#!/bin/bash
# Runs once after the dev container is created. Installs nothing: the tools
# come from the features in devcontainer.json, and the repository needs no
# PyPI packages.
set -e

echo "Tool versions:"
echo "  $(az --version | head -n 1)"
echo "  azd $(azd version)"
echo "  $(pwsh --version)"
echo "  $(git --version)"

# HL7 files are marked binary in .gitattributes; everything else is LF.
git config --global core.autocrlf input

echo ""
echo "Next: az login, azd auth login, azd env new <name>, azd up"
echo "See docs/Deploy.md for the full walkthrough."
