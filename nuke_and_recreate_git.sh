#!/bin/bash
# Script to nuke git history and start fresh
# WARNING: This will delete all git history!

set -e

echo "⚠️  WARNING: This will DELETE all git history!"
echo "📋 Current remote: $(git remote get-url origin 2>/dev/null || echo 'none')"
echo ""
read -p "Continue? Type 'yes' to confirm: " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

# Backup remote URL if it exists
REMOTE_URL=$(git remote get-url origin 2>/dev/null || echo "")

# Remove git history
echo "🗑️  Removing .git directory..."
rm -rf .git

# Initialize fresh git repo
echo "🆕 Initializing fresh git repository..."
git init

# Set default branch to main
git branch -M main

# Add all files
echo "📦 Staging all files..."
git add .

# Create initial commit
echo "💾 Creating initial commit..."
git commit -m "Initial commit: MCP server for data engineering tools

- 66+ tools for Kafka, databases, AWS, pandas, polars, DuckDB
- Infrastructure tools for Nomad and Consul
- Data quality and validation tools
- Chart generation and evidence bundles
- Comprehensive security guardrails"

# Restore remote if it existed
if [ -n "$REMOTE_URL" ]; then
    echo "🔗 Restoring remote..."
    git remote add origin "$REMOTE_URL"
    echo ""
    echo "✅ Git history nuked and recreated!"
    echo ""
    echo "📝 Next steps:"
    echo "1. Review the commit: git log"
    echo "2. Force push to remote: git push --force origin main"
    echo "   (This will overwrite the remote history)"
    echo ""
    echo "⚠️  Note: If others have cloned this repo, they'll need to re-clone."
else
    echo ""
    echo "✅ Git history nuked and recreated!"
    echo "📝 No remote configured. Add one with: git remote add origin <url>"
fi

