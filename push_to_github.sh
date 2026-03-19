#!/bin/bash
# ============================================================
# push_to_github.sh
# PROJECT: E-Commerce Sales Analytics
# AUTHOR: Subha Ragupathi
#
# USAGE:
#   chmod +x push_to_github.sh
#   ./push_to_github.sh
#
# PREREQUISITES:
#   - git installed
#   - GitHub repo created (empty) at:
#     https://github.com/Subha-Ragupathi/ecommerce-sales-analytics
#   - Your GitHub PAT ready
# ============================================================

set -e  # Exit on any error

GITHUB_USERNAME="Subha-Ragupathi"
REPO_NAME="ecommerce-sales-analytics"
REPO_URL="https://github.com/${GITHUB_USERNAME}/${REPO_NAME}.git"

echo "=============================================="
echo "  🚀 Pushing E-Commerce Analytics Project"
echo "  → Repo: ${REPO_URL}"
echo "=============================================="

# Prompt for PAT securely
echo ""
read -sp "Enter your GitHub Personal Access Token (PAT): " GITHUB_PAT
echo ""

# Build authenticated URL
AUTH_URL="https://${GITHUB_USERNAME}:${GITHUB_PAT}@github.com/${GITHUB_USERNAME}/${REPO_NAME}.git"

# Initialize git if not already
if [ ! -d ".git" ]; then
    git init
    echo "✅ Git repository initialized"
fi

# Set remote
git remote remove origin 2>/dev/null || true
git remote add origin "${AUTH_URL}"
echo "✅ Remote set to GitHub"

# Configure git identity (update if needed)
git config user.name  "Subha Ragupathi"
git config user.email "subha@example.com"

# Stage all files
git add .
echo "✅ Files staged"

# Commit
COMMIT_MSG="feat: Add complete E-Commerce Sales Analytics project

- 10,000 row synthetic dataset (2022-2024)
- Jupyter EDA notebook with 8 visualisations
- SQL star schema + KPI queries (RFM, CLV, Cohort)
- Azure ETL pipeline (Bronze → Silver → Gold)
- Power BI 5-page dashboard specification
- Professional README"

git commit -m "${COMMIT_MSG}" 2>/dev/null || echo "Nothing new to commit"

# Push to main branch
git branch -M main
git push -u origin main --force

echo ""
echo "=============================================="
echo "  ✅ Project pushed successfully!"
echo "  🔗 View at: https://github.com/${GITHUB_USERNAME}/${REPO_NAME}"
echo "=============================================="
