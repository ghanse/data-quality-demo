#!/bin/bash
# Quick Start Script for DQX Data Quality Demo
# This script helps you deploy the demo quickly

set -e

echo "========================================"
echo "DQX Data Quality Demo - Quick Start"
echo "========================================"
echo ""

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "Databricks CLI not found!"
    echo ""
    echo "Please install the Databricks CLI first:"
    echo "  pip install databricks-cli"
    echo ""
    echo "Or using Homebrew (macOS):"
    echo "  brew tap databricks/tap"
    echo "  brew install databricks"
    echo ""
    exit 1
fi

echo "Databricks CLI found: $(databricks --version)"
echo ""

# Check authentication
echo "Checking Databricks authentication..."
if ! databricks auth env 2>/dev/null | grep -q "Host:"; then
    echo "Not authenticated with Databricks"
    echo ""
    echo "Please authenticate first:"
    echo "  databricks auth login --host https://your-workspace.cloud.databricks.com"
    echo ""
    exit 1
fi

echo "Authenticated with Databricks"
echo ""

# Get target environment
echo "Select deployment target:"
echo "  1) dev (default)"
echo "  2) prod"
read -p "Enter choice [1]: " choice
choice=${choice:-1}

if [ "$choice" = "2" ]; then
    TARGET="prod"
    echo "Deploying to PRODUCTION"
    read -p "Are you sure? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Deployment cancelled"
        exit 0
    fi
else
    TARGET="dev"
    echo "📍 Deploying to DEVELOPMENT"
fi
echo ""

# Validate bundle
echo "Step 1: Validating bundle configuration..."
if databricks bundle validate --target "$TARGET"; then
    echo "Bundle validation passed"
else
    echo "Bundle validation failed"
    exit 1
fi
echo ""

# Deploy bundle
echo "Step 2: Deploying bundle to $TARGET..."
if databricks bundle deploy --target "$TARGET"; then
    echo "Bundle deployed successfully"
else
    echo "Bundle deployment failed"
    exit 1
fi
echo ""

# Ask if user wants to run the job
read -p "Do you want to run the data quality job now? (yes/no) [yes]: " run_job
run_job=${run_job:-yes}

if [ "$run_job" = "yes" ]; then
    echo "Step 3: Running the daily data quality refresh job..."
    echo "(This will generate data and run the DQX pipeline)"
    echo ""
    
    if databricks bundle run daily_data_quality_refresh --target "$TARGET"; then
        echo ""
        echo "Job started successfully!"
    else
        echo ""
        echo "Job run failed"
        exit 1
    fi
fi
