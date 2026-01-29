# =============================================================================
# Local Pipeline Execution Script (PowerShell)
# =============================================================================
# This script runs the Dataflow pipeline locally using DirectRunner.
# Useful for development, testing, and debugging on Windows.
#
# Usage:
#   .\scripts\run_local.ps1 [-Project "project-id"] [-InputFile "path/to/file.csv"]
#
# Requirements:
#   - Python 3.9+
#   - Virtual environment activated with dependencies installed
#   - GCS access configured (gcloud auth application-default login)
# =============================================================================

param(
    [string]$Project = $env:GCP_PROJECT,
    [string]$Region = "us-central1",
    [string]$InputBucket = "",
    [string]$InputFile = "data/sample.csv",
    [string]$OutputDataset = "data_warehouse_dev",
    [string]$OutputTable = "processed_records",
    [string]$DeadLetterBucket = "",
    [switch]$Help
)

if ($Help) {
    Write-Host @"
Usage: .\scripts\run_local.ps1 [options]

Options:
  -Project           GCP Project ID
  -Region            GCP Region (default: us-central1)
  -InputBucket       GCS bucket with input CSV
  -InputFile         Path to CSV file in bucket
  -OutputDataset     BigQuery dataset name
  -OutputTable       BigQuery table name
  -DeadLetterBucket  GCS bucket for invalid records
  -Help              Show this help message
"@
    exit 0
}

# Set defaults based on project
if (-not $Project) {
    $Project = "your-project-id"
}
if (-not $InputBucket) {
    $InputBucket = "$Project-input-dev"
}
if (-not $DeadLetterBucket) {
    $DeadLetterBucket = "$Project-dead-letter-dev"
}

Write-Host "============================================" -ForegroundColor Green
Write-Host "Running Dataflow Pipeline Locally" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Configuration:" -ForegroundColor Yellow
Write-Host "  Project:            $Project"
Write-Host "  Region:             $Region"
Write-Host "  Input:              gs://$InputBucket/$InputFile"
Write-Host "  Output:             ${Project}:${OutputDataset}.${OutputTable}"
Write-Host "  Dead Letter:        gs://$DeadLetterBucket/"
Write-Host ""

# Get script directory and project root
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

# Change to project root
Set-Location $ProjectRoot

# Set PYTHONPATH
$env:PYTHONPATH = "$ProjectRoot\src;$env:PYTHONPATH"

Write-Host "Starting pipeline..." -ForegroundColor Green
Write-Host ""

# Run the pipeline
python -m pipeline.main `
    --runner=DirectRunner `
    --project="$Project" `
    --region="$Region" `
    --input_bucket="$InputBucket" `
    --input_file="$InputFile" `
    --output_dataset="$OutputDataset" `
    --output_table="$OutputTable" `
    --dead_letter_bucket="$DeadLetterBucket" `
    --delimiter="," `
    --has_header=True `
    --schema_file="schemas/bigquery_schema.json"

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "Pipeline completed successfully!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Red
    Write-Host "Pipeline failed with exit code: $LASTEXITCODE" -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Red
}
