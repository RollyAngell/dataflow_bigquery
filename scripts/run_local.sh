#!/bin/bash
# =============================================================================
# Local Pipeline Execution Script
# =============================================================================
# This script runs the Dataflow pipeline locally using DirectRunner.
# Useful for development, testing, and debugging.
#
# Usage:
#   ./scripts/run_local.sh [options]
#
# Requirements:
#   - Python 3.9+
#   - Virtual environment activated with dependencies installed
#   - GCS access configured (gcloud auth application-default login)
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default configuration
PROJECT_ID="${GCP_PROJECT:-your-project-id}"
REGION="${GCP_REGION:-us-central1}"
INPUT_BUCKET="${INPUT_BUCKET:-${PROJECT_ID}-input-dev}"
INPUT_FILE="${INPUT_FILE:-data/sample.csv}"
OUTPUT_DATASET="${BQ_DATASET:-data_warehouse_dev}"
OUTPUT_TABLE="${BQ_TABLE:-processed_records}"
DEAD_LETTER_BUCKET="${DEAD_LETTER_BUCKET:-${PROJECT_ID}-dead-letter-dev}"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --project)
            PROJECT_ID="$2"
            shift 2
            ;;
        --input-bucket)
            INPUT_BUCKET="$2"
            shift 2
            ;;
        --input-file)
            INPUT_FILE="$2"
            shift 2
            ;;
        --output-dataset)
            OUTPUT_DATASET="$2"
            shift 2
            ;;
        --output-table)
            OUTPUT_TABLE="$2"
            shift 2
            ;;
        --dead-letter-bucket)
            DEAD_LETTER_BUCKET="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --project             GCP Project ID"
            echo "  --input-bucket        GCS bucket with input CSV"
            echo "  --input-file          Path to CSV file in bucket"
            echo "  --output-dataset      BigQuery dataset name"
            echo "  --output-table        BigQuery table name"
            echo "  --dead-letter-bucket  GCS bucket for invalid records"
            echo "  --help                Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}Running Dataflow Pipeline Locally${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Project:            ${PROJECT_ID}"
echo "  Region:             ${REGION}"
echo "  Input:              gs://${INPUT_BUCKET}/${INPUT_FILE}"
echo "  Output:             ${PROJECT_ID}:${OUTPUT_DATASET}.${OUTPUT_TABLE}"
echo "  Dead Letter:        gs://${DEAD_LETTER_BUCKET}/"
echo ""

# Check if virtual environment is activated
if [[ -z "${VIRTUAL_ENV}" ]]; then
    echo -e "${YELLOW}Warning: No virtual environment detected.${NC}"
    echo "Consider running: source venv/bin/activate"
fi

# Ensure we're in the project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Add src to PYTHONPATH
export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH}"

echo -e "${GREEN}Starting pipeline...${NC}"
echo ""

# Run the pipeline locally
python -m pipeline.main \
    --runner=DirectRunner \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --input_bucket="${INPUT_BUCKET}" \
    --input_file="${INPUT_FILE}" \
    --output_dataset="${OUTPUT_DATASET}" \
    --output_table="${OUTPUT_TABLE}" \
    --dead_letter_bucket="${DEAD_LETTER_BUCKET}" \
    --delimiter="," \
    --has_header=True \
    --schema_file="schemas/bigquery_schema.json"

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}Pipeline completed successfully!${NC}"
echo -e "${GREEN}============================================${NC}"
