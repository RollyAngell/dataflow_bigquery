#!/bin/bash
# =============================================================================
# Dataflow Pipeline Deployment Script
# =============================================================================
# This script deploys the pipeline to Google Cloud Dataflow.
#
# Usage:
#   ./scripts/run_dataflow.sh --env [dev|staging|prod] [options]
#
# Requirements:
#   - gcloud CLI authenticated
#   - Appropriate IAM permissions
#   - GCS buckets created
#   - BigQuery dataset created
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
ENVIRONMENT="dev"
PROJECT_ID="${GCP_PROJECT:-your-project-id}"
REGION="${GCP_REGION:-us-central1}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-}"
MAX_WORKERS=10
MACHINE_TYPE="n1-standard-2"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --env)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --project)
            PROJECT_ID="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --service-account)
            SERVICE_ACCOUNT="$2"
            shift 2
            ;;
        --max-workers)
            MAX_WORKERS="$2"
            shift 2
            ;;
        --machine-type)
            MACHINE_TYPE="$2"
            shift 2
            ;;
        --input-file)
            INPUT_FILE_OVERRIDE="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 --env [dev|staging|prod] [options]"
            echo ""
            echo "Required:"
            echo "  --env                 Environment (dev, staging, prod)"
            echo ""
            echo "Options:"
            echo "  --project             GCP Project ID"
            echo "  --region              GCP Region (default: us-central1)"
            echo "  --service-account     Service account email"
            echo "  --max-workers         Maximum number of workers (default: 10)"
            echo "  --machine-type        Worker machine type (default: n1-standard-2)"
            echo "  --input-file          Override input file path"
            echo "  --help                Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Set environment-specific variables
case $ENVIRONMENT in
    dev)
        INPUT_BUCKET="${PROJECT_ID}-input-dev"
        INPUT_FILE="${INPUT_FILE_OVERRIDE:-data/input.csv}"
        OUTPUT_DATASET="data_warehouse_dev"
        OUTPUT_TABLE="processed_records"
        DEAD_LETTER_BUCKET="${PROJECT_ID}-dead-letter-dev"
        DATAFLOW_BUCKET="${PROJECT_ID}-dataflow-dev"
        MAX_WORKERS=5
        ;;
    staging)
        INPUT_BUCKET="${PROJECT_ID}-input-staging"
        INPUT_FILE="${INPUT_FILE_OVERRIDE:-data/input.csv}"
        OUTPUT_DATASET="data_warehouse_staging"
        OUTPUT_TABLE="processed_records"
        DEAD_LETTER_BUCKET="${PROJECT_ID}-dead-letter-staging"
        DATAFLOW_BUCKET="${PROJECT_ID}-dataflow-staging"
        MAX_WORKERS=10
        ;;
    prod)
        INPUT_BUCKET="${PROJECT_ID}-input-prod"
        INPUT_FILE="${INPUT_FILE_OVERRIDE:-data/input.csv}"
        OUTPUT_DATASET="data_warehouse_prod"
        OUTPUT_TABLE="processed_records"
        DEAD_LETTER_BUCKET="${PROJECT_ID}-dead-letter-prod"
        DATAFLOW_BUCKET="${PROJECT_ID}-dataflow-prod"
        MAX_WORKERS=20
        ;;
    *)
        echo -e "${RED}Invalid environment: ${ENVIRONMENT}${NC}"
        echo "Valid options: dev, staging, prod"
        exit 1
        ;;
esac

# Generate job name with timestamp
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
JOB_NAME="csv-to-bq-${ENVIRONMENT}-${TIMESTAMP}"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Deploying Dataflow Pipeline${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Environment:        ${ENVIRONMENT}"
echo "  Project:            ${PROJECT_ID}"
echo "  Region:             ${REGION}"
echo "  Job Name:           ${JOB_NAME}"
echo "  Input:              gs://${INPUT_BUCKET}/${INPUT_FILE}"
echo "  Output:             ${PROJECT_ID}:${OUTPUT_DATASET}.${OUTPUT_TABLE}"
echo "  Dead Letter:        gs://${DEAD_LETTER_BUCKET}/"
echo "  Max Workers:        ${MAX_WORKERS}"
echo "  Machine Type:       ${MACHINE_TYPE}"
if [[ -n "${SERVICE_ACCOUNT}" ]]; then
    echo "  Service Account:    ${SERVICE_ACCOUNT}"
fi
echo ""

# Confirm deployment for production
if [[ "${ENVIRONMENT}" == "prod" ]]; then
    echo -e "${RED}WARNING: You are deploying to PRODUCTION!${NC}"
    read -p "Are you sure you want to continue? (yes/no): " CONFIRM
    if [[ "${CONFIRM}" != "yes" ]]; then
        echo "Deployment cancelled."
        exit 0
    fi
fi

# Ensure we're in the project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Add src to PYTHONPATH
export PYTHONPATH="${PROJECT_ROOT}/src:${PYTHONPATH}"

echo -e "${GREEN}Starting Dataflow job...${NC}"
echo ""

# Build the command
CMD="python -m pipeline.main \
    --runner=DataflowRunner \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --input_bucket=${INPUT_BUCKET} \
    --input_file=${INPUT_FILE} \
    --output_dataset=${OUTPUT_DATASET} \
    --output_table=${OUTPUT_TABLE} \
    --dead_letter_bucket=${DEAD_LETTER_BUCKET} \
    --temp_location=gs://${DATAFLOW_BUCKET}/temp \
    --staging_location=gs://${DATAFLOW_BUCKET}/staging \
    --job_name=${JOB_NAME} \
    --max_num_workers=${MAX_WORKERS} \
    --worker_machine_type=${MACHINE_TYPE} \
    --disk_size_gb=50 \
    --experiments=use_runner_v2 \
    --save_main_session \
    --setup_file=./setup.py"

# Add service account if specified
if [[ -n "${SERVICE_ACCOUNT}" ]]; then
    CMD="${CMD} --service_account_email=${SERVICE_ACCOUNT}"
fi

# Add production-specific options
if [[ "${ENVIRONMENT}" == "prod" ]]; then
    CMD="${CMD} --use_public_ips=false"
fi

# Execute the command
eval $CMD

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}Dataflow job submitted successfully!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo -e "${YELLOW}Monitor your job at:${NC}"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
