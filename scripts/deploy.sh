#!/usr/bin/env bash
set -Eeuo pipefail

echo "🚀 Starting deployment of Data Pipeline"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Config
ENVIRONMENT="${1:-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"
PROJECT_NAME="data-pipeline"
STACK_NAME="${PROJECT_NAME}-${ENVIRONMENT}"

echo -e "${YELLOW}Environment: ${ENVIRONMENT}${NC}"
echo -e "${YELLOW}Region: ${AWS_REGION}${NC}"

# Preconditions
echo "📋 Checking prerequisites..."
command -v aws  >/dev/null || { echo -e "${RED}❌ AWS CLI not found${NC}"; exit 1; }
command -v cdk  >/dev/null || { echo -e "${RED}❌ CDK not found${NC}"; exit 1; }
command -v python3 >/dev/null || { echo -e "${RED}❌ Python 3 not found${NC}"; exit 1; }

# (Optional but recommended) Docker check for bundling
if ! command -v docker >/dev/null 2>&1; then
  echo -e "${YELLOW}⚠️  Docker no encontrado. CDK intentará bundling local.${NC}"
fi

echo -e "${GREEN}✅ Prerequisites OK${NC}"

# Environment variables
export ENVIRONMENT="${ENVIRONMENT}"
export CDK_DEFAULT_REGION="${AWS_REGION}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
export AWS_ACCOUNT_ID

# Venv
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  if [[ ! -d ".venv" ]]; then
    echo "🐍 Creating virtualenv .venv"
    python3 -m venv .venv
  fi
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

python -m pip install --upgrade pip wheel setuptools

# Install dependencies, not these of the Lambda
echo "📦 Installing infra/test dependencies..."
if [[ -f requirements-dev.txt ]]; then
  pip install -r requirements-dev.txt
else
  pip install -r requirements.txt
fi

# Do not install Lambda runtime dependencies locally
# The CDK will handle bundling using the requirements.txt inside lambdas/data_extractor
# (or the path you have configured in your stack)
echo "🧰 Skipping local install of Lambda runtime deps (handled by CDK bundling)."

# Tests (skip if SKIP_TESTS=1)
if [[ "${SKIP_TESTS:-0}" != "1" ]]; then
  echo "🧪 Running tests..."
  pytest tests/ -v
else
  echo "🧪 Tests skipped (SKIP_TESTS=1)."
fi

# Synth
echo "🔨 Synthesizing CDK application..."
cdk synth

# Bootstrap
echo "🔧 Checking CDK bootstrap..."
cdk bootstrap "aws://${AWS_ACCOUNT_ID}/${AWS_REGION}" || true

# Deploy
echo "🚀 Deploying stacks..."
cdk deploy --all --require-approval never

echo -e "${GREEN}✅ Deployment completed successfully!${NC}"

# Outputs
echo ""
echo "📊 Deployment Summary:"
echo "========================"
aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
  --output table
