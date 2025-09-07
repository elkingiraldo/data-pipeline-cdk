#!/bin/bash
set -e

echo "🚀 Starting deployment of Data Pipeline"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ENVIRONMENT=${1:-dev}
AWS_REGION=${AWS_REGION:-us-east-1}
PROJECT_NAME="data-pipeline"

echo -e "${YELLOW}Environment: ${ENVIRONMENT}${NC}"
echo -e "${YELLOW}Region: ${AWS_REGION}${NC}"

# Check prerequisites
echo "📋 Checking prerequisites..."

if ! command -v aws &> /dev/null; then
    echo -e "${RED}❌ AWS CLI not found${NC}"
    exit 1
fi

if ! command -v cdk &> /dev/null; then
    echo -e "${RED}❌ CDK not found${NC}"
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 not found${NC}"
    exit 1
fi

echo -e "${GREEN}✅ All prerequisites met${NC}"

# Set environment variables
export ENVIRONMENT=$ENVIRONMENT
export CDK_DEFAULT_REGION=$AWS_REGION

# Install dependencies
echo "📦 Installing dependencies..."
pip3 install -r requirements.txt

# Install Lambda dependencies
echo "📦 Installing Lambda dependencies..."
cd lambda/data_extractor
pip3 install -r requirements.txt -t .
cd ../..

# Run tests
echo "🧪 Running tests..."
pytest tests/unit -v

# Synthesize CDK
echo "🔨 Synthesizing CDK application..."
cdk synth

# Bootstrap if needed
echo "🔧 Checking CDK bootstrap..."
cdk bootstrap aws://$AWS_ACCOUNT_ID/$AWS_REGION || true

# Deploy
echo "🚀 Deploying stacks..."
cdk deploy --all --require-approval never

echo -e "${GREEN}✅ Deployment completed successfully!${NC}"

# Output important information
echo ""
echo "📊 Deployment Summary:"
echo "========================"
aws cloudformation describe-stacks \
    --stack-name "${PROJECT_NAME}-${ENVIRONMENT}" \
    --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
    --output table