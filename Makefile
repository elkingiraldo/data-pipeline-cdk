.PHONY: help install test deploy destroy clean lint format

# Variables
PYTHON := python3
PIP := pip3
CDK := cdk
PROJECT_NAME := data-pipeline
ENVIRONMENT ?= dev

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

install: ## Install dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt
	cd lambda/data_extractor && $(PIP) install -r requirements.txt -t .
	pre-commit install

test: ## Run tests
	pytest tests/ -v --cov=infrastructure --cov=lambda --cov-report=html --cov-report=term

test-unit: ## Run unit tests only
	pytest tests/unit -v

test-integration: ## Run integration tests only
	pytest tests/integration -v

lint: ## Run linting
	flake8 infrastructure/ lambda/ tests/
	pylint infrastructure/ lambda/
	mypy infrastructure/ lambda/
	bandit -r infrastructure/ lambda/

format: ## Format code
	black infrastructure/ lambda/ tests/
	isort infrastructure/ lambda/ tests/

security-scan: ## Run security scan
	safety check
	bandit -r infrastructure/ lambda/ -f json -o security-report.json

synth: ## Synthesize CDK application
	$(CDK) synth

diff: ## Show CDK diff
	$(CDK) diff

deploy: ## Deploy to AWS
	@echo "Deploying $(PROJECT_NAME) to $(ENVIRONMENT) environment..."
	$(CDK) deploy --all --require-approval never

deploy-with-approval: ## Deploy with manual approval
	$(CDK) deploy --all

destroy: ## Destroy all stacks
	@echo "⚠️  WARNING: This will destroy all resources!"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	$(CDK) destroy --all --force

bootstrap: ## Bootstrap CDK
	$(CDK) bootstrap

clean: ## Clean temporary files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf cdk.out/ .pytest_cache/ .coverage htmlcov/ .tox/
	rm -rf lambda/data_extractor/*.dist-info 2>/dev/null || true

validate: lint test ## Run all validations

trigger-lambda: ## Manually trigger the Lambda function
	aws lambda invoke \
		--function-name $(PROJECT_NAME)-data-extractor \
		--payload '{"force_refresh": true}' \
		response.json

run-crawler: ## Manually run the Glue crawler
	aws glue start-crawler --name $(PROJECT_NAME)_crawler

query-athena: ## Run sample Athena query
	@echo "Run this query in Athena console:"
	@echo "SELECT * FROM $(PROJECT_NAME)_db.$(PROJECT_NAME)_raw_data LIMIT 10;"

logs: ## View Lambda logs
	aws logs tail /aws/lambda/$(PROJECT_NAME)-data-extractor --follow

init-project: install bootstrap ## Initialize project from scratch
