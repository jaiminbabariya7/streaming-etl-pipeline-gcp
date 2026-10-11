.PHONY: install install-dev lint format test run-producer run-bridge docker-up terraform-init clean

install:
	pip install -r requirements.txt

install-dev: install
	pip install pytest pytest-cov black isort flake8

lint:
	flake8 kafka/ dataflow/ airflow/ tests/ --max-line-length=100 --ignore=E501,W503
	black --check --line-length 100 kafka/ dataflow/ airflow/ tests/

format:
	black --line-length 100 kafka/ dataflow/ airflow/ tests/
	isort kafka/ dataflow/ airflow/ tests/

test:
	pytest tests/ -v --cov=kafka --cov=dataflow --cov-report=term-missing

run-producer:
	python kafka/producer.py --rate 50

run-bridge:
	python kafka/kafka_to_pubsub.py

run-pipeline-local:
	python -m dataflow.pipeline --runner=DirectRunner

run-pipeline-gcp:
	python -m dataflow.pipeline --runner=DataflowRunner

dbt-run:
	dbt run --project-dir dbt --profiles-dir dbt

dbt-test:
	dbt test --project-dir dbt --profiles-dir dbt

docker-up:
	docker compose -f docker/docker-compose.yml --env-file .env up --build -d

terraform-init:
	cd terraform && terraform init && terraform plan -var="project_id=$$GCP_PROJECT_ID"

terraform-apply:
	cd terraform && terraform apply -var="project_id=$$GCP_PROJECT_ID" -auto-approve

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov/ .pytest_cache/ dbt/target/
