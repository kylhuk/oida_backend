SHELL := /bin/bash

up:
	cp -n .env.example .env || true
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

test:
	pytest -q

verify:
	python -m compileall src tests
	PYTHONPATH=src python -m data_platform.dependency_locks
	PYTHONPATH=src python -m data_platform.secrets
	pytest -q


verify-locks:
	PYTHONPATH=src python -m data_platform.dependency_locks


verify-secrets:
	PYTHONPATH=src python -m data_platform.secrets


cleanup-metadata:
	PYTHONPATH=src python -m data_platform.metadata_cleanup


cleanup-metadata-apply:
	PYTHONPATH=src python -m data_platform.metadata_cleanup --apply


snapshot:
	PYTHONPATH=src python -m data_platform.repo_snapshot


healthcheck:
	PYTHONPATH=src python -m data_platform.baseline_health

verify-matrix:
	PYTHONPATH=src python -m data_platform.verification_matrix
