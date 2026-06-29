.PHONY: install dev test run demo

install:
	pip install -r requirements.txt

dev:
	pip install -r requirements-dev.txt

test:
	pytest -q

run:
	uvicorn realtime_stream_inference.app:app --reload --port 8000

demo:
	python scripts/demo.py
