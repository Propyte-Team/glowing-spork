FROM python:3.12-slim

WORKDIR /app

COPY enrichment_agent_v2/ ./enrichment_agent_v2/
COPY enrichment_runner.py ./enrichment_runner.py
COPY .env ./.env

RUN pip install --no-cache-dir -r enrichment_agent_v2/requirements.txt

ENV PORT=8080

# Enrichment Runner: FastAPI server + background loop cada 30 min
CMD sh -c "python enrichment_runner.py"
