# Pharmacy & Facility source API

`split_sources.py` writes the JSON payloads into `./data`. Start the server:

    python api_server.py --data ./data --port 8000

Or containerised:

    docker compose up -d

Get a token and pull a page:

    curl -s -X POST localhost:8000/oauth/token \
      -d 'grant_type=client_credentials&client_id=fabric&client_secret=fabric-dev-secret'

    curl -s -H "Authorization: Bearer $TOKEN" \
      'localhost:8000/api/v1/medication-orders?limit=100'

Endpoints: `medication-orders`, `hospitals`, `departments`, `beds`.
Also `/health` (no auth) and `/api/v1/_requests` (recent request log, useful
for confirming Fabric actually followed your pagination cursor).

To test your pipeline's retry policy against real failures, add `--chaos 0.08`.

The credentials here are development defaults. Change them, and put the real
secret in Azure Key Vault before referencing it from a pipeline — the
pipeline JSON is committed to Git.
