#!/usr/bin/env python3
"""
OHN Pharmacy & Facility REST API — a source system for Fabric to ingest from.

Why the standard library and not FastAPI
----------------------------------------
Fabric only ever sees HTTP and JSON. FastAPI would add OpenAPI docs and
nothing else Fabric can use, at the cost of a dependency you have to install
before anything works. This runs with `python api_server.py` on any machine
with Python 3.10+, which matters when the thing you are actually trying to
debug is a Fabric connector. If you want Swagger docs for the portfolio,
porting the handlers to FastAPI is an afternoon.

What it deliberately does that a file drop cannot
-------------------------------------------------
  * OAuth2 client-credentials token exchange, tokens expire in 15 minutes
  * Cursor pagination — the pipeline must follow next_url, not guess offsets
  * updated_since filtering, which is what makes incremental loads possible
  * Rate limiting with 429 + Retry-After
  * Optional chaos mode: intermittent 500s and slow responses, so you can
    prove your pipeline's retry policy actually works instead of assuming it

Usage:
    python api_server.py --data ./data --port 8000
    python api_server.py --data ./data --port 8000 --chaos 0.08

Then:
    curl -X POST localhost:8000/oauth/token \\
         -d 'client_id=fabric&client_secret=fabric-dev-secret&grant_type=client_credentials'
    curl -H 'Authorization: Bearer <token>' \\
         'localhost:8000/api/v1/medication-orders?limit=100'
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import secrets
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# =====================================================================
# CONFIG
# =====================================================================

CLIENTS = {
    "fabric": "fabric-dev-secret",
    "readonly": "readonly-dev-secret",
}

TOKEN_TTL_SECONDS = 900
RATE_LIMIT_REQUESTS = 120
RATE_LIMIT_WINDOW_SECONDS = 60
DEFAULT_PAGE_SIZE = 500
MAX_PAGE_SIZE = 2000

# entity -> (json filename, id field, updated-at field)
ENDPOINTS = {
    "medication-orders": ("pharm_medication_order.json", "medication_order_id", "updated_at"),
    "hospitals": ("facil_hospital.json", "hospital_id", "update_ts"),
    "departments": ("facil_department.json", "department_id", "update_ts"),
    "beds": ("facil_bed.json", "bed_id", "update_ts"),
}

STORE: dict[str, list[dict]] = {}
TOKENS: dict[str, float] = {}
RATE_BUCKET: dict[str, deque] = defaultdict(deque)
LOCK = threading.Lock()
CHAOS = 0.0
REQUEST_LOG: list[dict] = []


# =====================================================================
# DATA LOADING
# =====================================================================

def load_store(data_dir: str) -> None:
    for entity, (fname, id_field, ts_field) in ENDPOINTS.items():
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            print(f"  WARNING: {path} missing — /{entity} will return empty")
            STORE[entity] = []
            continue
        with open(path, encoding="utf-8") as fh:
            rows = json.load(fh)
        # Sort by (updated_at, id) so cursor pagination is stable. Without a
        # deterministic order, a cursor can skip or repeat rows between pages
        # and the pipeline silently loses data.
        rows.sort(key=lambda r: (r.get(ts_field) or "", str(r.get(id_field) or "")))
        STORE[entity] = rows
        print(f"  loaded {entity:<20} {len(rows):>8,} records")


# =====================================================================
# CURSOR
# =====================================================================

def encode_cursor(ts_value: str, id_value: str) -> str:
    raw = json.dumps({"t": ts_value, "i": id_value}).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[str, str] | None:
    try:
        pad = "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(cursor + pad)
        d = json.loads(raw)
        return d["t"], d["i"]
    except Exception:
        return None


# =====================================================================
# HANDLER
# =====================================================================

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "OHN-SourceAPI/1.0"

    # ---------------------------------------------------------- plumbing
    def log_message(self, fmt, *args):
        pass  # handled explicitly in _respond

    def _respond(self, code: int, payload: dict | list, extra_headers: dict | None = None):
        body = json.dumps(payload, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, str(v))
        self.end_headers()
        self.wfile.write(body)
        with LOCK:
            REQUEST_LOG.append({
                "ts": datetime.now(timezone.utc).isoformat(),
                "path": self.path, "status": code,
            })
        print(f"  {self.command} {self.path[:90]} -> {code}")

    def _error(self, code: int, message: str, extra_headers: dict | None = None):
        self._respond(code, {"error": {"code": code, "message": message}}, extra_headers)

    # ---------------------------------------------------------- auth
    def _client_key(self) -> str:
        return self.headers.get("Authorization", "") or self.client_address[0]

    def _check_rate_limit(self) -> bool:
        key = self._client_key()
        now = time.time()
        with LOCK:
            bucket = RATE_BUCKET[key]
            while bucket and now - bucket[0] > RATE_LIMIT_WINDOW_SECONDS:
                bucket.popleft()
            if len(bucket) >= RATE_LIMIT_REQUESTS:
                return False
            bucket.append(now)
        return True

    def _check_token(self) -> bool:
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return False
        token = auth[7:].strip()
        with LOCK:
            expiry = TOKENS.get(token)
        return expiry is not None and expiry > time.time()

    # ---------------------------------------------------------- POST
    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/oauth/token":
            return self._error(404, "Not found")

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode() if length else ""
        form = {k: v[0] for k, v in parse_qs(raw).items()}

        if form.get("grant_type") != "client_credentials":
            return self._error(400, "Only grant_type=client_credentials is supported")

        cid, secret = form.get("client_id"), form.get("client_secret")
        if CLIENTS.get(cid) != secret:
            return self._error(401, "Invalid client credentials")

        token = secrets.token_urlsafe(32)
        with LOCK:
            TOKENS[token] = time.time() + TOKEN_TTL_SECONDS
        return self._respond(200, {
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": TOKEN_TTL_SECONDS,
            "scope": "read:pharmacy read:facility",
        })

    # ---------------------------------------------------------- GET
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

        if path == "/health":
            return self._respond(200, {
                "status": "ok",
                "entities": {k: len(v) for k, v in STORE.items()},
                "server_time": datetime.now(timezone.utc).isoformat(),
            })

        if path == "/api/v1/_requests":
            with LOCK:
                return self._respond(200, {"count": len(REQUEST_LOG),
                                           "recent": REQUEST_LOG[-50:]})

        if not path.startswith("/api/v1/"):
            return self._error(404, "Not found")

        if not self._check_token():
            return self._error(401, "Missing or expired bearer token",
                               {"WWW-Authenticate": 'Bearer realm="ohn"'})

        if not self._check_rate_limit():
            return self._error(429, "Rate limit exceeded",
                               {"Retry-After": "30",
                                "X-RateLimit-Limit": RATE_LIMIT_REQUESTS,
                                "X-RateLimit-Remaining": 0})

        # Chaos: intermittent failures so a retry policy can be proven to
        # work rather than assumed. A pipeline that has never seen a 503 has
        # not been tested against one.
        if CHAOS > 0 and random.random() < CHAOS:
            mode = random.choice(["500", "503", "slow"])
            if mode == "slow":
                time.sleep(random.uniform(3, 9))
            else:
                return self._error(int(mode), "Upstream system temporarily unavailable",
                                   {"Retry-After": "5"})

        entity = path[len("/api/v1/"):]
        if entity not in ENDPOINTS:
            return self._error(404, f"Unknown entity '{entity}'. Available: "
                                    f"{', '.join(sorted(ENDPOINTS))}")

        return self._serve_collection(entity, params)

    # ---------------------------------------------------------- paging
    def _serve_collection(self, entity: str, params: dict):
        _, id_field, ts_field = ENDPOINTS[entity]
        rows = STORE.get(entity, [])

        # --- incremental filter ---------------------------------------
        updated_since = params.get("updated_since")
        if updated_since:
            normalised = updated_since.replace("T", " ").replace("Z", "").strip()
            rows = [r for r in rows if (r.get(ts_field) or "") > normalised]

        hospital = params.get("hospital_id")
        if hospital:
            rows = [r for r in rows if r.get("hospital_id") == hospital]

        total = len(rows)

        # --- limit ------------------------------------------------------
        try:
            limit = min(MAX_PAGE_SIZE, max(1, int(params.get("limit", DEFAULT_PAGE_SIZE))))
        except ValueError:
            return self._error(400, "limit must be an integer")

        # --- cursor -----------------------------------------------------
        start = 0
        cursor = params.get("cursor")
        if cursor:
            decoded = decode_cursor(cursor)
            if decoded is None:
                return self._error(400, "Malformed cursor")
            c_ts, c_id = decoded
            # Seek past the last record the client saw. Keyset pagination,
            # not OFFSET — an offset would shift under any concurrent write
            # and the pipeline would skip rows without ever erroring.
            for ix, r in enumerate(rows):
                if ((r.get(ts_field) or ""), str(r.get(id_field) or "")) > (c_ts, c_id):
                    start = ix
                    break
            else:
                start = len(rows)

        page = rows[start:start + limit]
        has_more = start + limit < len(rows)

        next_url = None
        next_cursor = None
        if has_more and page:
            last = page[-1]
            next_cursor = encode_cursor(str(last.get(ts_field) or ""),
                                        str(last.get(id_field) or ""))
            host = self.headers.get("Host", "localhost")
            qs = [f"limit={limit}", f"cursor={next_cursor}"]
            if updated_since:
                qs.append(f"updated_since={updated_since}")
            if hospital:
                qs.append(f"hospital_id={hospital}")
            next_url = f"http://{host}/api/v1/{entity}?" + "&".join(qs)

        return self._respond(200, {
            "data": page,
            "pagination": {
                "returned": len(page),
                "total_matching": total,
                "limit": limit,
                "has_more": has_more,
                "next_cursor": next_cursor,
                "next_url": next_url,
            },
            "meta": {
                "entity": entity,
                "watermark_field": ts_field,
                "server_time": datetime.now(timezone.utc).isoformat(),
            },
        }, {"X-Total-Count": total})


# =====================================================================
# MAIN
# =====================================================================

def main():
    global CHAOS
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="./data")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--chaos", type=float, default=0.0,
                    help="Probability of an injected 500/503/slow response (0..1)")
    args = ap.parse_args()

    CHAOS = args.chaos
    print("OHN Pharmacy & Facility API")
    load_store(args.data)
    if CHAOS:
        print(f"  chaos mode ON — {CHAOS:.0%} of requests will fail or stall")

    print(f"\n  listening on http://{args.host}:{args.port}")
    print(f"  token:   POST /oauth/token  (client_id=fabric)")
    print(f"  data:    GET  /api/v1/{{{', '.join(sorted(ENDPOINTS))}}}")
    print(f"  health:  GET  /health\n")

    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
