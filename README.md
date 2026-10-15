# Employee Attendance & Analytics API

FastAPI + MongoDB (PyMongo). All code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env            # or export MONGO_URI / MONGO_DB (real env vars take priority)
python sample_seed.py           # optional: loads the 9 sample documents
uvicorn app.main:app --port 8000
```
Needs MongoDB 6.0+ (uses `$densify`, `$setWindowFields`, `$documents`). Indexes are created at startup, idempotently.

## Notes
- Every endpoint in `openapi.yaml` is implemented. Nothing is missing.
- Starter defects and fixes are in `REVIEW.md`; design answers are in `DECISIONS.md`.
- Instants are truncated to whole seconds (R1) before they are stored. All half-up rounding uses `Decimal` in Python, or decimal arithmetic plus `$floor(x + 0.5)` inside MongoDB, because `$round` rounds half to even.
- Analytics are MongoDB aggregations. `working_days` (R7) is plain calendar arithmetic and does not loop over documents.
- Concurrency: unique indexes guard creates and punch-ins. Punch-out is a conditional update (`punch_out: null`). PATCH uses optimistic concurrency on status, punch times and the history length, so the loser gets 409 instead of overwriting history.
- Interpretations: the trend range limit counts days inclusively (a 92-day quarter is allowed, 93 days is 422). "A request that changes nothing" means `status`, `punch_in` and `punch_out` would all stay the same. `moving_avg_7d` averages the reported (4-decimal) daily rates.
- Tested locally against MongoDB 9.0 with edge-case fixtures (overnight shift, ties at the cutoff, weekend records, mid-month joiner, employee with no logs, legacy records, race conditions) and a 100k-record dataset. Every explain variant shows `IXSCAN` and no `COLLSCAN`.
