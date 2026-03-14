# Prithvi API

Prithvi is an agricultural ledger backend built with FastAPI and PostgreSQL. It tracks farmers, crops, input costs, harvests, crop stages, break-even pricing, and portfolio-level economics.

## What It Can Do

- Register farmers and crops
- Log crop input costs
- Log harvests and revenue
- Update crop stages and expected yield
- Compute break-even pricing
- Return farmer-level and crop-level economics
- Provide dashboard summaries, alerts, and full-ledger responses

## Project Files

- [api.py](/D:/Prithvi/api.py): main FastAPI application
- [connect.py](/D:/Prithvi/connect.py): direct database helper script
- [schema.sql](/D:/Prithvi/schema.sql): schema and seed data
- [queries.sql](/D:/Prithvi/queries.sql): useful manual SQL queries
- [Procfile](/D:/Prithvi/Procfile): process command for deployment
- [requirements.txt](/D:/Prithvi/requirements.txt): Python dependencies

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set your environment variables in `.env`:

```env
DB_HOST=localhost
DB_NAME=prithvi test
DB_USER=postgres
DB_PASSWORD=your_password
```

4. Create the database tables using [schema.sql](/D:/Prithvi/schema.sql).

## Run Locally

Start the API:

```bash
uvicorn api:app --reload
```

Open Swagger docs:

- [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

## Recommended Demo Endpoints

- `GET /dashboard/summary`
- `GET /dashboard/fpo-summary`
- `GET /alerts/overview`
- `GET /farmer/{name}/full-ledger`
- `GET /crop/{crop_id}/economics`

## Deployment

The included [Procfile](/D:/Prithvi/Procfile) is ready for platforms that use a single web process:

```procfile
web: uvicorn api:app --host 0.0.0.0 --port $PORT
```

Before deployment:

- make sure `.env` values are supplied through platform environment variables
- ensure PostgreSQL is reachable from the deployed service
- run the schema migration on the target database
- avoid committing `.env` and `__pycache__`

## Current API Surface

Core:

- `GET /`
- `GET /health`
- `GET /farmers`
- `GET /farmer/{name}`
- `GET /farmer/{name}/economics`
- `GET /farmer/{name}/full-ledger`

Crop ledger:

- `GET /crop/{crop_id}`
- `GET /crop/{crop_id}/costs`
- `GET /crop/{crop_id}/harvests`
- `GET /crop/{crop_id}/economics`
- `PATCH /crops/{crop_id}/stage`
- `PATCH /crops/{crop_id}/yield`

Writes:

- `POST /farmers/add`
- `POST /crops/add`
- `POST /costs/add`
- `POST /harvests/add`

Portfolio:

- `GET /dashboard/summary`
- `GET /dashboard/fpo-summary`
- `GET /alerts/overview`
