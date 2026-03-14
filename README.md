# Basegrow Admin Backend (Django + DRF)

Location: `/Users/prateekrajak/Desktop/Basegrow/basegrow-admin-be`

## Setup

```bash
cd /Users/prateekrajak/Desktop/Basegrow/basegrow-admin-be
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python manage.py runserver 0.0.0.0:8000
```

## Migrated API paths

- `POST /api/login`
- `GET|POST /api/mailcamp/clusters`
- `GET /api/mailcamp/fetch-campaign-data`
- `POST /api/mailcamp/get-cluster-data`
- `POST /api/mailcamp/domain-data-by-campaign`
- `POST /api/mailcamp/cluster-data-by-campaign`
- `POST /api/ongage/matrix`
- `POST /api/ongage/events`
- `POST /api/ongage/event-date-data`
- `POST /api/ongage/events-name-data`
- `POST /api/ongage/segments`
- `GET /api/ongage/get-esp-connection`
- `POST /api/ongage/event-status`
- `POST /api/ongage/segement-count`
- `POST /api/segment-count` (alias)
- `GET /api/spreadsheet-url`
- `POST /api/import`

## Legacy scripts

Existing backend scripts were copied as-is to:

- `legacy_scripts/deleteUserInteraction.ts`
- `legacy_scripts/fetchMailcamp.ts`
- `legacy_scripts/generateNextUserList.ts`
- `legacy_scripts/helpers/*`

These are preserved for migration reference and can be converted to Django management commands next.
