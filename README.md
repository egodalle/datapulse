# DataPulse - E-commerce Analytics Platform

🚀 **Live Demo**: [datapulsestore.lovable.app](https://datapulsestore.lovable.app/)

DataPulse is a SaaS platform that aggregates data from multiple e-commerce stores (Shopify, Amazon, Lazada, Shopee) and provides unified KPI dashboards.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   E-commerce    │────▶│     Airbyte     │────▶│   PostgreSQL    │
│    Platforms    │     │   (Extraction)  │     │  (Data Warehouse)│
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│       UI        │◀────│     FastAPI     │◀────│       dbt       │
│   (Dashboard)   │     │    (Backend)    │     │ (Transformation)│
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| **Extraction** | Airbyte |
| **Data Warehouse** | PostgreSQL |
| **Transformation** | dbt |
| **Backend API** | FastAPI |
| **Frontend** | [Lovable App](https://datapulsestore.lovable.app/) |

## Project Structure

```
datapulse/
├── api/                      # FastAPI backend
│   ├── main.py              # Application entry point
│   ├── routers/             # API endpoints
│   │   ├── health.py        # Health checks
│   │   ├── kpis.py          # KPI endpoints
│   │   └── stores.py        # Store connections
│   ├── services/            # Business logic
│   ├── models/              # Pydantic schemas
│   └── core/                # Config & database
│
├── datapulse_dbt/           # dbt project
│   ├── models/
│   │   ├── staging/         # Cleaned source data
│   │   │   ├── shopify/
│   │   │   ├── amazon/
│   │   │   ├── lazada/
│   │   │   └── shopee/
│   │   ├── intermediate/    # Unified models
│   │   └── marts/           # KPI aggregations
│   │       └── kpis/
│   ├── macros/              # Reusable SQL
│   └── seeds/               # Static data
│
└── venv/                    # Python environment
```

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 17
- Airbyte (Docker)
- dbt-core + dbt-postgres

### Setup

1. **Activate virtual environment**
   ```bash
   cd datapulse
   source venv/bin/activate
   ```

2. **Install API dependencies**
   ```bash
   pip install -r api/requirements.txt
   ```

3. **Run dbt models**
   ```bash
   cd datapulse_dbt
   dbt run
   ```

4. **Start the API**
   ```bash
   cd api
   uvicorn main:app --reload
   ```

5. **Access API docs**
   - Swagger UI: http://localhost:6000/docs
   - ReDoc: http://localhost:6000/redoc

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/kpis/dashboard` | Main dashboard summary |
| `GET /api/v1/kpis/platforms` | Platform overview metrics |
| `GET /api/v1/kpis/daily` | Daily KPI snapshots |
| `GET /api/v1/kpis/revenue` | Revenue by platform |
| `GET /api/v1/kpis/products` | Product performance |
| `GET /api/v1/stores/` | List store connections |
| `POST /api/v1/stores/connect` | Connect new store |

## KPIs Tracked

- **Revenue**: Total, by platform, growth rates, AOV
- **Orders**: Volume, fulfillment rate, cancellation rate
- **Products**: Top sellers, performance tiers, velocity
- **Trends**: Daily, weekly, monthly comparisons

## Connecting Stores

### Shopify
Uses native Airbyte connector. Requires:
- Store URL
- Admin API access token

### Amazon
Uses Amazon Seller Partner API connector. Requires:
- Seller ID
- MWS credentials

### Lazada / Shopee
Custom connectors using Open Platform APIs. Requires:
- App Key
- App Secret
- Access Token

## Development

### Running dbt
```bash
cd datapulse_dbt
dbt run              # Run all models
dbt run --select staging    # Run staging only
dbt run --select marts      # Run marts only
dbt test             # Run tests
dbt docs generate    # Generate documentation
```

### Running API
```bash
cd api
uvicorn main:app --reload --host 0.0.0.0 --port 6000
```

## License

MIT

