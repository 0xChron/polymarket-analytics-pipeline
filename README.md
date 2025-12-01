# polydash etl

Automated ETL pipeline for ingesting Polymarket Events and Markets data into postgres.

## Overview

This pipeline fetches real-time data from Polymarket's API and stores it in a PostgreSQL database. It automatically removes closed/inactive markets and maintains only active prediction market data.

**Key Features:**
- Single API call strategy (events with nested markets)
- Automatic cleanup of closed events/markets
- Optimized bulk inserts using `execute_values()`
- GitHub Actions scheduling (every 12 hours)
- Supabase free tier optimized

## Database Schema

### Events Table
Stores Polymarket prediction market events.

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | TEXT (PK) | Unique event identifier |
| `slug` | TEXT | URL-friendly event name |
| `title` | TEXT | Event title |
| `description` | TEXT | Event description |
| `end_date` | TIMESTAMP | When event closes |
| `image` | TEXT | Event image URL |
| `new` | BOOLEAN | Recently created flag |
| `liquidity` | NUMERIC | Total liquidity |
| `volume` | NUMERIC | Total volume traded |
| `volume24hr` | NUMERIC | 24-hour volume |
| `categories` | JSONB | Event categories/tags |
| `fetch_date` | TIMESTAMP | Last updated timestamp |

**Indexes:**
- `idx_events_fetch_date` - Query by update time
- `idx_events_volume24hr` - Sort by trading activity
- `idx_events_categories` - GIN index for category filtering

### Markets Table
Stores individual prediction markets (linked to events via foreign key).

| Column | Type | Description |
|--------|------|-------------|
| `market_id` | TEXT (PK) | Unique market identifier |
| `event_id` | TEXT (FK) | References `events.event_id` |
| `slug` | TEXT | URL-friendly market name |
| `question` | TEXT | Market question |
| `group_item_title` | TEXT | Group item title (if applicable) |
| `new` | BOOLEAN | Recently created flag |
| `liquidity` | NUMERIC | Market liquidity |
| `volume` | NUMERIC | Total volume traded |
| `volume24hr` | NUMERIC | 24-hour volume |
| `outcome_yes_price` | NUMERIC | Yes outcome price |
| `outcome_no_price` | NUMERIC | No outcome price |
| `one_day_price_change` | NUMERIC | 24-hour price change |
| `fetch_date` | TIMESTAMP | Last updated timestamp |

**Indexes:**
- `idx_markets_event_id` - Join with events table
- `idx_markets_fetch_date` - Query by update time
- `idx_markets_volume24hr` - Sort by trading activity

**Foreign Key:** `ON DELETE CASCADE` - Deleting an event automatically removes its markets.

## Getting Started

### Prerequisites
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- PostgreSQL database (Supabase recommended)

### Installation

1. **Clone the repository:**
    ```bash
    git clone https://github.com/0xChron/polymarket-analytics-pipeline.git
    cd polymarket-analytics-pipeline
    ```

2. **Install dependencies**
    ```bash
    uv sync
    ```

3. **Configure environment variables:**
    Create a .env file in the project root directory:
    ```bash
    DB_NAME=your_database_name
    DB_USER=your_database_user
    DB_PASSWORD=your_database_password
    DB_HOST=your_database_host
    DB_PORT=5432
    ```

4. **Run the pipeline**
    ```bash
    uv run python src/main.py
    ```

## Project Structure

```bash
polymarket-analytics-pipeline/
├── .github/
│   └── workflows/
│       └── ingest.yml
├── src/
│   ├── config.py   
│   ├── database.py    
│   ├── api_client.py 
│   ├── transformers.py
│   └── main.py 
├── .env     
├── .gitignore
├── pyproject.toml
└── README.md
```