# DuckLake-Lab: Exploring Modern Data Lakehouse Architecture

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.1.3-orange.svg)](https://duckdb.org/)

A practical implementation of a Data Lakehouse architecture using DuckDB, PostgreSQL, and MinIO. 

## 🔍 Overview

Implementation of a Lakehouse Architecture that combines the flexibility of Data Lakes with the performance and structure of Data Warehouses. It uses:

- **DuckDB** as the query engine for fast analytical queries. Optimized for OLAP queries, columnar execution.
- **PostgreSQL** as the catalog database for metadata management. Fast metadata lookups without scanning data.
- **MinIO** as object storage for parquet files (S3-compatible). Scalable, cost-effective storage layer.
- **Docker Compose** for simple deployment

### Features

- **Catalog-Based Metadata Management**: Track table schemas, locations, and lineage in PostgreSQL
- **Separation of Storage and Compute**: Query data stored in MinIO using DuckDB without moving data
- **Parquet-Based Storage**: Efficient columnar storage format optimized for analytics
- **Multiple Query Interfaces**: Python API, CLI, and Trino support
- **Containerized Infrastructure**: Easy setup with Docker Compose
- **Scalable Architecture**: Foundation for production lakehouse implementations

### Project Structure

```
ducklake-project/
├── data/                       # Data files directory
│   └── popolazione_residente.parquet
├── scripts/
│   ├── init_catalog.py         # Initialize catalog tables
│   ├── ingest_data.py          # Data ingestion pipeline
│   └── cli_query.py            # Interactive SQL CLI
├── .gitignore
├── .python-version
├── docker-compose.yml          # Infrastructure definition
├── pyproject.toml              # Python dependencies and metadata
├── README.md
├── Taskfile                    # Command runner file
└── uv.lock
```

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Query Engines                           │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │  DuckDB  │  │  Python API  │  │    Trino     │           │
│  └────┬─────┘  └───────┬──────┘  └────────┬─────┘           │
└───────┼────────────────┼──────────────────┼─────────────────┘
        │                │                  │
        │         ┌──────┴──────────────────┘
        │         │
┌───────▼─────────▼───────────────────────────────────────────┐
│              Catalog (PostgreSQL)                           │
│  • Table Metadata    • Schemas    • Partitions              │
│  • Namespaces        • Snapshots  • Lineage                 │
└─────────────────────────────────────────────────────────────┘
                        │
                        │ References
                        │
┌───────────────────────▼─────────────────────────────────────┐
│              Object Storage (MinIO/S3)                      │
│  s3://lakehouse/tables/default/popolazione_residente/       │
│    └── data.parquet                                         │
└─────────────────────────────────────────────────────────────┘
```

## 🕹️ Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/ducklake-lab.git
cd ducklake-lab
```

### 2. Start Infrastructure

```bash
uv run task start-container
```

This starts:
- PostgreSQL (port 5432) - Catalog database
- MinIO (ports 9000, 9001) - Object storage

### 3. Initialize Catalog and Ingest Data

Initialize the catalog database with necessary tables for DuckLake and then load the data.

```bash
uv run task load-data
```

### 4. CLI Tool 

Open an interactive SQL shell powered by DuckDB with:
- Meta-commands (\l, \d, \h, \q)
- Direct SQL query execution
- Automatic table name resolution
- Query history

```bash
uv run task cli
```

## 🛠️ Usage

### Dataset

This project includes an example dataset of **Italian municipality population data (2025). Source: ISTAT.**:

- `codice_comune`: Municipality code
- `nome_comune`: Municipality name
- `tot_maschi`: Male population
- `tot_femmine`: Female population
- `residenti`: Total residents

### MinIO Console

Access the MinIO web interface at [http://localhost:9001](http://localhost:9001)

- **Username**: `minioadmin`
- **Password**: `minioadmin`

Browse buckets, upload files, and manage object storage.

### Catalog Schema

The PostgreSQL catalog manages metadata through these tables:

**`namespaces`**
Logical groupings for tables (like databases/schemas)

**`tables`**
Table metadata including schema, location, and properties

**`partitions`**
Partition specifications and file locations

**`snapshots`**
Version history for time travel capabilities (future enhancement)

## 🤝🏻 Acknowledgments

- [DuckDB](https://duckdb.org/) - Fast in-process analytical database
- [MinIO](https://min.io/) - High-performance object storage
- [Trino](https://trino.io/) - Distributed SQL query engine
- [Apache Parquet](https://parquet.apache.org/) - Columnar storage format

---

**Built with ❤️ by Jacopo Sardellini**
