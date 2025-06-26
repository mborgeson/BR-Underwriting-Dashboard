# Database Implementation Guide

## Overview

The B&R Capital Dashboard uses PostgreSQL as its primary database for storing comprehensive underwriting model data with historical tracking and version control.

## Architecture

### Database Design Principles
- **Partitioning**: Tables partitioned by deal stage for performance
- **Historical Tracking**: Complete version history with timestamps
- **Data Integrity**: Foreign key constraints and data validation
- **Performance**: Optimized indexing for common query patterns
- **Scalability**: Designed to handle 1000+ properties with ~900 metrics each

### Schema Structure

```
comprehensive_underwriting_model_data/
├── properties (metadata table)
├── underwriting_data (main partitioned table)
│   ├── underwriting_data_dead_deals
│   ├── underwriting_data_initial_uw_review
│   ├── underwriting_data_active_uw_review
│   ├── underwriting_data_under_contract
│   ├── underwriting_data_closed_deals
│   └── underwriting_data_realized_deals
├── annual_cashflows (time series data)
├── rent_comparables (comparable data)
├── sales_comparables (comparable data)
└── extraction_metadata (extraction metrics)
```

## Data Model

### Core Tables

#### properties
- **Purpose**: Property metadata and basic information
- **Key Fields**: property_id, property_name, city, state, market
- **Relationships**: One-to-many with underwriting_data

#### underwriting_data (Partitioned by deal_stage)
- **Purpose**: Main underwriting metrics and financial data
- **Partitions**: 6 partitions by deal stage for performance
- **Key Fields**: ~70 core financial metrics
- **Version Tracking**: Automatic version numbering with latest flag

#### annual_cashflows
- **Purpose**: Year-by-year cashflow projections
- **Structure**: One row per property per year (typically 5 years)
- **Key Fields**: gross_income, expenses, noi, debt_service, cash_flow

#### rent_comparables / sales_comparables
- **Purpose**: Market comparable data
- **Structure**: Multiple rows per property extraction
- **Key Fields**: comp_name, price/rent, distance, units

### Version Tracking System

```sql
-- Automatic version management
CREATE TRIGGER manage_underwriting_versions
    BEFORE INSERT ON underwriting_data
    FOR EACH ROW
    EXECUTE FUNCTION manage_version_numbering();
```

- Each new extraction creates a new version
- Previous versions marked as `is_latest_version = FALSE`
- Complete historical audit trail maintained

## Setup Instructions

### 1. Prerequisites

```bash
# Install PostgreSQL
sudo apt update
sudo apt install postgresql postgresql-contrib

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Install Python dependencies
pip install psycopg2-binary structlog
```

### 2. Environment Configuration

Create `.env` file or set environment variables:

```bash
# Database connection
export DB_HOST=localhost
export DB_PORT=5432
export DB_USERNAME=postgres
export DB_PASSWORD=your_password
export DB_NAME=comprehensive_underwriting_model_data

# Connection pool settings
export DB_MIN_CONNECTIONS=2
export DB_MAX_CONNECTIONS=10
export DB_CONNECTION_TIMEOUT=30
export DB_QUERY_TIMEOUT=300
```

### 3. Database Initialization

```bash
# Test prerequisites and setup database
python setup_database.py

# With specific options
python setup_database.py --reset --test --load-sample-data
```

### 4. Validation

```bash
# Test database structure
python setup_database.py --validate

# Test code structure (no DB required)
python test_database_setup.py
```

## Usage Examples

### Loading Extraction Data

```python
from src.database.data_loader import DataLoader

# Initialize loader
loader = DataLoader()

# Load single extraction
extraction_id = loader.load_extraction_data(
    extraction_data=your_data_dict,
    deal_stage="active_uw_review",
    metadata=extraction_metadata
)

# Load batch results
extraction_ids = loader.load_batch_extraction_results(
    "data/batch_extractions/batch_results_20250626.json"
)
```

### Querying Data

```python
from src.database.connection import get_cursor

# Get latest data for all properties
with get_cursor() as cursor:
    cursor.execute("""
        SELECT property_name, purchase_price, levered_returns_irr
        FROM latest_underwriting_data
        WHERE purchase_price IS NOT NULL
        ORDER BY purchase_price DESC
    """)
    results = cursor.fetchall()

# Get property history
with get_cursor() as cursor:
    cursor.execute("""
        SELECT version_number, extraction_timestamp, purchase_price
        FROM property_history
        WHERE property_name = %s
    """, ("Emparrado",))
    history = cursor.fetchall()
```

### Using Views

```sql
-- Portfolio summary by stage
SELECT * FROM portfolio_summary;

-- Latest data only
SELECT * FROM latest_underwriting_data 
WHERE market = 'Phoenix';

-- Property version history
SELECT * FROM property_history 
WHERE property_name = 'Emparrado';
```

## Performance Optimization

### Indexing Strategy
- Primary keys on all tables (UUID)
- Foreign key indexes for joins
- Composite indexes for common query patterns
- GIN indexes for text search

### Query Optimization
- Use partitioned queries when filtering by deal_stage
- Leverage latest_underwriting_data view for current data
- Use specific property_id when querying historical data

### Connection Management
- Connection pooling with configurable min/max connections
- Automatic connection cleanup
- Query timeout protection

## Maintenance

### Backup Strategy
```bash
# Full database backup
pg_dump comprehensive_underwriting_model_data > backup_$(date +%Y%m%d).sql

# Restore from backup
psql comprehensive_underwriting_model_data < backup_20250626.sql
```

### Database Monitoring
```sql
-- Check database size
SELECT pg_size_pretty(pg_database_size('comprehensive_underwriting_model_data'));

-- Monitor active connections
SELECT count(*) FROM pg_stat_activity 
WHERE datname = 'comprehensive_underwriting_model_data';

-- Check table sizes
SELECT schemaname, tablename, 
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## Migration Management

### Adding New Fields
```python
# 1. Update schema.py with new field
# 2. Create migration in migrations.py
# 3. Run migration
from src.database.migrations import MigrationManager

manager = MigrationManager()
manager.run_migrations()
```

### Schema Updates
- All schema changes managed through migration system
- Automatic rollback capability
- Version tracking for database structure

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Check PostgreSQL service: `sudo systemctl status postgresql`
   - Verify connection parameters in environment variables
   - Check firewall settings

2. **Permission Denied**
   - Ensure user has CREATE DATABASE permissions
   - Check pg_hba.conf authentication settings

3. **Schema Errors**
   - Run validation: `python setup_database.py --validate`
   - Check migration status
   - Reset if necessary: `python setup_database.py --reset`

4. **Performance Issues**
   - Check indexes: Query `pg_indexes` table
   - Monitor connections: Use `pg_stat_activity`
   - Analyze query plans: Use `EXPLAIN ANALYZE`

### Diagnostic Queries
```sql
-- Check database statistics
SELECT * FROM pg_stat_database 
WHERE datname = 'comprehensive_underwriting_model_data';

-- Check table access patterns
SELECT schemaname, tablename, seq_scan, seq_tup_read, 
       idx_scan, idx_tup_fetch
FROM pg_stat_user_tables;

-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes;
```

## Integration with Extraction System

The database integrates seamlessly with the Phase 2 extraction system:

1. **Automatic Loading**: Batch extraction results automatically loaded
2. **Version Management**: Each file update creates new version
3. **Error Tracking**: Extraction metadata stored for debugging
4. **Data Validation**: Type conversion and null handling

## Next Phase Integration

Prepared for Phase 4 (Monitoring) and Phase 5 (Dashboard):
- Views optimized for dashboard queries
- Aggregation tables for performance
- Real-time data access patterns
- Historical trend analysis capability