"""
Database Schema Definition for B&R Capital Dashboard

This module defines the PostgreSQL schema for storing comprehensive
underwriting model data with historical tracking and partitioning.

Schema Design:
- Partitioned by deal_stage for performance
- Historical versioning with extraction timestamps
- Optimized indexing for common queries
- Data type optimization for 1140+ fields
"""

from typing import Dict, List, Any
import structlog
from .connection import get_cursor

logger = structlog.get_logger().bind(component="DatabaseSchema")

class SchemaManager:
    """Manages database schema creation and migration"""
    
    def __init__(self):
        self.schema_version = "1.0.0"
        
    def create_database_schema(self):
        """Create the complete database schema"""
        logger.info("creating_database_schema", version=self.schema_version)
        
        try:
            with get_cursor() as cursor:
                # Create schema
                self._create_extensions(cursor)
                self._create_enums(cursor)
                self._create_main_tables(cursor)
                self._create_partitions(cursor)
                self._create_indexes(cursor)
                self._create_views(cursor)
                self._create_functions(cursor)
                
                logger.info("database_schema_created_successfully")
                
        except Exception as e:
            logger.error("database_schema_creation_failed", error=str(e))
            raise
    
    def _create_extensions(self, cursor):
        """Create required PostgreSQL extensions"""
        logger.info("creating_database_extensions")
        
        extensions = [
            "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";",  # For UUID generation
            "CREATE EXTENSION IF NOT EXISTS \"pg_trgm\";",     # For text search
            "CREATE EXTENSION IF NOT EXISTS \"btree_gin\";",   # For GIN indexes
        ]
        
        for extension in extensions:
            cursor.execute(extension)
    
    def _create_enums(self, cursor):
        """Create enumeration types"""
        logger.info("creating_database_enums")
        
        # Deal stages enum
        cursor.execute("""
            DO $$ BEGIN
                CREATE TYPE deal_stage_enum AS ENUM (
                    'dead_deals',
                    'initial_uw_review',
                    'active_uw_review',
                    'under_contract',
                    'closed_deals',
                    'realized_deals'
                );
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """)
        
        # Data categories enum
        cursor.execute("""
            DO $$ BEGIN
                CREATE TYPE data_category_enum AS ENUM (
                    'general_assumptions',
                    'exit_assumptions',
                    'noi_assumptions',
                    'debt_equity_assumptions',
                    'budget_assumptions',
                    'property_return_metrics',
                    'equity_return_metrics',
                    'unit_mix_assumptions',
                    'rent_comps',
                    'sales_comps',
                    'annual_cashflows'
                );
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """)
    
    def _create_main_tables(self, cursor):
        """Create main database tables"""
        logger.info("creating_main_tables")
        
        # Properties metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                property_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                property_name VARCHAR(255) NOT NULL UNIQUE,
                property_city VARCHAR(100),
                property_state VARCHAR(2),
                property_address TEXT,
                market VARCHAR(100),
                submarket VARCHAR(100),
                county VARCHAR(100),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        
        # Main underwriting data table (partitioned)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS underwriting_data (
                extraction_id UUID DEFAULT uuid_generate_v4(),
                property_id UUID NOT NULL,
                property_name VARCHAR(255) NOT NULL,
                deal_stage deal_stage_enum NOT NULL,
                file_path TEXT NOT NULL,
                extraction_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                file_modified_date TIMESTAMP WITH TIME ZONE,
                file_size_mb NUMERIC(10,2),
                
                -- Version tracking
                version_number INTEGER NOT NULL DEFAULT 1,
                is_latest_version BOOLEAN NOT NULL DEFAULT TRUE,
                
                -- General Assumptions (32 fields)
                year_built INTEGER,
                year_renovated INTEGER,
                location_quality VARCHAR(10),
                building_quality VARCHAR(10),
                units INTEGER,
                avg_square_feet NUMERIC(10,2),
                parking_spaces_covered INTEGER,
                parking_spaces_uncovered INTEGER,
                individually_metered VARCHAR(10),
                current_owner TEXT,
                last_sale_date NUMERIC(20,10),
                last_sale_price NUMERIC(15,2),
                last_sale_price_per_unit NUMERIC(10,2),
                last_sale_cap_rate NUMERIC(8,6),
                building_height NUMERIC(5,1),
                building_type VARCHAR(50),
                project_type VARCHAR(50),
                number_of_buildings NUMERIC(5,1),
                building_zoning VARCHAR(20),
                land_area NUMERIC(10,2),
                parcel_number VARCHAR(100),
                property_latitude VARCHAR(50),
                property_longitude VARCHAR(50),
                property_address_field NUMERIC(10,2),
                property_zip VARCHAR(20),
                
                -- Exit Assumptions (3 fields)
                exit_period_months NUMERIC(5,1),
                exit_cap_rate NUMERIC(8,6),
                sales_transaction_costs NUMERIC(8,6),
                
                -- NOI Assumptions (representative sample - we'll add more)
                empirical_rent NUMERIC(10,2),
                rent_psf NUMERIC(8,4),
                gross_potential_rental_income NUMERIC(12,2),
                concessions NUMERIC(8,6),
                loss_to_lease NUMERIC(8,6),
                vacancy_loss NUMERIC(8,6),
                bad_debts NUMERIC(8,6),
                other_loss NUMERIC(8,6),
                property_management_fee NUMERIC(12,2),
                net_rental_income NUMERIC(12,2),
                parking_income NUMERIC(12,2),
                laundry_income NUMERIC(12,2),
                other_income NUMERIC(12,2),
                effective_gross_income NUMERIC(12,2),
                
                -- Operating Expenses
                advertising_marketing NUMERIC(12,2),
                management_fee NUMERIC(12,2),
                payroll NUMERIC(12,2),
                repairs_maintenance NUMERIC(12,2),
                contract_services NUMERIC(12,2),
                turnover NUMERIC(12,2),
                utilities NUMERIC(12,2),
                insurance NUMERIC(12,2),
                real_estate_taxes NUMERIC(12,2),
                other_expenses NUMERIC(12,2),
                total_operating_expenses NUMERIC(12,2),
                net_operating_income NUMERIC(12,2),
                
                -- Debt and Equity Assumptions (representative sample)
                purchase_price NUMERIC(15,2),
                hard_costs_budget NUMERIC(15,2),
                soft_costs_budget NUMERIC(15,2),
                total_hard_costs NUMERIC(15,2),
                total_soft_costs NUMERIC(15,2),
                total_acquisition_budget NUMERIC(15,2),
                loan_amount NUMERIC(15,2),
                loan_to_cost NUMERIC(8,6),
                loan_to_value NUMERIC(8,6),
                equity_lp_capital NUMERIC(15,2),
                equity_gp_capital NUMERIC(15,2),
                
                -- Return Metrics
                t12_return_on_pp NUMERIC(8,6),
                t12_return_on_cost NUMERIC(8,6),
                levered_returns_irr NUMERIC(8,6),
                levered_returns_moic NUMERIC(8,4),
                basis_unit_at_close NUMERIC(10,2),
                basis_unit_at_exit NUMERIC(10,2),
                
                -- Metadata
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                
                -- Composite primary key including partition key
                PRIMARY KEY (extraction_id, deal_stage)
                -- Note: Foreign key constraint removed for partitioned table
            ) PARTITION BY LIST (deal_stage);
        """)
        
        # Annual cashflows table (for time series data)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS annual_cashflows (
                cashflow_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                extraction_id UUID NOT NULL,
                property_id UUID NOT NULL,
                year_number INTEGER NOT NULL,
                
                -- Cashflow fields
                gross_potential_income NUMERIC(15,2),
                vacancy_loss NUMERIC(15,2),
                effective_gross_income NUMERIC(15,2),
                operating_expenses NUMERIC(15,2),
                net_operating_income NUMERIC(15,2),
                debt_service NUMERIC(15,2),
                before_tax_cash_flow NUMERIC(15,2),
                
                -- Capital items
                capital_improvements NUMERIC(15,2),
                tenant_improvements NUMERIC(15,2),
                leasing_commissions NUMERIC(15,2),
                
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                
                -- Note: Cannot reference partitioned table directly
            );
        """)
        
        # Rent comparables table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rent_comparables (
                rent_comp_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                extraction_id UUID NOT NULL,
                property_id UUID NOT NULL,
                comp_number INTEGER NOT NULL,
                
                -- Comparable data
                comp_name VARCHAR(255),
                comp_address TEXT,
                comp_city VARCHAR(100),
                comp_distance NUMERIC(8,2),
                comp_units INTEGER,
                comp_year_built INTEGER,
                comp_rent_psf NUMERIC(8,2),
                comp_total_rent NUMERIC(10,2),
                
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                
                -- Note: Cannot reference partitioned table directly
            );
        """)
        
        # Sales comparables table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales_comparables (
                sales_comp_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                extraction_id UUID NOT NULL,
                property_id UUID NOT NULL,
                comp_number INTEGER NOT NULL,
                
                -- Comparable data
                comp_name VARCHAR(255),
                comp_address TEXT,
                comp_city VARCHAR(100),
                comp_units INTEGER,
                comp_year_built INTEGER,
                comp_price NUMERIC(15,2),
                comp_price_per_unit NUMERIC(10,2),
                comp_cap_rate NUMERIC(8,6),
                comp_sale_date DATE,
                
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                
                -- Note: Cannot reference partitioned table directly
            );
        """)
        
        # Extraction metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS extraction_metadata (
                metadata_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                extraction_id UUID NOT NULL,
                total_fields_attempted INTEGER NOT NULL,
                successful_extractions INTEGER NOT NULL,
                failed_extractions INTEGER NOT NULL,
                extraction_duration_seconds NUMERIC(8,2),
                error_count INTEGER DEFAULT 0,
                warnings_count INTEGER DEFAULT 0,
                
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                
                -- Note: Cannot reference partitioned table directly
            );
        """)
    
    def _create_partitions(self, cursor):
        """Create table partitions by deal stage"""
        logger.info("creating_table_partitions")
        
        partitions = [
            ("dead_deals", "dead_deals"),
            ("initial_uw_review", "initial_uw_review"),
            ("active_uw_review", "active_uw_review"),
            ("under_contract", "under_contract"),
            ("closed_deals", "closed_deals"),
            ("realized_deals", "realized_deals")
        ]
        
        for partition_name, stage_value in partitions:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS underwriting_data_{partition_name}
                PARTITION OF underwriting_data
                FOR VALUES IN ('{stage_value}');
            """)
    
    def _create_indexes(self, cursor):
        """Create database indexes for performance"""
        logger.info("creating_database_indexes")
        
        indexes = [
            # Properties table indexes
            "CREATE INDEX IF NOT EXISTS idx_properties_name ON properties(property_name);",
            "CREATE INDEX IF NOT EXISTS idx_properties_city_state ON properties(property_city, property_state);",
            "CREATE INDEX IF NOT EXISTS idx_properties_market ON properties(market);",
            
            # Underwriting data indexes
            "CREATE INDEX IF NOT EXISTS idx_underwriting_property_id ON underwriting_data(property_id);",
            "CREATE INDEX IF NOT EXISTS idx_underwriting_extraction_timestamp ON underwriting_data(extraction_timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_underwriting_latest_version ON underwriting_data(is_latest_version) WHERE is_latest_version = TRUE;",
            "CREATE INDEX IF NOT EXISTS idx_underwriting_property_latest ON underwriting_data(property_id, is_latest_version) WHERE is_latest_version = TRUE;",
            
            # Text search indexes
            "CREATE INDEX IF NOT EXISTS idx_properties_name_gin ON properties USING gin(property_name gin_trgm_ops);",
            
            # Cashflows indexes
            "CREATE INDEX IF NOT EXISTS idx_cashflows_extraction_id ON annual_cashflows(extraction_id);",
            "CREATE INDEX IF NOT EXISTS idx_cashflows_property_year ON annual_cashflows(property_id, year_number);",
            
            # Comparables indexes
            "CREATE INDEX IF NOT EXISTS idx_rent_comps_extraction_id ON rent_comparables(extraction_id);",
            "CREATE INDEX IF NOT EXISTS idx_sales_comps_extraction_id ON sales_comparables(extraction_id);",
            
            # Metadata indexes
            "CREATE INDEX IF NOT EXISTS idx_extraction_metadata_extraction_id ON extraction_metadata(extraction_id);"
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
            except Exception as e:
                logger.warning("index_creation_warning", sql=index_sql, error=str(e))
    
    def _create_views(self, cursor):
        """Create database views for common queries"""
        logger.info("creating_database_views")
        
        # Latest data view
        cursor.execute("""
            CREATE OR REPLACE VIEW latest_underwriting_data AS
            SELECT 
                u.*,
                p.property_city,
                p.property_state,
                p.market,
                p.submarket,
                p.county
            FROM underwriting_data u
            JOIN properties p ON u.property_id = p.property_id
            WHERE u.is_latest_version = TRUE;
        """)
        
        # Portfolio summary view
        cursor.execute("""
            CREATE OR REPLACE VIEW portfolio_summary AS
            SELECT 
                deal_stage,
                COUNT(*) as property_count,
                SUM(units) as total_units,
                SUM(purchase_price) as total_purchase_price,
                AVG(last_sale_cap_rate) as avg_cap_rate,
                AVG(levered_returns_irr) as avg_irr
            FROM latest_underwriting_data
            WHERE purchase_price IS NOT NULL
            GROUP BY deal_stage;
        """)
        
        # Property history view
        cursor.execute("""
            CREATE OR REPLACE VIEW property_history AS
            SELECT 
                property_name,
                deal_stage,
                extraction_timestamp,
                version_number,
                purchase_price,
                levered_returns_irr,
                net_operating_income,
                file_path
            FROM underwriting_data
            ORDER BY property_name, extraction_timestamp DESC;
        """)
    
    def _create_functions(self, cursor):
        """Create database functions and triggers"""
        logger.info("creating_database_functions")
        
        # Function to update 'updated_at' timestamp
        cursor.execute("""
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ language 'plpgsql';
        """)
        
        # Trigger for properties table
        cursor.execute("""
            DO $$ BEGIN
                CREATE TRIGGER update_properties_updated_at 
                    BEFORE UPDATE ON properties 
                    FOR EACH ROW 
                    EXECUTE FUNCTION update_updated_at_column();
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """)
        
        # Function to manage version numbering
        cursor.execute("""
            CREATE OR REPLACE FUNCTION manage_version_numbering()
            RETURNS TRIGGER AS $$
            BEGIN
                -- Set all previous versions to not latest
                UPDATE underwriting_data 
                SET is_latest_version = FALSE 
                WHERE property_id = NEW.property_id 
                AND extraction_id != NEW.extraction_id;
                
                -- Set version number
                SELECT COALESCE(MAX(version_number), 0) + 1 
                INTO NEW.version_number
                FROM underwriting_data 
                WHERE property_id = NEW.property_id;
                
                -- Ensure this is marked as latest
                NEW.is_latest_version = TRUE;
                
                RETURN NEW;
            END;
            $$ language 'plpgsql';
        """)
        
        # Trigger for version management
        cursor.execute("""
            DO $$ BEGIN
                CREATE TRIGGER manage_underwriting_versions
                    BEFORE INSERT ON underwriting_data
                    FOR EACH ROW
                    EXECUTE FUNCTION manage_version_numbering();
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """)
    
    def drop_schema(self):
        """Drop the entire schema (use with caution)"""
        logger.warning("dropping_database_schema")
        
        with get_cursor() as cursor:
            # Drop tables in reverse dependency order
            tables = [
                'extraction_metadata',
                'sales_comparables',
                'rent_comparables',
                'annual_cashflows',
                'underwriting_data',
                'properties'
            ]
            
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            
            # Drop views
            views = [
                'latest_underwriting_data',
                'portfolio_summary',
                'property_history'
            ]
            
            for view in views:
                cursor.execute(f"DROP VIEW IF EXISTS {view} CASCADE;")
            
            # Drop functions
            cursor.execute("DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;")
            cursor.execute("DROP FUNCTION IF EXISTS manage_version_numbering() CASCADE;")
            
            # Drop enums
            cursor.execute("DROP TYPE IF EXISTS deal_stage_enum CASCADE;")
            cursor.execute("DROP TYPE IF EXISTS data_category_enum CASCADE;")
            
            logger.info("database_schema_dropped")
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get information about the current schema"""
        with get_cursor() as cursor:
            # Get table information
            cursor.execute("""
                SELECT table_name, 
                       pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC;
            """)
            tables = cursor.fetchall()
            
            # Get view information
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            views = [row[0] for row in cursor.fetchall()]
            
            # Get index information
            cursor.execute("""
                SELECT schemaname, tablename, indexname, indexdef
                FROM pg_indexes 
                WHERE schemaname = 'public'
                ORDER BY tablename, indexname;
            """)
            indexes = cursor.fetchall()
            
            return {
                'schema_version': self.schema_version,
                'tables': [{'name': name, 'size': size} for name, size in tables],
                'views': views,
                'indexes': [{'table': table, 'name': name, 'definition': definition} 
                           for schema, table, name, definition in indexes]
            }