"""
Database Migration System for B&R Capital Dashboard

This module handles database migrations, initialization, and schema updates
to ensure consistent database state across environments.

Features:
- Initial database setup
- Version tracking
- Migration rollback capability
- Data integrity checks
- Environment-specific configurations
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import structlog
from .connection import get_cursor, DatabaseConfig, initialize_database, test_connection
from .schema import SchemaManager

logger = structlog.get_logger().bind(component="DatabaseMigrations")

class MigrationManager:
    """Manages database migrations and initialization"""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.schema_manager = SchemaManager()
        
    def initialize_database(self) -> bool:
        """
        Initialize the database from scratch
        
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("initializing_database", database=self.config.database_name)
        
        try:
            # 1. Test connection
            if not self._test_connection():
                logger.error("database_connection_failed")
                return False
            
            # 2. Create database if it doesn't exist
            if not self._create_database_if_not_exists():
                logger.error("database_creation_failed")
                return False
            
            # 3. Initialize connection with the new database
            initialize_database(self.config)
            
            # 4. Create schema
            self.schema_manager.create_database_schema()
            
            # 5. Initialize migration tracking
            self._initialize_migration_tracking()
            
            # 6. Record initial migration
            self._record_migration("001_initial_schema", "Initial database schema creation")
            
            logger.info("database_initialization_completed")
            return True
            
        except Exception as e:
            logger.error("database_initialization_failed", error=str(e))
            return False
    
    def _test_connection(self) -> bool:
        """Test database server connection"""
        try:
            # Test connection to PostgreSQL server (not specific database)
            test_config = DatabaseConfig()
            test_config.database_name = "postgres"  # Connect to default postgres database
            
            initialize_database(test_config)
            return test_connection()
            
        except Exception as e:
            logger.error("database_server_connection_failed", error=str(e))
            return False
    
    def _create_database_if_not_exists(self) -> bool:
        """Create the database if it doesn't exist"""
        try:
            # Connect to postgres database to create our target database
            # Use a direct connection without transaction for CREATE DATABASE
            from .connection import get_connection
            
            with get_connection() as conn:
                conn.autocommit = True  # Required for CREATE DATABASE
                cursor = conn.cursor()
                
                # Check if database exists
                cursor.execute("""
                    SELECT 1 FROM pg_database WHERE datname = %s
                """, (self.config.database_name,))
                
                if cursor.fetchone():
                    logger.info("database_already_exists", database=self.config.database_name)
                    cursor.close()
                    return True
                
                # Create database (cannot be in a transaction)
                cursor.execute(f'CREATE DATABASE "{self.config.database_name}"')
                
                cursor.close()
                logger.info("database_created", database=self.config.database_name)
                return True
                
        except Exception as e:
            logger.error("database_creation_error", error=str(e))
            return False
    
    def _initialize_migration_tracking(self):
        """Create migration tracking table"""
        with get_cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    migration_id SERIAL PRIMARY KEY,
                    migration_name VARCHAR(255) NOT NULL UNIQUE,
                    description TEXT,
                    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    rollback_sql TEXT,
                    checksum VARCHAR(64)
                );
            """)
            
            # Create index for fast lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_schema_migrations_name 
                ON schema_migrations(migration_name);
            """)
    
    def _record_migration(self, migration_name: str, description: str, 
                         rollback_sql: Optional[str] = None):
        """Record a migration in the tracking table"""
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO schema_migrations (migration_name, description, rollback_sql)
                VALUES (%s, %s, %s)
                ON CONFLICT (migration_name) DO NOTHING
            """, (migration_name, description, rollback_sql))
    
    def run_migrations(self) -> bool:
        """Run any pending migrations"""
        logger.info("checking_for_pending_migrations")
        
        try:
            # Get list of applied migrations
            applied_migrations = self._get_applied_migrations()
            
            # Define available migrations
            available_migrations = self._get_available_migrations()
            
            # Find pending migrations
            pending_migrations = [
                migration for migration in available_migrations
                if migration['name'] not in applied_migrations
            ]
            
            if not pending_migrations:
                logger.info("no_pending_migrations")
                return True
            
            # Apply pending migrations
            for migration in pending_migrations:
                logger.info("applying_migration", migration=migration['name'])
                
                if self._apply_migration(migration):
                    self._record_migration(
                        migration['name'], 
                        migration['description'],
                        migration.get('rollback_sql')
                    )
                    logger.info("migration_applied", migration=migration['name'])
                else:
                    logger.error("migration_failed", migration=migration['name'])
                    return False
            
            logger.info("all_migrations_applied", count=len(pending_migrations))
            return True
            
        except Exception as e:
            logger.error("migration_process_failed", error=str(e))
            return False
    
    def _get_applied_migrations(self) -> List[str]:
        """Get list of already applied migrations"""
        try:
            with get_cursor() as cursor:
                cursor.execute("""
                    SELECT migration_name FROM schema_migrations 
                    ORDER BY applied_at
                """)
                return [row[0] for row in cursor.fetchall()]
        except:
            # Migration table doesn't exist yet
            return []
    
    def _get_available_migrations(self) -> List[Dict[str, str]]:
        """Get list of available migrations"""
        return [
            {
                'name': '001_initial_schema',
                'description': 'Initial database schema creation',
                'sql': '',  # Already handled by schema manager
                'rollback_sql': 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'
            },
            {
                'name': '002_add_missing_fields',
                'description': 'Add any missing fields discovered during testing',
                'sql': self._get_missing_fields_migration_sql(),
                'rollback_sql': ''
            }
        ]
    
    def _apply_migration(self, migration: Dict[str, str]) -> bool:
        """Apply a specific migration"""
        try:
            if migration['sql']:
                with get_cursor() as cursor:
                    cursor.execute(migration['sql'])
            return True
        except Exception as e:
            logger.error("migration_application_failed", 
                        migration=migration['name'], error=str(e))
            return False
    
    def _get_missing_fields_migration_sql(self) -> str:
        """Generate SQL for adding any missing fields"""
        return """
        -- Add any missing fields that were discovered during Phase 2 testing
        -- This migration can be expanded as needed
        
        -- Example: Add fields that might have been missed
        DO $$ BEGIN
            -- Add example field if it doesn't exist
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'underwriting_data' 
                AND column_name = 'example_field'
            ) THEN
                ALTER TABLE underwriting_data ADD COLUMN example_field NUMERIC(15,2);
            END IF;
        END $$;
        """
    
    def rollback_migration(self, migration_name: str) -> bool:
        """Rollback a specific migration"""
        logger.warning("attempting_migration_rollback", migration=migration_name)
        
        try:
            with get_cursor() as cursor:
                # Get rollback SQL
                cursor.execute("""
                    SELECT rollback_sql FROM schema_migrations 
                    WHERE migration_name = %s
                """, (migration_name,))
                
                result = cursor.fetchone()
                if not result or not result[0]:
                    logger.error("no_rollback_sql_found", migration=migration_name)
                    return False
                
                # Execute rollback
                cursor.execute(result[0])
                
                # Remove from migration tracking
                cursor.execute("""
                    DELETE FROM schema_migrations WHERE migration_name = %s
                """, (migration_name,))
                
                logger.info("migration_rolled_back", migration=migration_name)
                return True
                
        except Exception as e:
            logger.error("migration_rollback_failed", 
                        migration=migration_name, error=str(e))
            return False
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Get current migration status"""
        try:
            applied_migrations = self._get_applied_migrations()
            available_migrations = self._get_available_migrations()
            
            pending_migrations = [
                migration['name'] for migration in available_migrations
                if migration['name'] not in applied_migrations
            ]
            
            return {
                'applied_migrations': applied_migrations,
                'pending_migrations': pending_migrations,
                'total_available': len(available_migrations),
                'database_name': self.config.database_name,
                'schema_version': self.schema_manager.schema_version
            }
            
        except Exception as e:
            logger.error("migration_status_check_failed", error=str(e))
            return {}
    
    def validate_database_integrity(self) -> Dict[str, Any]:
        """Validate database integrity and structure"""
        logger.info("validating_database_integrity")
        
        try:
            with get_cursor() as cursor:
                validation_results = {
                    'tables_exist': self._check_tables_exist(cursor),
                    'indexes_exist': self._check_indexes_exist(cursor),
                    'constraints_valid': self._check_constraints_valid(cursor),
                    'data_types_correct': self._check_data_types(cursor),
                    'partitions_exist': self._check_partitions_exist(cursor)
                }
                
                validation_results['overall_valid'] = all(validation_results.values())
                
                if validation_results['overall_valid']:
                    logger.info("database_integrity_validation_passed")
                else:
                    logger.warning("database_integrity_validation_failed", 
                                 results=validation_results)
                
                return validation_results
                
        except Exception as e:
            logger.error("database_integrity_validation_error", error=str(e))
            return {'overall_valid': False, 'error': str(e)}
    
    def _check_tables_exist(self, cursor) -> bool:
        """Check if all required tables exist"""
        required_tables = [
            'properties', 'underwriting_data', 'annual_cashflows',
            'rent_comparables', 'sales_comparables', 'extraction_metadata'
        ]
        
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        
        existing_tables = [row[0] for row in cursor.fetchall()]
        return all(table in existing_tables for table in required_tables)
    
    def _check_indexes_exist(self, cursor) -> bool:
        """Check if key indexes exist"""
        cursor.execute("""
            SELECT COUNT(*) FROM pg_indexes 
            WHERE schemaname = 'public'
        """)
        
        index_count = cursor.fetchone()[0]
        return index_count >= 10  # Should have at least 10 indexes
    
    def _check_constraints_valid(self, cursor) -> bool:
        """Check if constraints are valid (adjusted for partitioned tables)"""
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.table_constraints 
            WHERE constraint_type IN ('PRIMARY KEY', 'UNIQUE') AND table_schema = 'public'
        """)
        
        constraint_count = cursor.fetchone()[0]
        # Note: Foreign keys removed due to PostgreSQL partitioned table limitations
        return constraint_count >= 7  # Should have primary keys and unique constraints
    
    def _check_data_types(self, cursor) -> bool:
        """Check if critical columns have correct data types"""
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'underwriting_data' 
            AND column_name IN ('extraction_id', 'property_id', 'purchase_price')
        """)
        
        columns = dict(cursor.fetchall())
        return (
            columns.get('extraction_id') == 'uuid' and
            columns.get('property_id') == 'uuid' and
            columns.get('purchase_price') == 'numeric'
        )
    
    def _check_partitions_exist(self, cursor) -> bool:
        """Check if table partitions exist"""
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name LIKE 'underwriting_data_%'
            AND table_type = 'BASE TABLE'
        """)
        
        partition_count = cursor.fetchone()[0]
        return partition_count >= 6  # Should have 6 partitions for deal stages
    
    def reset_database(self) -> bool:
        """Reset the database (drop and recreate schema)"""
        logger.warning("resetting_database", database=self.config.database_name)
        
        try:
            # Drop existing schema
            self.schema_manager.drop_schema()
            
            # Recreate schema
            return self.initialize_database()
            
        except Exception as e:
            logger.error("database_reset_failed", error=str(e))
            return False

# Convenience functions
def setup_database(config: Optional[DatabaseConfig] = None) -> bool:
    """Setup database with initial schema"""
    migration_manager = MigrationManager(config)
    return migration_manager.initialize_database()

def migrate_database(config: Optional[DatabaseConfig] = None) -> bool:
    """Run database migrations"""
    migration_manager = MigrationManager(config)
    return migration_manager.run_migrations()

def validate_database(config: Optional[DatabaseConfig] = None) -> Dict[str, Any]:
    """Validate database integrity"""
    migration_manager = MigrationManager(config)
    return migration_manager.validate_database_integrity()