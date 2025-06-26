"""
PostgreSQL Database Connection Management for B&R Capital Dashboard

This module handles database connections, connection pooling, and basic
database operations for the comprehensive underwriting model data.
"""

import os
import logging
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from typing import Optional, Dict, Any
import structlog

# Configure logging
logger = structlog.get_logger().bind(component="DatabaseConnection")

class DatabaseConfig:
    """Database configuration management"""
    
    def __init__(self):
        # Database connection parameters
        self.database_name = os.getenv("DB_NAME", "comprehensive_underwriting_model_data")
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = int(os.getenv("DB_PORT", "5432"))
        self.username = os.getenv("DB_USERNAME", "postgres")
        self.password = os.getenv("DB_PASSWORD", "")
        
        # Connection pool settings
        self.min_connections = int(os.getenv("DB_MIN_CONNECTIONS", "2"))
        self.max_connections = int(os.getenv("DB_MAX_CONNECTIONS", "10"))
        
        # Connection timeout settings
        self.connection_timeout = int(os.getenv("DB_CONNECTION_TIMEOUT", "30"))
        self.query_timeout = int(os.getenv("DB_QUERY_TIMEOUT", "300"))
        
    def get_connection_string(self) -> str:
        """Generate PostgreSQL connection string"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}"
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as dictionary"""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database_name,
            'user': self.username,
            'password': self.password,
            'connect_timeout': self.connection_timeout
        }

class DatabaseConnectionManager:
    """Manages PostgreSQL connections with connection pooling"""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.connection_pool = None
        self._initialize_connection_pool()
        
    def _initialize_connection_pool(self):
        """Initialize the connection pool"""
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.config.min_connections,
                maxconn=self.config.max_connections,
                **self.config.get_connection_params()
            )
            logger.info(
                "database_connection_pool_initialized",
                min_connections=self.config.min_connections,
                max_connections=self.config.max_connections
            )
        except Exception as e:
            logger.error(
                "database_connection_pool_failed",
                error=str(e),
                host=self.config.host,
                database=self.config.database_name
            )
            raise
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        connection = None
        try:
            # Get connection from pool
            connection = self.connection_pool.getconn()
            logger.debug("database_connection_acquired")
            yield connection
            
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error("database_connection_error", error=str(e))
            raise
            
        finally:
            if connection:
                # Return connection to pool
                self.connection_pool.putconn(connection)
                logger.debug("database_connection_released")
    
    @contextmanager
    def get_cursor(self, commit=True):
        """Context manager for database cursors"""
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
                if commit:
                    connection.commit()
                    logger.debug("database_transaction_committed")
            except Exception as e:
                connection.rollback()
                logger.error("database_transaction_rollback", error=str(e))
                raise
            finally:
                cursor.close()
    
    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    logger.info("database_connection_test_successful")
                    return True
                else:
                    logger.error("database_connection_test_failed", result=result)
                    return False
        except Exception as e:
            logger.error("database_connection_test_error", error=str(e))
            return False
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get database information"""
        try:
            with self.get_cursor() as cursor:
                # Get PostgreSQL version
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                
                # Get database size
                cursor.execute("""
                    SELECT pg_size_pretty(pg_database_size(%s))
                """, (self.config.database_name,))
                db_size = cursor.fetchone()[0]
                
                # Get connection count
                cursor.execute("""
                    SELECT count(*) FROM pg_stat_activity 
                    WHERE datname = %s
                """, (self.config.database_name,))
                active_connections = cursor.fetchone()[0]
                
                return {
                    'database_name': self.config.database_name,
                    'postgresql_version': version,
                    'database_size': db_size,
                    'active_connections': active_connections,
                    'pool_min_connections': self.config.min_connections,
                    'pool_max_connections': self.config.max_connections
                }
        except Exception as e:
            logger.error("database_info_error", error=str(e))
            return {}
    
    def close_all_connections(self):
        """Close all connections in the pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("database_connection_pool_closed")

# Global database manager instance
db_manager = None

def get_database_manager() -> DatabaseConnectionManager:
    """Get or create the global database manager"""
    global db_manager
    if db_manager is None:
        db_manager = DatabaseConnectionManager()
    return db_manager

def initialize_database(config: Optional[DatabaseConfig] = None):
    """Initialize the database connection"""
    global db_manager
    db_manager = DatabaseConnectionManager(config)
    return db_manager

# Convenience functions
def get_connection():
    """Get a database connection"""
    return get_database_manager().get_connection()

def get_cursor(commit=True):
    """Get a database cursor"""
    return get_database_manager().get_cursor(commit)

def test_connection() -> bool:
    """Test database connectivity"""
    return get_database_manager().test_connection()