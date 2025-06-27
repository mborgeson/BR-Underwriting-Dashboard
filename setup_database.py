#!/usr/bin/env python3
"""
Database Setup Script for B&R Capital Dashboard

This script initializes the PostgreSQL database for the comprehensive
underwriting model data system.

Usage:
    python setup_database.py [--reset] [--test] [--load-sample-data]

Options:
    --reset: Drop and recreate the entire database schema
    --test: Run database connectivity and integrity tests
    --load-sample-data: Load sample data from extraction results
    --validate: Validate database structure and integrity
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.database.connection import DatabaseConfig, initialize_database, get_database_manager
from src.database.migrations import MigrationManager, setup_database, validate_database
from src.database.data_loader import DataLoader
from src.database.schema import SchemaManager
import structlog

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger().bind(component="DatabaseSetup")

def print_banner():
    """Print setup banner"""
    print("=" * 70)
    print("🏢 B&R CAPITAL DASHBOARD - DATABASE SETUP")
    print("=" * 70)
    print("PostgreSQL Database: comprehensive_underwriting_model_data")
    print("Phase 3: Database Implementation")
    print("=" * 70)

def check_prerequisites():
    """Check if PostgreSQL is available and configured"""
    logger.info("checking_prerequisites")
    
    # Check for required environment variables
    required_env_vars = {
        'DB_HOST': os.getenv('DB_HOST', 'localhost'),
        'DB_PORT': os.getenv('DB_PORT', '5432'),
        'DB_USERNAME': os.getenv('DB_USERNAME', 'postgres'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD', '')
    }
    
    print("\n📋 Database Configuration:")
    for var, value in required_env_vars.items():
        display_value = "***" if "PASSWORD" in var and value else value
        print(f"  • {var}: {display_value}")
    
    # Test PostgreSQL connectivity
    print("\n🔌 Testing PostgreSQL connectivity...")
    try:
        config = DatabaseConfig()
        # Test with postgres database first
        test_config = DatabaseConfig()
        test_config.database_name = "postgres"
        
        initialize_database(test_config)
        db_manager = get_database_manager()
        
        if db_manager.test_connection():
            print("  ✅ PostgreSQL server connection successful")
            
            # Get database info
            db_info = db_manager.get_database_info()
            print(f"  • PostgreSQL Version: {db_info.get('postgresql_version', 'Unknown')}")
            print(f"  • Active Connections: {db_info.get('active_connections', 0)}")
            
            return True
        else:
            print("  ❌ PostgreSQL server connection failed")
            return False
            
    except Exception as e:
        print(f"  ❌ PostgreSQL connection error: {e}")
        print("\n💡 Troubleshooting:")
        print("  1. Ensure PostgreSQL is installed and running")
        print("  2. Check connection parameters in environment variables")
        print("  3. Verify user permissions")
        return False

def setup_new_database():
    """Set up a new database from scratch"""
    logger.info("setting_up_new_database")
    
    print("\n🚀 Setting up new database...")
    
    try:
        config = DatabaseConfig()
        migration_manager = MigrationManager(config)
        
        if migration_manager.initialize_database():
            print("  ✅ Database initialization successful")
            
            # Get schema info
            schema_manager = SchemaManager()
            schema_info = schema_manager.get_schema_info()
            
            print(f"\n📊 Database Schema Created:")
            print(f"  • Schema Version: {schema_info['schema_version']}")
            print(f"  • Tables: {len(schema_info['tables'])}")
            print(f"  • Views: {len(schema_info['views'])}")
            print(f"  • Indexes: {len(schema_info['indexes'])}")
            
            print("\n📋 Tables Created:")
            for table in schema_info['tables']:
                print(f"    - {table['name']} ({table['size']})")
                
            return True
        else:
            print("  ❌ Database initialization failed")
            return False
            
    except Exception as e:
        logger.error("database_setup_failed", error=str(e))
        print(f"  ❌ Setup error: {e}")
        return False

def test_database():
    """Run comprehensive database tests"""
    logger.info("testing_database")
    
    print("\n🧪 Running database tests...")
    
    try:
        # Test connection
        config = DatabaseConfig()
        initialize_database(config)
        db_manager = get_database_manager()
        
        if not db_manager.test_connection():
            print("  ❌ Database connection test failed")
            return False
        
        print("  ✅ Database connection test passed")
        
        # Validate database integrity
        validation_results = validate_database(config)
        
        print("\n📋 Database Validation Results:")
        for check, result in validation_results.items():
            if check == 'overall_valid':
                continue
            status = "✅" if result else "❌"
            print(f"  {status} {check.replace('_', ' ').title()}")
        
        overall_status = "✅" if validation_results.get('overall_valid', False) else "❌"
        print(f"\n{overall_status} Overall Database Validation: {'PASSED' if validation_results.get('overall_valid', False) else 'FAILED'}")
        
        return validation_results.get('overall_valid', False)
        
    except Exception as e:
        logger.error("database_test_failed", error=str(e))
        print(f"  ❌ Test error: {e}")
        return False

def load_sample_data():
    """Load sample data from extraction results"""
    logger.info("loading_sample_data")
    
    print("\n📦 Loading sample data...")
    
    try:
        # Look for sample extraction data
        sample_files = [
            "data/extractions/test_extraction_Emparrado.json",
            "data/batch_extractions/batch_results_*.json"
        ]
        
        data_loader = DataLoader()
        loaded_count = 0
        
        # Load individual extraction file
        emparrado_file = Path("data/extractions/test_extraction_Emparrado.json")
        if emparrado_file.exists():
            print(f"  📄 Loading: {emparrado_file}")
            
            import json
            with open(emparrado_file, 'r') as f:
                extraction_data = json.load(f)
            
            # Create mock metadata
            metadata = {
                'total_fields': 1140,
                'successful': 1140,
                'duration_seconds': 22.5,
                'errors': [],
                'warnings': []
            }
            
            extraction_id = data_loader.load_extraction_data(
                extraction_data, 
                "active_uw_review", 
                metadata
            )
            
            print(f"    ✅ Loaded extraction: {extraction_id}")
            loaded_count += 1
        
        # Load batch results if available
        batch_dir = Path("data/batch_extractions")
        if batch_dir.exists():
            batch_files = list(batch_dir.glob("batch_results_*.json"))
            for batch_file in batch_files[:1]:  # Load only first batch file
                print(f"  📄 Loading batch: {batch_file}")
                
                extraction_ids = data_loader.load_batch_extraction_results(str(batch_file))
                print(f"    ✅ Loaded {len(extraction_ids)} extractions from batch")
                loaded_count += len(extraction_ids)
        
        if loaded_count > 0:
            # Get summary
            summary = data_loader.get_latest_data_summary()
            print(f"\n📊 Data Loading Summary:")
            print(f"  • Total Properties: {summary.get('total_properties', 0)}")
            print(f"  • Total Units: {summary.get('total_units', 0)}")
            print(f"  • Avg Purchase Price: ${summary.get('avg_purchase_price', 0):,.0f}")
            print(f"  • Latest Extraction: {summary.get('latest_extraction', 'N/A')}")
            
            return True
        else:
            print("  ⚠️  No sample data files found")
            print("     Run extraction tests first to generate sample data")
            return False
            
    except Exception as e:
        logger.error("sample_data_loading_failed", error=str(e))
        print(f"  ❌ Sample data loading error: {e}")
        return False

def reset_database():
    """Reset the database (drop and recreate)"""
    logger.warning("resetting_database")
    
    print("\n⚠️  RESETTING DATABASE...")
    response = input("This will delete all data. Are you sure? (type 'yes' to confirm): ")
    
    if response.lower() != 'yes':
        print("  Database reset cancelled")
        return False
    
    try:
        config = DatabaseConfig()
        migration_manager = MigrationManager(config)
        
        if migration_manager.reset_database():
            print("  ✅ Database reset successful")
            return True
        else:
            print("  ❌ Database reset failed")
            return False
            
    except Exception as e:
        logger.error("database_reset_failed", error=str(e))
        print(f"  ❌ Reset error: {e}")
        return False

def main():
    """Main setup function"""
    parser = argparse.ArgumentParser(description="B&R Capital Dashboard Database Setup")
    parser.add_argument("--reset", action="store_true", help="Reset database")
    parser.add_argument("--test", action="store_true", help="Test database")
    parser.add_argument("--load-sample-data", action="store_true", help="Load sample data")
    parser.add_argument("--validate", action="store_true", help="Validate database")
    
    args = parser.parse_args()
    
    print_banner()
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Prerequisites check failed. Please fix issues and try again.")
        sys.exit(1)
    
    success = True
    
    # Reset database if requested
    if args.reset:
        success = success and reset_database()
    
    # Set up database (if not resetting, this will be skipped if DB exists)
    if not args.reset or success:
        success = success and setup_new_database()
    
    # Test database
    if args.test or args.validate:
        success = success and test_database()
    
    # Load sample data
    if args.load_sample_data and success:
        success = success and load_sample_data()
    
    # Final status
    print("\n" + "=" * 70)
    if success:
        print("🎉 DATABASE SETUP COMPLETED SUCCESSFULLY!")
        print("\n✅ Next Steps:")
        print("  1. Database is ready for Phase 3 testing")
        print("  2. Run extraction workflows to populate data")
        print("  3. Proceed to Phase 4: Monitoring System")
    else:
        print("❌ DATABASE SETUP FAILED!")
        print("\n💡 Troubleshooting:")
        print("  1. Check PostgreSQL installation and configuration")
        print("  2. Verify environment variables")
        print("  3. Check database permissions")
    print("=" * 70)

if __name__ == "__main__":
    main()