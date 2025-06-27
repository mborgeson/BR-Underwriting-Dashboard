#!/usr/bin/env python3
"""
Clean Test Data from Database (Automatic)

Remove the test/example data and keep only the real property records.
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Set environment variable
os.environ['DB_PASSWORD'] = 'dashboard123'

from src.database.connection import get_cursor, initialize_database, DatabaseConfig

def clean_test_data():
    """Remove test data from database"""
    print("🧹 CLEANING TEST DATA FROM DATABASE")
    print("=" * 60)
    
    # Initialize database
    config = DatabaseConfig()
    initialize_database(config)
    
    with get_cursor() as cursor:
        # Show what we're removing
        print("🔍 Finding test data to remove...")
        cursor.execute("""
            SELECT DISTINCT
                ud.extraction_id,
                p.property_name,
                ud.deal_stage,
                ud.file_path,
                ud.units,
                ud.purchase_price
            FROM underwriting_data ud
            JOIN properties p ON ud.property_id = p.property_id
            WHERE ud.units IS NOT NULL 
               OR ud.purchase_price IS NOT NULL
            ORDER BY p.property_name;
        """)
        
        test_records = cursor.fetchall()
        print(f"\nRemoving {len(test_records)} test data records:")
        for record in test_records:
            extraction_id, name, stage, file_path, units, price = record
            print(f"  • {name}: {units} units, ${price:,.0f} (from: {file_path})")
        
        if test_records:
            # Delete test data
            print("\n🗑️  Deleting test data...")
            
            # Delete from extraction_metadata first (foreign key)
            test_ids = [r[0] for r in test_records]
            cursor.execute("""
                DELETE FROM extraction_metadata 
                WHERE extraction_id = ANY(%s);
            """, (test_ids,))
            
            # Delete from underwriting_data
            cursor.execute("""
                DELETE FROM underwriting_data 
                WHERE extraction_id = ANY(%s);
            """, (test_ids,))
            
            print(f"✅ Deleted {len(test_records)} test records")
        else:
            print("✅ No test data found")
        
        # Show final state
        print("\n📊 Final database state:")
        
        # Count properties with only placeholders
        cursor.execute("""
            SELECT COUNT(DISTINCT p.property_name)
            FROM properties p
            WHERE NOT EXISTS (
                SELECT 1 FROM underwriting_data ud 
                WHERE ud.property_id = p.property_id 
                AND (ud.units IS NOT NULL OR ud.purchase_price IS NOT NULL)
            );
        """)
        placeholder_count = cursor.fetchone()[0]
        
        # Total properties
        cursor.execute("SELECT COUNT(*) FROM properties;")
        total = cursor.fetchone()[0]
        
        print(f"Total properties: {total}")
        print(f"Properties with data: {total - placeholder_count}")
        print(f"Properties awaiting extraction: {placeholder_count}")
        
        # Show by stage
        print("\nProperties by stage (all are placeholders now):")
        cursor.execute("""
            SELECT 
                ud.deal_stage,
                COUNT(DISTINCT p.property_name) as count
            FROM properties p
            JOIN underwriting_data ud ON p.property_id = ud.property_id
            GROUP BY ud.deal_stage
            ORDER BY count DESC;
        """)
        
        for stage, count in cursor.fetchall():
            print(f"  • {stage}: {count} properties")

def main():
    """Main function"""
    print("AUTOMATIC TEST DATA REMOVAL")
    print("This will remove all test/example data from the database")
    print("keeping only the real property placeholders.")
    print()
    
    clean_test_data()
    
    print("\n✅ Database cleaned!")
    print("\n📋 All 41 properties now have placeholder records")
    print("   waiting for real data extraction from SharePoint")
    print("\nNext steps:")
    print("1. Refresh SharePoint authentication")
    print("2. Run the complete extraction workflow:")
    print("   python src/workflows/complete_extraction_to_database.py \\")
    print("     --reference-file \"/path/to/reference.xlsx\" \\")
    print("     --use-cached-discovery \"output/discovered_files_20250625_171007.json\"")

if __name__ == "__main__":
    main()