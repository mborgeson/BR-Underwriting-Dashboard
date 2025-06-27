#!/usr/bin/env python3
"""
Load All 41 Properties from Discovery Data

Since we can't download the actual Excel files due to SharePoint authentication,
this script creates property records based on the discovery metadata.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
import re

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Set environment variable
os.environ['DB_PASSWORD'] = 'dashboard123'

from src.database.connection import get_cursor, initialize_database, DatabaseConfig

def extract_location_from_deal_name(deal_name):
    """Extract city and state from deal name like 'Property Name (City, ST)'"""
    match = re.search(r'\(([^,]+),\s*([A-Z]{2})\)', deal_name)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return None, None

def convert_deal_stage(stage_text):
    """Convert deal stage text to database enum"""
    stage_mapping = {
        '0) Dead Deals': 'dead_deals',
        '1) Initial UW and Review': 'initial_uw_review',
        '2) Active UW and Review': 'active_uw_review', 
        '3) Under Contract': 'under_contract',
        '4) Closed Deals': 'closed_deals',
        '5) Realized Deals': 'realized_deals'
    }
    return stage_mapping.get(stage_text, 'initial_uw_review')

def extract_property_name(deal_name):
    """Extract property name from deal name"""
    # Remove location info in parentheses
    name = re.sub(r'\s*\([^)]+\)\s*$', '', deal_name)
    return name.strip()

def load_all_discovered_properties():
    """Load all 41 properties from discovery data"""
    print("🏢 Loading All 41 Properties from Discovery Data")
    print("=" * 60)
    
    # Initialize database
    config = DatabaseConfig()
    initialize_database(config)
    
    # Load discovery data
    discovery_file = Path("output/discovered_files_20250625_171007.json")
    if not discovery_file.exists():
        print(f"❌ Discovery file not found: {discovery_file}")
        return 0
    
    with open(discovery_file, 'r') as f:
        discovered_files = json.load(f)
    
    print(f"📄 Found {len(discovered_files)} discovered files")
    
    # Track unique properties
    unique_properties = {}
    
    # Process each file
    for file_data in discovered_files:
        deal_name = file_data.get('deal_name', '')
        property_name = extract_property_name(deal_name)
        
        # Skip if we've already processed this property
        if property_name in unique_properties:
            continue
            
        unique_properties[property_name] = file_data
    
    print(f"📊 Found {len(unique_properties)} unique properties")
    
    # Load properties into database
    properties_loaded = 0
    
    with get_cursor() as cursor:
        for property_name, file_data in unique_properties.items():
            try:
                deal_name = file_data.get('deal_name', '')
                city, state = extract_location_from_deal_name(deal_name)
                deal_stage = convert_deal_stage(file_data.get('deal_stage', ''))
                
                # Skip properties we already have actual data for
                cursor.execute("""
                    SELECT COUNT(*) FROM properties WHERE property_name = %s
                """, (property_name,))
                
                if cursor.fetchone()[0] > 0:
                    print(f"  ⚪ {property_name} - already has extraction data")
                    continue
                
                print(f"  🏗️  Loading: {property_name}")
                
                # 1. Register property
                cursor.execute("""
                    INSERT INTO properties (property_name, property_city, property_state)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (property_name) DO UPDATE SET
                        property_city = COALESCE(properties.property_city, EXCLUDED.property_city),
                        property_state = COALESCE(properties.property_state, EXCLUDED.property_state),
                        updated_at = NOW()
                    RETURNING property_id;
                """, (property_name, city, state))
                
                property_id = cursor.fetchone()[0]
                
                # 2. Create placeholder underwriting record
                cursor.execute("""
                    INSERT INTO underwriting_data (
                        property_id, property_name, deal_stage,
                        file_path, extraction_timestamp,
                        file_size_mb, file_modified_date
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING extraction_id;
                """, (
                    property_id,
                    property_name,
                    deal_stage,
                    file_data.get('file_path', ''),
                    datetime.now(),
                    file_data.get('size_mb', 0),
                    datetime.fromisoformat(file_data.get('last_modified', '').replace('Z', '+00:00')) if file_data.get('last_modified') else None
                ))
                
                extraction_id = cursor.fetchone()[0]
                
                # 3. Add metadata
                cursor.execute("""
                    INSERT INTO extraction_metadata (
                        extraction_id,
                        total_fields_attempted,
                        successful_extractions,
                        failed_extractions,
                        error_count,
                        warnings_count
                    ) VALUES (%s, %s, %s, %s, %s, %s);
                """, (
                    extraction_id,
                    0,  # No fields extracted yet
                    0,
                    0,
                    1,  # Mark as needing extraction
                    0
                ))
                
                properties_loaded += 1
                print(f"    ✅ {property_name} ({city}, {state}) - {deal_stage}")
                
            except Exception as e:
                print(f"    ❌ Error loading {property_name}: {e}")
                continue
    
    return properties_loaded

def show_final_summary():
    """Show summary of all properties in database"""
    with get_cursor() as cursor:
        # Total counts
        cursor.execute("SELECT COUNT(DISTINCT property_name) FROM properties;")
        total_properties = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM underwriting_data;")
        total_records = cursor.fetchone()[0]
        
        print(f"\n📊 DATABASE SUMMARY")
        print("=" * 60)
        print(f"Total Properties: {total_properties}")
        print(f"Total Underwriting Records: {total_records}")
        
        # Properties by stage
        print(f"\n📋 PROPERTIES BY STAGE:")
        cursor.execute("""
            SELECT deal_stage, COUNT(DISTINCT property_name) as count
            FROM underwriting_data
            GROUP BY deal_stage
            ORDER BY count DESC;
        """)
        
        for stage, count in cursor.fetchall():
            print(f"  • {stage}: {count} properties")
        
        # Properties with actual data vs placeholders
        print(f"\n📈 DATA STATUS:")
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN units IS NOT NULL OR purchase_price IS NOT NULL THEN 'With Extraction Data'
                    ELSE 'Placeholder (Needs Extraction)'
                END as status,
                COUNT(DISTINCT property_name) as count
            FROM underwriting_data
            GROUP BY status;
        """)
        
        for status, count in cursor.fetchall():
            print(f"  • {status}: {count} properties")
        
        # List all properties
        print(f"\n📋 ALL PROPERTIES IN DATABASE:")
        cursor.execute("""
            SELECT DISTINCT
                p.property_name,
                p.property_city,
                p.property_state,
                ud.deal_stage,
                CASE 
                    WHEN ud.units IS NOT NULL THEN 'Has Data'
                    ELSE 'Needs Extraction'
                END as data_status
            FROM properties p
            LEFT JOIN underwriting_data ud ON p.property_id = ud.property_id
            ORDER BY p.property_name;
        """)
        
        for i, (name, city, state, stage, status) in enumerate(cursor.fetchall(), 1):
            location = f"{city}, {state}" if city and state else "Location Unknown"
            print(f"  {i:2d}. {name} ({location}) - {stage} - {status}")

def main():
    """Main function"""
    print("🏢 B&R CAPITAL DASHBOARD - LOAD ALL 41 PROPERTIES")
    print("=" * 70)
    print("Loading property records for all discovered SharePoint files")
    print("=" * 70)
    
    loaded = load_all_discovered_properties()
    
    print(f"\n✅ Loaded {loaded} new property records")
    
    show_final_summary()
    
    print(f"\n💡 NEXT STEPS:")
    print("1. Refresh SharePoint authentication")
    print("2. Run: python test_batch_extraction.py")
    print("3. Select option 2 for 'Full mode (process all 41 files)'")
    print("4. This will download and extract actual data for all properties")

if __name__ == "__main__":
    main()