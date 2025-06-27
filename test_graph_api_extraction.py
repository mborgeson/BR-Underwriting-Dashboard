#!/usr/bin/env python3
"""
Test Graph API Extraction

Test the new Graph API streaming extraction without full workflow.
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.extraction.graph_api_extractor import GraphAPIFileExtractor
from src.data_extraction.excel_extraction_system import CellMappingParser, ExcelDataExtractor

def test_graph_api_connection(client_id: str, client_secret: str):
    """Test basic Graph API connection"""
    print("🔌 Testing Graph API Connection...")
    
    extractor = GraphAPIFileExtractor(client_id, client_secret)
    
    # Test authentication
    if extractor.authenticate():
        print("  ✅ Authentication successful")
        
        # Test site access
        try:
            site_info = extractor.get_site_info()
            print(f"  ✅ Site access successful: {site_info['site_name']}")
            
            # Test drive access
            drive_info = extractor.get_drive_info(site_info['site_id'])
            print(f"  ✅ Drive access successful: {drive_info['drive_name']}")
            
            return extractor, site_info, drive_info
            
        except Exception as e:
            print(f"  ❌ Site/Drive access failed: {e}")
            return None, None, None
    else:
        print("  ❌ Authentication failed")
        return None, None, None

def test_file_discovery(extractor: GraphAPIFileExtractor, site_id: str, drive_id: str):
    """Test UW file discovery"""
    print("\n🔍 Testing File Discovery...")
    
    try:
        files = extractor.discover_uw_files(site_id, drive_id)
        print(f"  ✅ Discovered {len(files)} UW model files")
        
        if files:
            print("  📋 Files found:")
            for file_info in files[:5]:  # Show first 5
                print(f"    • {file_info['deal_name']} ({file_info['deal_stage']})")
            
            if len(files) > 5:
                print(f"    ... and {len(files) - 5} more files")
        
        return files
        
    except Exception as e:
        print(f"  ❌ File discovery failed: {e}")
        return []

def test_single_extraction(extractor: GraphAPIFileExtractor, file_info: dict, reference_file: str):
    """Test extraction from a single file"""
    print(f"\n📄 Testing Extraction: {file_info['deal_name']}")
    
    try:
        # Load cell mappings
        parser = CellMappingParser(reference_file)
        mappings = parser.load_mappings()
        excel_extractor = ExcelDataExtractor(mappings)
        
        print(f"  📊 Loaded {len(mappings)} cell mappings")
        
        # Extract data
        extracted_data = extractor.extract_from_file_info(file_info, excel_extractor)
        
        if extracted_data:
            # Count successful extractions
            non_null_values = len([v for v in extracted_data.values() if v is not None])
            success_rate = (non_null_values / len(mappings)) * 100
            
            print(f"  ✅ Extraction successful")
            print(f"  📊 Extracted {non_null_values}/{len(mappings)} fields ({success_rate:.1f}%)")
            
            # Show sample data
            print("  🔍 Sample extracted data:")
            sample_fields = ['PROPERTY_NAME', 'UNITS', 'PURCHASE_PRICE', 'YEAR_BUILT', 'LEVERED_RETURNS_IRR']
            for field in sample_fields:
                value = extracted_data.get(field, 'Not found')
                if value is not None:
                    print(f"    • {field}: {value}")
            
            return extracted_data
        else:
            print("  ❌ Extraction failed")
            return None
            
    except Exception as e:
        print(f"  ❌ Extraction error: {e}")
        return None

def main():
    """Main test function"""
    print("🏢 B&R CAPITAL DASHBOARD - GRAPH API EXTRACTION TEST")
    print("=" * 70)
    
    # Get credentials
    client_id = os.getenv("AZURE_CLIENT_ID", "5a620cea-31fe-40f6-8b48-d55bc5465dc9")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    
    if not client_secret:
        print("❌ AZURE_CLIENT_SECRET environment variable not set")
        print("   Set it with: export AZURE_CLIENT_SECRET='your-secret-here'")
        return 1
    
    # Reference file
    reference_file = "/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx"
    
    if not Path(reference_file).exists():
        print(f"❌ Reference file not found: {reference_file}")
        return 1
    
    print(f"📋 Using reference file: {Path(reference_file).name}")
    print(f"🔑 Client ID: {client_id}")
    print()
    
    # Test connection
    extractor, site_info, drive_info = test_graph_api_connection(client_id, client_secret)
    
    if not extractor:
        print("\n❌ Graph API connection failed. Cannot proceed with tests.")
        return 1
    
    # Test file discovery
    files = test_file_discovery(extractor, site_info['site_id'], drive_info['drive_id'])
    
    if not files:
        print("\n❌ No files discovered. Cannot test extraction.")
        return 1
    
    # Test extraction on first file
    test_file = files[0]
    extracted_data = test_single_extraction(extractor, test_file, reference_file)
    
    if extracted_data:
        print("\n🎉 ALL TESTS PASSED!")
        print("\nNext steps:")
        print("1. Run complete workflow for all files:")
        print(f"   python complete_realtime_workflow.py \\")
        print(f"     --client-id \"{client_id}\" \\")
        print(f"     --client-secret \"$AZURE_CLIENT_SECRET\" \\")
        print(f"     --reference-file \"{reference_file}\"")
        return 0
    else:
        print("\n❌ Extraction test failed.")
        return 1

if __name__ == "__main__":
    sys.exit(main())