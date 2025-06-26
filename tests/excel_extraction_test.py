"""
Test script for B&R Capital Excel Data Extraction
Demonstrates integration with SharePoint and extraction pipeline
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Add project root to path (same pattern as your run_discovery.py)
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.insert(0, str(project_root))

# Import required libraries
import numpy as np
import pandas as pd

# Now import from src
from src.data_extraction.excel_extraction_system import (
    CellMappingParser, 
    ExcelDataExtractor, 
    BatchFileProcessor,
    export_to_csv
)

# Configuration
REFERENCE_FILE = r"/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx"
OUTPUT_DIR = r"/home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard/data/extractions"

# Critical metrics to validate (from your documentation)
CRITICAL_METRICS = [
    "PROPERTY_NAME",
    "PROPERTY_CITY", 
    "SUBMARKET_CLUSTER",
    "PROPERTY_LATITUDE",
    "PROPERTY_LONGITUDE",
    "YEAR_BUILT",
    "UNITS",
    "AVG_SQUARE_FEET",
    "EMPIRICAL_RENT",
    "RENT_PSF",
    "NET_RENTAL_INCOME",
    "EFFECTIVE_GROSS_INCOME",
    "TOTAL_OPERATING_EXPENSES",
    "NET_OPERATING_INCOME",
    "PURCHASE_PRICE",
    "TOTAL_HARD_COSTS",
    "TOTAL_ACQUISITION_BUDGET",
    "EQUITY_LP_CAPITAL",
    "LOAN_TO_VALUE",
    "T12_RETURN_ON_PP",
    "T12_RETURN_ON_COST",
    "BASIS_UNIT_AT_CLOSE",
    "BASIS_UNIT_AT_EXIT",
    "LEVERED_RETURNS_IRR",
    "LEVERED_RETURNS_MOIC",
    "EXIT_PERIOD_MONTHS",
    "EXIT_CAP_RATE"
]


def test_single_file_extraction():
    """Test extraction from a single file"""
    print("\n=== Testing Single File Extraction ===")
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Step 1: Load mappings
    print("Loading cell mappings...")
    parser = CellMappingParser(REFERENCE_FILE)
    mappings = parser.load_mappings()
    print(f"Loaded {len(mappings)} cell mappings")
    
    # Display mapping categories
    categories = {}
    for mapping in mappings.values():
        if mapping.category not in categories:
            categories[mapping.category] = 0
        categories[mapping.category] += 1
    
    print("\nMapping Categories:")
    for category, count in sorted(categories.items()):
        print(f"  - {category}: {count} fields")
    
    # Step 2: Create extractor
    print("\nCreating extractor...")
    extractor = ExcelDataExtractor(mappings)
    
    # Step 3: Test with sample file (corrected path)
    test_file = {
        'file_path': r'/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Emparrado UW Model vCurrent.xlsb',
        'deal_name': 'Emparrado',
        'deal_stage': '1) Initial UW and Review',
        'modified_date': datetime.now().isoformat()
    }
    
    print(f"\nExtracting from: {test_file['file_path']}")
    
    # Validate file exists
    if not os.path.exists(test_file['file_path']):
        print(f"ERROR: Test file does not exist: {test_file['file_path']}")
        print("Please ensure the file is in the correct location.")
        return None
    
    try:
        # Extract data
        extracted_data = extractor.extract_from_file(test_file['file_path'])
        
        # Add metadata
        extracted_data.update({
            '_deal_name': test_file['deal_name'],
            '_deal_stage': test_file['deal_stage'],
            '_file_modified_date': test_file['modified_date']
        })
        
        # Display results
        metadata = extracted_data.get('_extraction_metadata', {})
        print(f"\nExtraction Results:")
        print(f"  - Total fields: {metadata.get('total_fields', 0)}")
        print(f"  - Successful: {metadata.get('successful', 0)}")
        print(f"  - Failed: {metadata.get('failed', 0)}")
        print(f"  - Duration: {metadata.get('duration_seconds', 0):.2f} seconds")
        
        # Check critical metrics
        print("\nCritical Metrics Check:")
        missing_critical = []
        for metric in CRITICAL_METRICS:
            if metric in extracted_data:
                value = extracted_data[metric]
                if pd.notna(value):
                    print(f"  ✓ {metric}: {value}")
                else:
                    print(f"  ✗ {metric}: NaN")
                    missing_critical.append(metric)
            else:
                print(f"  ✗ {metric}: Not found in mappings")
                missing_critical.append(metric)
        
        if missing_critical:
            print(f"\nWarning: {len(missing_critical)} critical metrics missing or NaN")
        
        # Save single file results
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_file = os.path.join(OUTPUT_DIR, f"test_extraction_{test_file['deal_name']}.json")
        with open(output_file, 'w') as f:
            # Convert numpy types for JSON serialization
            clean_data = {}
            for k, v in extracted_data.items():
                if isinstance(v, (np.integer, np.floating)):
                    clean_data[k] = float(v)
                elif isinstance(v, np.ndarray):
                    # Handle numpy arrays
                    if v.size == 0:
                        clean_data[k] = None
                    elif v.size == 1:
                        clean_data[k] = float(v.item()) if np.isrealobj(v) else str(v.item())
                    else:
                        clean_data[k] = v.tolist()
                elif pd.isna(v) if not isinstance(v, (list, dict, np.ndarray)) else False:
                    clean_data[k] = None
                else:
                    clean_data[k] = v
            json.dump(clean_data, f, indent=2, default=str)
        
        print(f"\nResults saved to: {output_file}")
        
        return extracted_data
        
    except Exception as e:
        print(f"\nError during extraction: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def test_batch_processing():
    """Test batch processing of multiple files"""
    print("\n=== Testing Batch Processing ===")
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Create test file list (you'll need to update with actual files)
    test_files = [
        {
            'file_path': r'/Real Estate/Deals/1) Initial UW and Review/Cimarron (Mesa, AZ)/UW Model/Cimarron UW Model vCurrent.xlsb',
            'deal_name': 'Cimarron',
            'deal_stage': '1) Initial UW and Review',
            'modified_date': '2025-02-01'
        },
        {
            'file_path': r'/Real Estate/Deals/1) Initial UW and Review/Emparrado (Mesa, AZ)/UW Model/Emparrado UW Model vCurrent.xlsb',
            'deal_name': 'Emparrado',
            'deal_stage': '1) Initial UW and Review',
            'modified_date': '2025-02-05'
        },
        {
            'file_path': r'/Real Estate/Deals/0) Dead Deals/Tamarak (Tempe, AZ)/UW Model/Tamarak UW Model vCurrent.xlsb',
            'deal_name': 'Tamarak',
            'deal_stage': '0) Dead Deals',
            'modified_date': '2024-12-15'
        }
    ]
    
    print(f"Processing {len(test_files)} files...")
    
    # Load mappings and create processor
    parser = CellMappingParser(REFERENCE_FILE)
    mappings = parser.load_mappings()
    extractor = ExcelDataExtractor(mappings)
    processor = BatchFileProcessor(extractor, batch_size=2)
    
    # Process files
    results = processor.process_files(test_files, max_workers=2)
    
    print(f"\nBatch Processing Complete:")
    print(f"  - Files processed: {len(results)}")
    
    # Export to CSV
    if results:
        output_csv = os.path.join(OUTPUT_DIR, f"batch_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        export_to_csv(results, output_csv)
        print(f"  - Results exported to: {output_csv}")
    
    return results


def validate_mapping_coverage():
    """Validate that all expected fields are in the mappings"""
    print("\n=== Validating Mapping Coverage ===")
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    parser = CellMappingParser(REFERENCE_FILE)
    mappings = parser.load_mappings()
    
    # Export mapping summary
    summary_path = os.path.join(OUTPUT_DIR, "mapping_summary.csv")
    parser.export_mapping_summary(summary_path)
    print(f"Mapping summary exported to: {summary_path}")
    
    # Check for critical metrics
    print("\nChecking critical metrics coverage:")
    covered = []
    missing = []
    
    for metric in CRITICAL_METRICS:
        if metric in mappings:
            covered.append(metric)
        else:
            missing.append(metric)
    
    print(f"  - Covered: {len(covered)}/{len(CRITICAL_METRICS)}")
    if missing:
        print(f"  - Missing from mappings:")
        for m in missing:
            print(f"    • {m}")
    
    return mappings


def main():
    """Run all tests"""
    print("B&R Capital Excel Extraction Test Suite")
    print("=" * 50)
    
    # Test 1: Validate mappings
    mappings = validate_mapping_coverage()
    
    # Test 2: Single file extraction
    single_result = test_single_file_extraction()
    
    # Test 3: Batch processing (comment out if you don't have multiple test files)
    # batch_results = test_batch_processing()
    
    print("\n" + "=" * 50)
    print("Testing complete!")
    print(f"Check output directory for results: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()