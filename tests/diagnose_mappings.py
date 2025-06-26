"""
Diagnostics script to troubleshoot cell mapping issues
Helps identify problems with the Excel reference file
"""

import os
import sys
import pandas as pd
from pathlib import Path

# Add project root to path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.insert(0, str(project_root))

from src.data_extraction.excel_extraction_system import CellMappingParser

def main():
    print("B&R Capital - Cell Mapping Diagnostics")
    print("=" * 50)
    
    # Reference file path
    ref_file = r"/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx"
    
    print(f"Reference file: {ref_file}")
    print(f"File exists: {os.path.exists(ref_file)}")
    
    if not os.path.exists(ref_file):
        print("ERROR: Reference file not found!")
        return
    
    # Step 1: Try to read Excel file directly
    print("\n=== Step 1: Reading Excel File Directly ===")
    try:
        # Try to read all sheets
        xl_file = pd.ExcelFile(ref_file)
        print(f"Available sheets: {xl_file.sheet_names}")
        
        # Try the expected sheet name
        expected_sheet = "UW Model - Cell Reference Table"
        if expected_sheet in xl_file.sheet_names:
            print(f"✓ Found expected sheet: {expected_sheet}")
            
            # Read the sheet
            df = pd.read_excel(ref_file, sheet_name=expected_sheet)
            print(f"Sheet shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print("\nFirst 5 rows:")
            print(df.head())
            
            # Check if we have the expected columns
            expected_cols = ['B', 'C', 'D', 'G']
            if all(col in df.columns for col in expected_cols):
                print("✓ All expected columns found")
            else:
                print("⚠ Expected columns not found, let's check actual data...")
                
                # Show actual column names and try to identify the right ones
                print(f"\nActual columns: {list(df.columns)}")
                print("\nSample data from each column:")
                for i, col in enumerate(df.columns[:8]):  # Show first 8 columns
                    sample_values = df[col].dropna().head(3).tolist()
                    print(f"  Column {i} ({col}): {sample_values}")
            
        else:
            print(f"⚠ Expected sheet '{expected_sheet}' not found")
            print("Available sheets:")
            for sheet in xl_file.sheet_names:
                print(f"  - {sheet}")
                
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 2: Test the mapping parser
    print(f"\n=== Step 2: Testing CellMappingParser ===")
    try:
        parser = CellMappingParser(ref_file)
        mappings = parser.load_mappings()
        
        print(f"Mappings loaded: {len(mappings)}")
        
        if mappings:
            print("✓ Mappings loaded successfully!")
            
            # Show some sample mappings
            print("\nSample mappings:")
            for i, (field_name, mapping) in enumerate(list(mappings.items())[:5]):
                print(f"  {i+1}. {field_name}")
                print(f"     Category: {mapping.category}")
                print(f"     Description: {mapping.description}")
                print(f"     Sheet: {mapping.sheet_name}")
                print(f"     Cell: {mapping.cell_address}")
                print()
            
            # Check for critical metrics
            critical_metrics = [
                "PROPERTY_NAME", "PROPERTY_CITY", "PURCHASE_PRICE", 
                "UNITS", "EMPIRICAL_RENT", "NET_OPERATING_INCOME"
            ]
            
            print("Critical metrics check:")
            found_metrics = []
            for metric in critical_metrics:
                if metric in mappings:
                    found_metrics.append(metric)
                    print(f"  ✓ {metric}")
                else:
                    print(f"  ✗ {metric}")
            
            print(f"\nFound {len(found_metrics)}/{len(critical_metrics)} critical metrics")
            
        else:
            print("⚠ No mappings loaded - there's an issue with the parsing logic")
            
    except Exception as e:
        print(f"Error in mapping parser: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
