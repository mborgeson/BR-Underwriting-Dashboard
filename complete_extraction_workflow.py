"""
Complete workflow showing how to use the extraction system with correct SharePoint paths
This integrates all components using the verified path structure
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add the project root to Python path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Import your modules
from src.data_extraction.excel_extraction_system import (
    CellMappingParser,
    ExcelDataExtractor,
    BatchFileProcessor,
    export_to_csv
)
from src.data_extraction.sharepoint_excel_integration import SharePointExcelExtractor

# Configuration
REFERENCE_FILE = r"/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx"
OUTPUT_DIR = r"/home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard/data/extractions"
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "YOUR_CLIENT_SECRET_HERE")


def run_complete_extraction():
    """Run the complete extraction workflow"""
    
    print("=" * 70)
    print("B&R CAPITAL EXCEL DATA EXTRACTION")
    print("=" * 70)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Reference File: {os.path.basename(REFERENCE_FILE)}")
    print(f"Output Directory: {OUTPUT_DIR}")
    print("=" * 70)
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Step 1: Initialize the integrated extractor
    print("\n[Step 1] Initializing extraction system...")
    try:
        extractor = SharePointExcelExtractor(REFERENCE_FILE, CLIENT_SECRET)
        print("✓ Extraction system initialized")
        print(f"✓ Loaded {len(extractor.mappings)} cell mappings")
    except Exception as e:
        print(f"✗ Failed to initialize: {e}")
        return
    
    # Step 2: Test SharePoint connection
    print("\n[Step 2] Testing SharePoint connection...")
    try:
        extractor.authenticate()
        site_id = extractor.get_site_id()
        real_estate_drive_id = extractor.get_real_estate_drive_id(site_id)
        print("✓ Connected to SharePoint")
        print(f"✓ Site ID: {site_id}")
        print(f"✓ Real Estate Drive ID: {real_estate_drive_id}")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return
    
    # Step 3: Discover files
    print("\n[Step 3] Discovering Excel files...")
    try:
        files = extractor.discover_excel_files()
        print(f"✓ Found {len(files)} files matching criteria")
        
        # Show summary by stage
        df_files = pd.DataFrame(files)
        if not df_files.empty:
            stage_counts = df_files['deal_stage'].value_counts()
            print("\nFiles by stage:")
            for stage, count in stage_counts.items():
                print(f"  - {stage}: {count} files")
    except Exception as e:
        print(f"✗ Discovery failed: {e}")
        return
    
    # Step 4: Select files to process
    print("\n[Step 4] Selecting files to process...")
    
    # Option 1: Process all files
    files_to_process = files
    
    # Option 2: Process only specific stages (uncomment to use)
    # target_stages = ["1) Initial UW and Review", "2) Active UW and Review"]
    # files_to_process = [f for f in files if f['deal_stage'] in target_stages]
    
    # Option 3: Process most recent N files (uncomment to use)
    # files_to_process = sorted(files, key=lambda x: x['modified_date'], reverse=True)[:10]
    
    print(f"✓ Selected {len(files_to_process)} files for processing")
    
    # Step 5: Process files
    print("\n[Step 5] Processing files...")
    print("-" * 70)
    
    results = []
    errors = []
    
    for i, file_info in enumerate(files_to_process, 1):
        print(f"\n[{i}/{len(files_to_process)}] {file_info['deal_name']} - {file_info['file_name']}")
        
        try:
            # Download and extract
            result = extractor.download_and_extract(file_info)
            
            if result:
                # Show extraction summary
                metadata = result.get('_extraction_metadata', {})
                print(f"  ✓ Extracted {metadata.get('successful', 0)}/{metadata.get('total_fields', 0)} fields")
                print(f"  ✓ Time: {metadata.get('duration_seconds', 0):.2f}s")
                
                results.append(result)
            else:
                print("  ✗ Extraction failed")
                errors.append(file_info)
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
            errors.append(file_info)
        
        # Progress indicator
        if i % 5 == 0:
            print(f"\nProgress: {i}/{len(files_to_process)} files completed...")
    
    # Step 6: Save results
    print("\n[Step 6] Saving results...")
    
    if results:
        # Save detailed CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_path = os.path.join(OUTPUT_DIR, f"extraction_results_{timestamp}.csv")
        export_to_csv(results, csv_path)
        print(f"✓ Results saved to: {csv_path}")
        
        # Save summary report
        summary_path = os.path.join(OUTPUT_DIR, f"extraction_summary_{timestamp}.txt")
        create_summary_report(results, errors, summary_path)
        print(f"✓ Summary saved to: {summary_path}")
    
    # Step 7: Final summary
    print("\n" + "=" * 70)
    print("EXTRACTION COMPLETE")
    print("=" * 70)
    print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Files Processed: {len(results)}")
    print(f"Files Failed: {len(errors)}")
    print(f"Success Rate: {len(results)/len(files_to_process)*100:.1f}%")
    
    if errors:
        print("\nFailed files:")
        for err in errors[:5]:  # Show first 5
            print(f"  - {err['deal_name']} ({err['file_name']})")
        if len(errors) > 5:
            print(f"  ... and {len(errors) - 5} more")


def create_summary_report(results, errors, output_path):
    """Create a detailed summary report"""
    with open(output_path, 'w') as f:
        f.write("B&R CAPITAL EXTRACTION SUMMARY REPORT\n")
        f.write("=" * 70 + "\n\n")
        
        f.write(f"Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Files Processed: {len(results)}\n")
        f.write(f"Total Files Failed: {len(errors)}\n\n")
        
        # Success summary
        if results:
            f.write("SUCCESSFULLY PROCESSED FILES:\n")
            f.write("-" * 70 + "\n")
            
            for result in results:
                deal_name = result.get('_deal_name', 'Unknown')
                stage = result.get('_deal_stage', 'Unknown')
                metadata = result.get('_extraction_metadata', {})
                
                f.write(f"\n{deal_name} ({stage})\n")
                f.write(f"  - Fields extracted: {metadata.get('successful', 0)}/{metadata.get('total_fields', 0)}\n")
                f.write(f"  - Extraction time: {metadata.get('duration_seconds', 0):.2f}s\n")
                f.write(f"  - File size: {result.get('_file_size_mb', 0):.1f} MB\n")
                
                # Check critical metrics
                critical_metrics = [
                    'PROPERTY_NAME', 'PURCHASE_PRICE', 'LEVERED_RETURNS_IRR',
                    'UNITS', 'EMPIRICAL_RENT', 'EXIT_CAP_RATE'
                ]
                
                missing_critical = []
                for metric in critical_metrics:
                    if metric not in result or pd.isna(result.get(metric)):
                        missing_critical.append(metric)
                
                if missing_critical:
                    f.write(f"  - Missing critical metrics: {', '.join(missing_critical)}\n")
        
        # Error summary
        if errors:
            f.write("\n\nFAILED FILES:\n")
            f.write("-" * 70 + "\n")
            
            for error in errors:
                f.write(f"\n{error['deal_name']} - {error['file_name']}\n")
                f.write(f"  - Stage: {error['deal_stage']}\n")
                f.write(f"  - Modified: {error['modified_date']}\n")
        
        # Statistics
        f.write("\n\nEXTRACTION STATISTICS:\n")
        f.write("-" * 70 + "\n")
        
        if results:
            total_fields_attempted = sum(r['_extraction_metadata']['total_fields'] for r in results)
            total_fields_extracted = sum(r['_extraction_metadata']['successful'] for r in results)
            total_time = sum(r['_extraction_metadata']['duration_seconds'] for r in results)
            
            f.write(f"Total fields attempted: {total_fields_attempted:,}\n")
            f.write(f"Total fields extracted: {total_fields_extracted:,}\n")
            f.write(f"Overall success rate: {total_fields_extracted/total_fields_attempted*100:.1f}%\n")
            f.write(f"Total extraction time: {total_time:.1f}s\n")
            f.write(f"Average time per file: {total_time/len(results):.1f}s\n")


if __name__ == "__main__":
    # Run the complete extraction
    run_complete_extraction()
    
    # Optional: Run specific test scenarios
    # test_single_file()
    # test_specific_stage()
    # test_recent_files_only()