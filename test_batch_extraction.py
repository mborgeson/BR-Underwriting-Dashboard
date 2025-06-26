#!/usr/bin/env python3
"""
Test script for B&R Capital Batch Extraction Processor

This script tests the batch processor with the discovered SharePoint files.
It can run in test mode (limited files) or full mode (all files).
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.workflows.batch_extraction_processor import BatchExtractionProcessor

def main():
    print("B&R Capital Dashboard - Batch Extraction Test")
    print("=" * 50)
    
    # File paths
    discovery_file = project_root / "output" / "discovered_files_20250625_171007.json"
    reference_file = Path("/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx")
    
    # Check if files exist
    if not discovery_file.exists():
        print(f"❌ Discovery file not found: {discovery_file}")
        return
        
    if not reference_file.exists():
        print(f"❌ Reference file not found: {reference_file}")
        return
        
    print(f"✅ Discovery file: {discovery_file}")
    print(f"✅ Reference file: {reference_file}")
    print()
    
    # Ask user for test mode
    print("Select processing mode:")
    print("1. Test mode (process 3 files)")
    print("2. Full mode (process all 41 files)")
    choice = input("Enter choice (1 or 2): ").strip()
    
    max_files = 3 if choice == "1" else None
    mode_name = "TEST" if choice == "1" else "FULL"
    
    print(f"\n🚀 Starting {mode_name} batch processing...")
    print("-" * 40)
    
    try:
        # Initialize processor
        processor = BatchExtractionProcessor(
            reference_file_path=str(reference_file),
            output_dir=str(project_root / "data" / "batch_extractions")
        )
        
        # Process batch
        results = processor.process_batch(
            discovery_file_path=str(discovery_file),
            max_workers=3,
            max_files=max_files
        )
        
        # Print results
        print("\n" + "=" * 50)
        print("BATCH PROCESSING RESULTS")
        print("=" * 50)
        
        stats = results['stats']
        print(f"📊 Total Files Processed: {stats['total_files']}")
        print(f"✅ Successful Extractions: {stats['successful_extractions']}")
        print(f"❌ Failed Extractions: {stats['failed_extractions']}")
        
        if stats['total_files'] > 0:
            success_rate = (stats['successful_extractions'] / stats['total_files']) * 100
            print(f"📈 Success Rate: {success_rate:.1f}%")
            
        print(f"📋 Total Fields Extracted: {stats['total_fields_extracted']:,}")
        
        if stats['successful_extractions'] > 0:
            avg_fields = stats['total_fields_extracted'] / stats['successful_extractions']
            print(f"📊 Average Fields per File: {avg_fields:.1f}")
            
        # Processing time
        if stats['start_time'] and stats['end_time']:
            total_time = (stats['end_time'] - stats['start_time']).total_seconds()
            print(f"⏱️  Total Processing Time: {total_time:.1f} seconds")
            
        # Output files
        print(f"\n📁 Output Files Generated:")
        for output_type, file_path in results['output_files'].items():
            if file_path:
                print(f"  • {output_type}: {Path(file_path).name}")
                
        # Errors (if any)
        if stats['processing_errors']:
            print(f"\n❌ Processing Errors ({len(stats['processing_errors'])}):")
            for error in stats['processing_errors'][:5]:  # Show first 5 errors
                print(f"  • {error['file_name']}: {error['error']}")
            if len(stats['processing_errors']) > 5:
                print(f"  • ... and {len(stats['processing_errors']) - 5} more errors")
                
        print("\n" + "=" * 50)
        print("✅ Batch processing completed successfully!")
        print(f"📂 Check output directory: {processor.output_dir}")
        
    except Exception as e:
        print(f"\n❌ Batch processing failed: {e}")
        import traceback
        print(f"Full error:\n{traceback.format_exc()}")
        return 1
        
    return 0


if __name__ == "__main__":
    exit(main())