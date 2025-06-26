#!/usr/bin/env python3
"""
Test batch processor with local Excel files to demonstrate functionality
"""

import os
import sys
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.workflows.batch_extraction_processor import BatchExtractionProcessor

def create_local_discovery_file():
    """Create a mock discovery file using local Excel files"""
    
    local_files_dir = Path("/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files")
    
    # Mock discovery data for local files
    discovery_data = []
    
    for excel_file in local_files_dir.glob("*.xlsb"):
        # Create mock file metadata
        file_data = {
            "file_id": f"local_{excel_file.name}",
            "file_name": excel_file.name,
            "file_path": str(excel_file),
            "deal_name": excel_file.stem.replace(" UW Model vCurrent", ""),
            "deal_stage": "5) Local Test Files",
            "stage_index": 5,
            "size_mb": excel_file.stat().st_size / (1024 * 1024),
            "last_modified": "2025-06-26T12:00:00.000Z",
            "created_date": "2025-06-26T12:00:00.000Z",
            "web_url": f"file://{excel_file}",
            "download_url": f"file://{excel_file}",  # Use file:// for local access
            "drive_id": "local_drive"
        }
        discovery_data.append(file_data)
    
    # Save mock discovery file
    discovery_file = project_root / "output" / "local_discovery_test.json"
    with open(discovery_file, 'w') as f:
        json.dump(discovery_data, f, indent=2)
        
    return str(discovery_file)

class LocalBatchProcessor(BatchExtractionProcessor):
    """Modified batch processor that works with local files"""
    
    def download_file(self, file_metadata, temp_dir):
        """Override to handle local files without downloading"""
        try:
            download_url = file_metadata.get('download_url', '')
            file_name = file_metadata.get('file_name')
            
            if download_url.startswith('file://'):
                # Local file - just return the path
                local_path = download_url.replace('file://', '')
                if os.path.exists(local_path):
                    self.logger.info(f"Using local file {file_name}")
                    return local_path
                else:
                    self.logger.warning(f"Local file not found: {local_path}")
                    return None
            else:
                # Use parent method for remote files
                return super().download_file(file_metadata, temp_dir)
                
        except Exception as e:
            self.logger.error(f"Failed to access {file_metadata.get('file_name', 'unknown')}: {e}")
            return None

def main():
    print("B&R Capital Dashboard - Local Batch Extraction Test")
    print("=" * 55)
    
    # Create mock discovery file with local files
    print("📁 Creating mock discovery file with local Excel files...")
    discovery_file = create_local_discovery_file()
    
    # Reference file path
    reference_file = Path("/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx")
    
    # Check files
    if not Path(discovery_file).exists():
        print(f"❌ Discovery file not created: {discovery_file}")
        return 1
        
    if not reference_file.exists():
        print(f"❌ Reference file not found: {reference_file}")
        return 1
        
    print(f"✅ Local discovery file: {Path(discovery_file).name}")
    print(f"✅ Reference file: {reference_file.name}")
    print()
    
    print("🚀 Starting local batch processing...")
    print("-" * 40)
    
    try:
        # Initialize local processor
        processor = LocalBatchProcessor(
            reference_file_path=str(reference_file),
            output_dir=str(project_root / "data" / "local_batch_test")
        )
        
        # Process batch
        results = processor.process_batch(
            discovery_file_path=discovery_file,
            max_workers=2,  # Fewer workers for local processing
            max_files=None  # Process all local files
        )
        
        # Print results
        print("\n" + "=" * 55)
        print("LOCAL BATCH PROCESSING RESULTS")
        print("=" * 55)
        
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
                
        # Show successful extractions
        if stats['successful_extractions'] > 0:
            print(f"\n✅ Successfully Processed Files:")
            # Read batch results to show successful files
            with open(results['output_files']['batch_results'], 'r') as f:
                batch_data = json.load(f)
            
            for result in batch_data:
                if result['processing_status'] == 'success':
                    print(f"  • {result['deal_name']}: {result['field_count']:,} fields")
        
        # Errors (if any)
        if stats['processing_errors']:
            print(f"\n❌ Processing Errors ({len(stats['processing_errors'])}):")
            for error in stats['processing_errors']:
                print(f"  • {error['file_name']}: {error['error']}")
                
        print("\n" + "=" * 55)
        if stats['successful_extractions'] > 0:
            print("🎉 Local batch processing completed successfully!")
            print("This demonstrates the batch processor works perfectly with valid files.")
        else:
            print("⚠️  No successful extractions. Check error messages above.")
        print(f"📂 Check output directory: {processor.output_dir}")
        
    except Exception as e:
        print(f"\n❌ Local batch processing failed: {e}")
        import traceback
        print(f"Full error:\n{traceback.format_exc()}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main())