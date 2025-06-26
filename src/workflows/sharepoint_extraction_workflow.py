#!/usr/bin/env python3
"""
SharePoint to Dashboard Extraction Workflow
Downloads Excel files from SharePoint and extracts data using the B&R extraction system
"""

import json
import os
import tempfile
import requests
from pathlib import Path
from typing import List, Dict, Any
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from data_extraction.excel_extraction_system import (
    CellMappingParser, 
    ExcelDataExtractor, 
    BatchFileProcessor,
    export_to_csv
)
# Simple version - use pre-authenticated download URLs

class SharePointExtractionWorkflow:
    """Downloads SharePoint files and runs extraction workflow"""
    
    def __init__(self, reference_file_path: str, discovered_files_path: str, output_dir: str):
        self.reference_file_path = reference_file_path
        self.discovered_files_path = discovered_files_path
        self.output_dir = output_dir
        # Note: Using pre-authenticated download URLs from discovery
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
    def run_complete_workflow(self, max_files: int = None) -> Dict[str, Any]:
        """Run the complete workflow: download files and extract data"""
        
        print(f"🚀 Starting SharePoint Extraction Workflow")
        print(f"📁 Output directory: {self.output_dir}")
        
        # Step 1: Load discovered files
        print(f"\n📋 Step 1: Loading discovered files...")
        with open(self.discovered_files_path, 'r') as f:
            discovered_files = json.load(f)
        
        if max_files:
            discovered_files = discovered_files[:max_files]
            print(f"   Limited to first {max_files} files for testing")
        
        print(f"   Found {len(discovered_files)} files to process")
        
        # Step 2: Load cell mappings
        print(f"\n🗺️  Step 2: Loading cell mappings...")
        parser = CellMappingParser(self.reference_file_path)
        mappings = parser.load_mappings()
        print(f"   Loaded {len(mappings)} cell mappings")
        
        # Step 3: Download and process files
        print(f"\n⬇️  Step 3: Downloading and processing files...")
        extractor = ExcelDataExtractor(mappings)
        results = []
        failed_files = []
        
        # Use pre-authenticated download URLs (tokens embedded)
        
        for i, file_info in enumerate(discovered_files, 1):
            file_name = file_info['file_name']
            download_url = file_info['download_url']
            
            print(f"   [{i}/{len(discovered_files)}] Processing: {file_name}")
            
            try:
                # Download file to temporary location
                with tempfile.NamedTemporaryFile(suffix='.xlsb', delete=False) as temp_file:
                    temp_path = temp_file.name
                    
                    # Download file content (URL already contains auth token)
                    response = requests.get(download_url, timeout=30)
                    response.raise_for_status()
                    
                    temp_file.write(response.content)
                    temp_file.flush()
                    
                    # Extract data from downloaded file
                    extracted_data = extractor.extract_from_file(temp_path, None)
                    
                    # Add metadata from SharePoint
                    extracted_data.update({
                        '_file_name': file_name,
                        '_deal_name': file_info.get('deal_name'),
                        '_deal_stage': file_info.get('deal_stage'),
                        '_file_modified_date': file_info.get('last_modified'),
                        '_file_size_mb': file_info.get('size_mb'),
                        '_sharepoint_file_id': file_info.get('file_id')
                    })
                    
                    results.append(extracted_data)
                    print(f"      ✅ Success: {extracted_data['_extraction_metadata']['successful']} fields extracted")
                    
                # Clean up temp file
                os.unlink(temp_path)
                    
            except Exception as e:
                error_msg = f"Failed to process {file_name}: {str(e)}"
                print(f"      ❌ Error: {error_msg}")
                failed_files.append({
                    'file_name': file_name,
                    'error': str(e)
                })
        
        # Step 4: Export results
        print(f"\n💾 Step 4: Exporting results...")
        
        # Export to CSV
        if results:
            csv_output_path = os.path.join(self.output_dir, f"sharepoint_extraction_results.csv")
            export_to_csv(results, csv_output_path)
            print(f"   ✅ Results exported to: {csv_output_path}")
        
        # Export summary
        summary = {
            'total_files_discovered': len(discovered_files),
            'total_files_processed': len(results),
            'total_files_failed': len(failed_files),
            'total_fields_extracted': sum(r['_extraction_metadata']['successful'] for r in results),
            'failed_files': failed_files,
            'output_files': {
                'csv_results': csv_output_path if results else None,
                'summary': os.path.join(self.output_dir, "workflow_summary.json")
            }
        }
        
        # Save summary
        summary_path = os.path.join(self.output_dir, "workflow_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n🎉 Workflow Complete!")
        print(f"   📊 Files processed: {len(results)}/{len(discovered_files)}")
        print(f"   📋 Total fields extracted: {summary['total_fields_extracted']}")
        print(f"   ❌ Failed files: {len(failed_files)}")
        print(f"   📄 Summary: {summary_path}")
        
        return summary

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="SharePoint to Dashboard Extraction Workflow")
    parser.add_argument("--reference-file", required=True, help="Path to cell mapping reference file")
    parser.add_argument("--discovered-files", required=True, help="Path to discovered files JSON")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument("--max-files", type=int, help="Limit number of files to process (for testing)")
    
    args = parser.parse_args()
    
    # Run workflow
    workflow = SharePointExtractionWorkflow(
        args.reference_file,
        args.discovered_files, 
        args.output_dir
    )
    
    summary = workflow.run_complete_workflow(args.max_files)
    
    # Print final summary
    print(f"\n📈 Final Results:")
    print(f"   Success rate: {summary['total_files_processed']}/{summary['total_files_discovered']} files")
    if summary['total_files_processed'] > 0:
        avg_fields = summary['total_fields_extracted'] / summary['total_files_processed']
        print(f"   Average fields per file: {avg_fields:.1f}")

if __name__ == "__main__":
    main()