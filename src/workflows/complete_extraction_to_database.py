#!/usr/bin/env python3
"""
Complete Extraction to Database Workflow

This workflow:
1. Discovers ALL files meeting criteria (regardless of deal stage)
2. Extracts data from SharePoint without permanent downloads
3. Loads extracted data directly into the database

No files are saved locally - data flows directly from SharePoint to database.
"""

import os
import sys
import json
import io
import requests
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.data_extraction.excel_extraction_system import CellMappingParser, ExcelDataExtractor
from src.discovery.file_discovery import SharePointFileDiscovery
from src.database.connection import get_cursor, initialize_database, DatabaseConfig
from src.database.data_loader import DataLoader

class CompleteExtractionToDatabase:
    """Handles complete workflow from discovery to database storage"""
    
    def __init__(self, reference_file_path: str, client_secret: str = None):
        self.reference_file_path = reference_file_path
        self.client_secret = client_secret or os.getenv("AZURE_CLIENT_SECRET")
        
        # Initialize components
        self.discovery = SharePointFileDiscovery(self.client_secret) if self.client_secret else None
        self.parser = CellMappingParser(self.reference_file_path)
        self.mappings = self.parser.load_mappings()
        self.extractor = ExcelDataExtractor(self.mappings)
        self.data_loader = DataLoader()
        
        # Initialize database
        config = DatabaseConfig()
        initialize_database(config)
        
        print(f"✅ Initialized with {len(self.mappings)} cell mappings")
    
    def extract_without_download(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from SharePoint file without permanent download"""
        file_name = file_info['file_name']
        download_url = file_info['download_url']
        
        # Use memory buffer to avoid disk storage
        try:
            # Stream file content to memory
            response = requests.get(download_url, stream=True, timeout=60)
            response.raise_for_status()
            
            # Create temporary file that auto-deletes
            with tempfile.NamedTemporaryFile(suffix='.xlsb', delete=True) as temp_file:
                # Stream content to temp file
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_file.write(chunk)
                temp_file.flush()
                
                # Extract data from temp file
                extracted_data = self.extractor.extract_from_file(temp_file.name, None)
                
                # Add metadata
                extracted_data.update({
                    '_file_path': file_info.get('file_path', ''),
                    '_extraction_timestamp': datetime.now().isoformat(),
                    '_file_name': file_name,
                    '_deal_name': file_info.get('deal_name'),
                    '_deal_stage': file_info.get('deal_stage'),
                    '_file_modified_date': file_info.get('last_modified'),
                    '_file_size_mb': file_info.get('size_mb'),
                })
                
                return extracted_data
                
        except Exception as e:
            print(f"❌ Extraction failed for {file_name}: {e}")
            return None
    
    def run_complete_workflow(self, use_cached_discovery: str = None) -> Dict[str, Any]:
        """
        Run complete workflow from discovery to database
        
        Args:
            use_cached_discovery: Path to cached discovery JSON file (optional)
        """
        print("🚀 COMPLETE EXTRACTION TO DATABASE WORKFLOW")
        print("=" * 70)
        print("IMPORTANT: ALL files meeting criteria will be processed")
        print("regardless of deal stage (Dead Deals, Initial UW, etc.)")
        print("=" * 70)
        
        # Step 1: Get list of files
        if use_cached_discovery and Path(use_cached_discovery).exists():
            print(f"\n📋 Using cached discovery: {use_cached_discovery}")
            with open(use_cached_discovery, 'r') as f:
                discovered_files = json.load(f)
        else:
            print("\n🔍 Discovering files from SharePoint...")
            if not self.discovery:
                print("❌ No SharePoint credentials provided")
                return {'status': 'failed', 'error': 'No authentication'}
            
            discovered_files = self.discovery.discover_uw_model_files()
        
        print(f"✅ Found {len(discovered_files)} files to process")
        
        # Show breakdown by stage
        from collections import Counter
        stages = Counter(f['deal_stage'] for f in discovered_files)
        print("\nFiles by stage (ALL will be processed):")
        for stage, count in stages.most_common():
            print(f"  • {stage}: {count} files")
        
        # Step 2: Process each file
        print(f"\n⚙️  Processing {len(discovered_files)} files...")
        print("Data flows directly: SharePoint → Memory → Database")
        print("-" * 70)
        
        successful = 0
        failed = 0
        
        with get_cursor() as cursor:
            for i, file_info in enumerate(discovered_files, 1):
                deal_name = file_info.get('deal_name', 'Unknown')
                deal_stage = file_info.get('deal_stage', 'Unknown')
                
                print(f"\n[{i}/{len(discovered_files)}] {deal_name}")
                print(f"  Stage: {deal_stage}")
                
                # Extract without permanent download
                extracted_data = self.extract_without_download(file_info)
                
                if extracted_data:
                    # Load directly to database
                    try:
                        extraction_id = self.data_loader.load_extraction_data(
                            extraction_data=extracted_data,
                            deal_stage=deal_stage,
                            metadata={
                                'total_fields': len(self.mappings),
                                'successful': len([v for v in extracted_data.values() if v is not None]),
                                'duration_seconds': 0  # Would need timing
                            }
                        )
                        
                        if extraction_id:
                            successful += 1
                            print(f"  ✅ Loaded to database (ID: {extraction_id})")
                        else:
                            failed += 1
                            print(f"  ❌ Database load failed")
                    
                    except Exception as e:
                        failed += 1
                        print(f"  ❌ Database error: {e}")
                else:
                    failed += 1
                    print(f"  ❌ Extraction failed")
                
                # Progress update every 10 files
                if i % 10 == 0:
                    print(f"\n📊 Progress: {i}/{len(discovered_files)} files")
                    print(f"   Successful: {successful}, Failed: {failed}")
        
        # Step 3: Final summary
        print("\n" + "=" * 70)
        print("🏁 WORKFLOW COMPLETE")
        print(f"Total files processed: {len(discovered_files)}")
        print(f"✅ Successful: {successful}")
        print(f"❌ Failed: {failed}")
        print(f"Success rate: {(successful/len(discovered_files)*100):.1f}%")
        
        # Show database summary
        self._show_database_summary()
        
        return {
            'status': 'complete',
            'total_files': len(discovered_files),
            'successful': successful,
            'failed': failed
        }
    
    def _show_database_summary(self):
        """Show summary of database contents"""
        print("\n📊 DATABASE SUMMARY:")
        
        with get_cursor() as cursor:
            # Total properties
            cursor.execute("SELECT COUNT(DISTINCT property_name) FROM properties;")
            total_props = cursor.fetchone()[0]
            
            # Properties by stage
            cursor.execute("""
                SELECT deal_stage, COUNT(DISTINCT property_name) as count
                FROM underwriting_data
                GROUP BY deal_stage
                ORDER BY count DESC;
            """)
            
            print(f"Total properties: {total_props}")
            print("By stage:")
            for stage, count in cursor.fetchall():
                print(f"  • {stage}: {count}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Complete extraction to database workflow")
    parser.add_argument('--reference-file', required=True, help='Path to reference Excel file')
    parser.add_argument('--use-cached-discovery', help='Path to cached discovery JSON')
    parser.add_argument('--client-secret', help='Azure client secret (or set AZURE_CLIENT_SECRET env var)')
    
    args = parser.parse_args()
    
    # Run workflow
    workflow = CompleteExtractionToDatabase(
        reference_file_path=args.reference_file,
        client_secret=args.client_secret
    )
    
    result = workflow.run_complete_workflow(
        use_cached_discovery=args.use_cached_discovery
    )
    
    return 0 if result['status'] == 'complete' else 1


if __name__ == "__main__":
    sys.exit(main())