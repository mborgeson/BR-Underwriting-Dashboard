"""
B&R Capital Dashboard - Batch Excel Extraction Processor

This module handles batch processing of multiple Excel files discovered from SharePoint.
It processes all files from the discovery results, tracks progress, handles errors,
and generates consolidated outputs.

Key Features:
- Processes all files from SharePoint discovery results
- Progress tracking with detailed logging
- Error recovery and retry logic
- Consolidated output generation
- Parallel processing for improved performance
- Detailed batch summary reports
"""

import os
import sys
import json
import logging
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.data_extraction.excel_extraction_system import ExcelDataExtractor, CellMappingParser
from src.data_extraction.sharepoint_excel_integration import SharePointExcelExtractor

class BatchExtractionProcessor:
    """
    Handles batch processing of multiple Excel files from SharePoint discovery results
    """
    
    def __init__(self, reference_file_path: str, output_dir: str = None):
        """
        Initialize the batch processor
        
        Args:
            reference_file_path: Path to the Excel reference file with cell mappings
            output_dir: Directory for output files (defaults to project/data/batch_extractions)
        """
        self.reference_file_path = reference_file_path
        self.output_dir = output_dir or str(project_root / "data" / "batch_extractions")
        
        # Create output directory if it doesn't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        
        # Load cell mappings and initialize the Excel extractor
        parser = CellMappingParser(reference_file_path)
        mappings = parser.load_mappings()
        self.extractor = ExcelDataExtractor(mappings)
        
        # Processing stats
        self.stats = {
            'total_files': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_fields_extracted': 0,
            'start_time': None,
            'end_time': None,
            'processing_errors': []
        }
        
        # Set up logging
        self.setup_logging()
        
    def setup_logging(self):
        """Configure logging for batch processing"""
        log_file = Path(self.output_dir) / f"batch_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def load_discovery_results(self, discovery_file_path: str) -> List[Dict]:
        """
        Load the SharePoint discovery results
        
        Args:
            discovery_file_path: Path to the discovery JSON file
            
        Returns:
            List of file metadata dictionaries
        """
        try:
            with open(discovery_file_path, 'r') as f:
                discovery_data = json.load(f)
            
            self.logger.info(f"Loaded {len(discovery_data)} files from discovery results")
            return discovery_data
            
        except Exception as e:
            self.logger.error(f"Failed to load discovery results: {e}")
            raise
            
    def download_file(self, file_metadata: Dict, temp_dir: str) -> Optional[str]:
        """
        Download a file from SharePoint using the metadata
        
        Args:
            file_metadata: File metadata from discovery results
            temp_dir: Temporary directory for downloads
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            download_url = file_metadata.get('download_url')
            file_name = file_metadata.get('file_name')
            
            if not download_url:
                self.logger.warning(f"No download URL for {file_name}")
                return None
                
            # Create local file path
            local_file_path = Path(temp_dir) / file_name
            
            # Download file
            self.logger.info(f"Downloading {file_name}...")
            response = requests.get(download_url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Save file
            with open(local_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            self.logger.info(f"Successfully downloaded {file_name} ({file_metadata.get('size_mb', 0):.1f} MB)")
            return str(local_file_path)
            
        except Exception as e:
            self.logger.error(f"Failed to download {file_metadata.get('file_name', 'unknown')}: {e}")
            return None
            
    def process_single_file(self, file_metadata: Dict, temp_dir: str) -> Dict:
        """
        Process a single Excel file
        
        Args:
            file_metadata: File metadata from discovery results
            temp_dir: Temporary directory for downloads
            
        Returns:
            Processing result dictionary
        """
        file_name = file_metadata.get('file_name', 'unknown')
        deal_name = file_metadata.get('deal_name', 'unknown')
        
        result = {
            'file_name': file_name,
            'deal_name': deal_name,
            'deal_stage': file_metadata.get('deal_stage'),
            'stage_index': file_metadata.get('stage_index'),
            'file_size_mb': file_metadata.get('size_mb'),
            'last_modified': file_metadata.get('last_modified'),
            'processing_status': 'failed',
            'extraction_data': None,
            'field_count': 0,
            'processing_time_seconds': 0,
            'error_message': None
        }
        
        start_time = time.time()
        
        try:
            # Download file
            local_file_path = self.download_file(file_metadata, temp_dir)
            if not local_file_path:
                result['error_message'] = "Download failed"
                return result
                
            # Extract data
            self.logger.info(f"Extracting data from {file_name}...")
            extraction_result = self.extractor.extract_from_file(local_file_path)
            
            # Clean up downloaded file
            try:
                os.remove(local_file_path)
            except:
                pass
                
            # Process results
            if extraction_result and isinstance(extraction_result, dict):
                # Filter out metadata fields to get actual extracted data
                extracted_fields = {k: v for k, v in extraction_result.items() 
                                  if not k.startswith('_')}
                
                result['processing_status'] = 'success'
                result['extraction_data'] = extracted_fields
                result['field_count'] = len(extracted_fields)
                result['processing_time_seconds'] = time.time() - start_time
                
                self.logger.info(f"Successfully extracted {result['field_count']} fields from {file_name}")
                
            else:
                result['error_message'] = "No data extracted"
                
        except Exception as e:
            result['error_message'] = str(e)
            result['processing_time_seconds'] = time.time() - start_time
            self.logger.error(f"Error processing {file_name}: {e}")
            
            # Log full traceback for debugging
            self.logger.debug(f"Full traceback for {file_name}:\n{traceback.format_exc()}")
            
        return result
        
    def process_batch(self, discovery_file_path: str, max_workers: int = 3, 
                     max_files: Optional[int] = None) -> Dict:
        """
        Process a batch of files from discovery results
        
        Args:
            discovery_file_path: Path to discovery JSON file
            max_workers: Maximum number of parallel workers
            max_files: Maximum number of files to process (for testing)
            
        Returns:
            Batch processing results
        """
        self.logger.info("Starting batch extraction process...")
        self.stats['start_time'] = datetime.now()
        
        # Load discovery results
        discovery_data = self.load_discovery_results(discovery_file_path)
        
        # Limit files if specified (for testing)
        if max_files:
            discovery_data = discovery_data[:max_files]
            self.logger.info(f"Limited to {max_files} files for testing")
            
        self.stats['total_files'] = len(discovery_data)
        
        # Create temporary directory for downloads
        temp_dir = Path(self.output_dir) / "temp_downloads"
        temp_dir.mkdir(exist_ok=True)
        
        batch_results = []
        
        try:
            # Process files with parallel workers
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_file = {
                    executor.submit(self.process_single_file, file_metadata, str(temp_dir)): file_metadata
                    for file_metadata in discovery_data
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_file):
                    file_metadata = future_to_file[future]
                    try:
                        result = future.result()
                        batch_results.append(result)
                        
                        # Update stats
                        if result['processing_status'] == 'success':
                            self.stats['successful_extractions'] += 1
                            self.stats['total_fields_extracted'] += result['field_count']
                        else:
                            self.stats['failed_extractions'] += 1
                            self.stats['processing_errors'].append({
                                'file_name': result['file_name'],
                                'error': result['error_message']
                            })
                            
                        # Progress update
                        completed = len(batch_results)
                        self.logger.info(f"Progress: {completed}/{self.stats['total_files']} files processed")
                        
                    except Exception as e:
                        self.logger.error(f"Failed to process {file_metadata.get('file_name', 'unknown')}: {e}")
                        
        finally:
            # Clean up temporary directory
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except:
                pass
                
        self.stats['end_time'] = datetime.now()
        
        # Generate consolidated outputs
        return self.generate_outputs(batch_results)
        
    def generate_outputs(self, batch_results: List[Dict]) -> Dict:
        """
        Generate consolidated output files and summary
        
        Args:
            batch_results: List of processing results
            
        Returns:
            Summary of outputs generated
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. Save raw batch results
        results_file = Path(self.output_dir) / f"batch_results_{timestamp}.json"
        with open(results_file, 'w') as f:
            json.dump(batch_results, f, indent=2, default=str)
            
        # 2. Create consolidated data file (successful extractions only)
        consolidated_data = []
        for result in batch_results:
            if result['processing_status'] == 'success' and result['extraction_data']:
                # Add metadata to each extracted record
                for field_name, field_value in result['extraction_data'].items():
                    consolidated_data.append({
                        'deal_name': result['deal_name'],
                        'deal_stage': result['deal_stage'],
                        'stage_index': result['stage_index'],
                        'file_name': result['file_name'],
                        'last_modified': result['last_modified'],
                        'field_name': field_name,
                        'field_value': field_value,
                        'extraction_timestamp': self.stats['start_time'].isoformat()
                    })
                    
        # Save consolidated data
        if consolidated_data:
            consolidated_file = Path(self.output_dir) / f"consolidated_extraction_{timestamp}.json"
            with open(consolidated_file, 'w') as f:
                json.dump(consolidated_data, f, indent=2, default=str)
                
            # Also save as CSV for easier analysis
            df = pd.DataFrame(consolidated_data)
            csv_file = Path(self.output_dir) / f"consolidated_extraction_{timestamp}.csv"
            df.to_csv(csv_file, index=False)
            
        # 3. Generate summary report
        summary = self.generate_summary_report(batch_results, timestamp)
        
        # 4. Generate processing log summary
        self.generate_processing_summary(batch_results, timestamp)
        
        return {
            'summary': summary,
            'output_files': {
                'batch_results': str(results_file),
                'consolidated_json': str(consolidated_file) if consolidated_data else None,
                'consolidated_csv': str(csv_file) if consolidated_data else None
            },
            'stats': self.stats
        }
        
    def generate_summary_report(self, batch_results: List[Dict], timestamp: str) -> Dict:
        """Generate a comprehensive summary report"""
        
        # Calculate processing time
        total_time = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        # Analyze results by deal stage
        stage_analysis = {}
        for result in batch_results:
            stage = result.get('deal_stage', 'Unknown')
            if stage not in stage_analysis:
                stage_analysis[stage] = {'total': 0, 'successful': 0, 'failed': 0}
            stage_analysis[stage]['total'] += 1
            if result['processing_status'] == 'success':
                stage_analysis[stage]['successful'] += 1
            else:
                stage_analysis[stage]['failed'] += 1
                
        # Top performing files (by field count)
        successful_results = [r for r in batch_results if r['processing_status'] == 'success']
        top_files = sorted(successful_results, key=lambda x: x['field_count'], reverse=True)[:10]
        
        summary = {
            'batch_processing_summary': {
                'timestamp': timestamp,
                'total_processing_time_seconds': round(total_time, 2),
                'total_files_processed': self.stats['total_files'],
                'successful_extractions': self.stats['successful_extractions'],
                'failed_extractions': self.stats['failed_extractions'],
                'success_rate_percent': round((self.stats['successful_extractions'] / self.stats['total_files']) * 100, 1) if self.stats['total_files'] > 0 else 0,
                'total_fields_extracted': self.stats['total_fields_extracted'],
                'average_fields_per_file': round(self.stats['total_fields_extracted'] / max(self.stats['successful_extractions'], 1), 1),
                'average_processing_time_per_file': round(total_time / max(self.stats['total_files'], 1), 2)
            },
            'results_by_deal_stage': stage_analysis,
            'top_performing_files': [
                {
                    'deal_name': f['deal_name'],
                    'file_name': f['file_name'],
                    'field_count': f['field_count'],
                    'processing_time': round(f['processing_time_seconds'], 2)
                }
                for f in top_files
            ],
            'processing_errors': self.stats['processing_errors']
        }
        
        # Save summary report
        summary_file = Path(self.output_dir) / f"batch_summary_{timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
            
        return summary
        
    def generate_processing_summary(self, batch_results: List[Dict], timestamp: str):
        """Generate a human-readable processing summary"""
        
        summary_text = f"""
# B&R Capital Dashboard - Batch Extraction Summary
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Processing Overview
- **Total Files**: {self.stats['total_files']}
- **Successful Extractions**: {self.stats['successful_extractions']}
- **Failed Extractions**: {self.stats['failed_extractions']}
- **Success Rate**: {round((self.stats['successful_extractions'] / self.stats['total_files']) * 100, 1) if self.stats['total_files'] > 0 else 0}%
- **Total Fields Extracted**: {self.stats['total_fields_extracted']:,}
- **Processing Time**: {(self.stats['end_time'] - self.stats['start_time']).total_seconds():.1f} seconds

## Results by Deal Stage
"""
        
        # Add stage breakdown
        stage_stats = {}
        for result in batch_results:
            stage = result.get('deal_stage', 'Unknown')
            if stage not in stage_stats:
                stage_stats[stage] = {'total': 0, 'successful': 0}
            stage_stats[stage]['total'] += 1
            if result['processing_status'] == 'success':
                stage_stats[stage]['successful'] += 1
                
        for stage, stats in stage_stats.items():
            success_rate = (stats['successful'] / stats['total']) * 100 if stats['total'] > 0 else 0
            summary_text += f"- **{stage}**: {stats['successful']}/{stats['total']} files ({success_rate:.1f}% success)\n"
            
        # Add error summary if any
        if self.stats['processing_errors']:
            summary_text += "\n## Processing Errors\n"
            for error in self.stats['processing_errors']:
                summary_text += f"- **{error['file_name']}**: {error['error']}\n"
                
        # Save summary text
        summary_file = Path(self.output_dir) / f"processing_summary_{timestamp}.md"
        with open(summary_file, 'w') as f:
            f.write(summary_text)


def main():
    """Command line interface for batch processing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='B&R Capital Batch Excel Extraction')
    parser.add_argument('--discovery-file', required=True, help='Path to discovery results JSON file')
    parser.add_argument('--reference-file', required=True, help='Path to Excel reference file')
    parser.add_argument('--output-dir', help='Output directory for results')
    parser.add_argument('--max-workers', type=int, default=3, help='Maximum parallel workers')
    parser.add_argument('--max-files', type=int, help='Maximum files to process (for testing)')
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = BatchExtractionProcessor(
        reference_file_path=args.reference_file,
        output_dir=args.output_dir
    )
    
    # Process batch
    results = processor.process_batch(
        discovery_file_path=args.discovery_file,
        max_workers=args.max_workers,
        max_files=args.max_files
    )
    
    # Print summary
    print(f"\n{'='*60}")
    print("BATCH PROCESSING COMPLETE")
    print(f"{'='*60}")
    print(f"Total Files: {results['stats']['total_files']}")
    print(f"Successful: {results['stats']['successful_extractions']}")
    print(f"Failed: {results['stats']['failed_extractions']}")
    print(f"Total Fields: {results['stats']['total_fields_extracted']:,}")
    print(f"Output Files: {len([f for f in results['output_files'].values() if f])}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()