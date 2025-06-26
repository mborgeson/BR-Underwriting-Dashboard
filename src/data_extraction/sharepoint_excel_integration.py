"""
Integration script connecting SharePoint file discovery with Excel extraction
This shows the complete data flow from SharePoint to extracted data
"""

import os
import io
import json
import requests
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

# Import from your extraction system
try:
    # Try relative import first (when imported as a module)
    from .excel_extraction_system import (
        CellMappingParser,
        ExcelDataExtractor,
        BatchFileProcessor,
        export_to_csv
    )
except ImportError:
    # Fall back to absolute import (when run directly)
    from excel_extraction_system import (
        CellMappingParser,
        ExcelDataExtractor,
        BatchFileProcessor,
        export_to_csv
    )

# Configuration
AZURE_CLIENT_ID = "5a620cea-31fe-40f6-8b48-d55bc5465dc9"
AZURE_TENANT_ID = "383e5745-a469-4712-aaa9-f7d79c981e10"
SHAREPOINT_SITE = "https://bandrcapital.sharepoint.com/sites/BRCapital-Internal"
DEALS_FOLDER = "/Real Estate/Deals"


class SharePointExcelExtractor:
    """Integrates SharePoint file access with Excel data extraction"""
    
    def __init__(self, reference_file_path: str, azure_client_secret: str):
        # Initialize SharePoint connection
        self.client_id = AZURE_CLIENT_ID
        self.tenant_id = AZURE_TENANT_ID
        self.client_secret = azure_client_secret
        self.access_token = None
        
        # Initialize extraction components
        print("Loading cell mappings...")
        parser = CellMappingParser(reference_file_path)
        self.mappings = parser.load_mappings()
        self.extractor = ExcelDataExtractor(self.mappings)
        self.processor = BatchFileProcessor(self.extractor)
        
        print(f"Initialized with {len(self.mappings)} cell mappings")
    
    def authenticate(self):
        """Get access token for Microsoft Graph API"""
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'https://graph.microsoft.com/.default',
            'grant_type': 'client_credentials'
        }
        
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        
        self.access_token = response.json()['access_token']
        print("Successfully authenticated with SharePoint")
    
    def get_site_id(self) -> str:
        """Get SharePoint site ID"""
        # Parse site URL
        site_path = SHAREPOINT_SITE.replace("https://bandrcapital.sharepoint.com", "")
        
        headers = {'Authorization': f'Bearer {self.access_token}'}
        url = f"https://graph.microsoft.com/v1.0/sites/bandrcapital.sharepoint.com:{site_path}"
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        return response.json()['id']
    
    def get_real_estate_drive_id(self, site_id: str) -> str:
        """Get the ID of the Real Estate document library"""
        headers = {'Authorization': f'Bearer {self.access_token}'}
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        drives = response.json().get('value', [])
        
        for drive in drives:
            if drive.get('name') == 'Real Estate':
                drive_id = drive.get('id')
                print(f"Found Real Estate library with ID: {drive_id}")
                return drive_id
        
        # Log available drives for debugging
        print("Available document libraries:")
        for drive in drives:
            print(f"  - {drive.get('name')} (ID: {drive.get('id')})")
            
        raise ValueError("Real Estate document library not found")
    
    def discover_excel_files(self) -> List[Dict[str, Any]]:
        """Discover all eligible Excel files in SharePoint"""
        if not self.access_token:
            self.authenticate()
        
        site_id = self.get_site_id()
        real_estate_drive_id = self.get_real_estate_drive_id(site_id)
        headers = {'Authorization': f'Bearer {self.access_token}'}
        
        # Deal stages to scan
        deal_stages = [
            "0) Dead Deals",
            "1) Initial UW and Review", 
            "2) Active UW and Review",
            "3) Deals Under Contract",
            "4) Closed Deals",
            "5) Realized Deals"
        ]
        
        all_files = []
        
        # First, find the Deals folder in Real Estate library
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{real_estate_drive_id}/root/children"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to get Real Estate root items: {response.status_code}")
            return all_files
        
        items = response.json().get('value', [])
        deals_folder_id = None
        
        # Find the Deals folder
        for item in items:
            if item.get('folder') and item.get('name') == 'Deals':
                deals_folder_id = item.get('id')
                print(f"Found Deals folder with ID: {deals_folder_id}")
                break
        
        if not deals_folder_id:
            print("Deals folder not found in Real Estate library")
            return all_files
        
        # Get stage folders inside Deals
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{real_estate_drive_id}/items/{deals_folder_id}/children"
        
        while url:
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"Failed to get Deals folder contents: {response.status_code}")
                break
            
            data = response.json()
            stage_items = data.get('value', [])
            
            for stage_item in stage_items:
                if stage_item.get('folder'):
                    stage_name = stage_item.get('name')
                    
                    # Check if it's one of our expected stage folders
                    if any(stage_name.startswith(stage) for stage in deal_stages):
                        print(f"\nScanning stage folder: {stage_name}")
                        stage_files = self._scan_stage_folder(
                            site_id, 
                            real_estate_drive_id, 
                            stage_item.get('id'), 
                            stage_name
                        )
                        all_files.extend(stage_files)
            
            # Get next page URL if exists
            url = data.get('@odata.nextLink')
        
        print(f"\nTotal files found: {len(all_files)}")
        return all_files
    
    def _scan_stage_folder(self, site_id: str, drive_id: str, stage_folder_id: str, stage_name: str) -> List[Dict[str, Any]]:
        """Scan a specific stage folder for deal folders and their UW Model folders"""
        files = []
        headers = {'Authorization': f'Bearer {self.access_token}'}
        
        # Get all deal folders in the stage
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{stage_folder_id}/children"
        
        while url:
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"Failed to get stage folder contents: {response.status_code}")
                break
            
            data = response.json()
            items = data.get('value', [])
            
            for item in items:
                if item.get('folder'):
                    # This is a deal folder
                    deal_name = item.get('name')
                    deal_folder_id = item.get('id')
                    
                    # Look for UW Model folder within this deal
                    uw_files = self._scan_for_uw_model_folder(
                        site_id,
                        drive_id,
                        deal_folder_id,
                        deal_name,
                        stage_name
                    )
                    files.extend(uw_files)
            
            # Get next page URL if exists
            url = data.get('@odata.nextLink')
        
        return files
    
    def _scan_for_uw_model_folder(self, site_id: str, drive_id: str, deal_folder_id: str, 
                                  deal_name: str, stage_name: str) -> List[Dict[str, Any]]:
        """Look for UW Model folder within a specific deal folder"""
        files = []
        headers = {'Authorization': f'Bearer {self.access_token}'}
        
        # Get contents of the deal folder
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{deal_folder_id}/children"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            return files
        
        items = response.json().get('value', [])
        
        # Look for UW Model folder
        uw_model_folder_id = None
        for item in items:
            if item.get('folder') and 'UW Model' in item.get('name', ''):
                uw_model_folder_id = item.get('id')
                break
        
        if not uw_model_folder_id:
            return files
        
        # Scan files in the UW Model folder
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{uw_model_folder_id}/children"
        
        while url:
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                break
            
            data = response.json()
            items = data.get('value', [])
            
            for item in items:
                if item.get('file') and self._is_valid_file(item):
                    file_info = {
                        'file_id': item['id'],
                        'file_name': item['name'],
                        'file_path': f"Deals/{stage_name}/{deal_name}/UW Model/{item['name']}",
                        'deal_name': deal_name,  # Use exact deal folder name
                        'deal_stage': stage_name,
                        'modified_date': item['lastModifiedDateTime'],
                        'size_mb': item['size'] / (1024 * 1024),
                        'download_url': item.get('@microsoft.graph.downloadUrl'),
                        'drive_id': drive_id  # Store for later download
                    }
                    files.append(file_info)
                    print(f"  Found: {item['name']} in {deal_name}")
            
            # Get next page URL if exists
            url = data.get('@odata.nextLink')
        
        return files
    
    def _is_valid_file(self, file_item: Dict) -> bool:
        """Check if file meets criteria"""
        name = file_item.get('name', '').lower()
        
        # Check file type
        if not (name.endswith('.xlsb') or name.endswith('.xlsm')):
            return False
        
        # Check name includes "UW Model vCurrent"
        if 'uw model vcurrent' not in name:
            return False
        
        # Check excludes
        if 'speedboat' in name or 'vold' in name:
            return False
        
        # Check modified date
        modified_date = pd.to_datetime(file_item['lastModifiedDateTime'])
        cutoff_date = pd.to_datetime('2024-07-15')
        if modified_date < cutoff_date:
            return False
        
        return True
    
    def download_and_extract(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Download file from SharePoint and extract data"""
        print(f"\nProcessing: {file_info['file_name']}")
        
        try:
            # Download file content
            headers = {'Authorization': f'Bearer {self.access_token}'}
            
            if file_info.get('download_url'):
                # Use direct download URL if available
                response = requests.get(file_info['download_url'], headers=headers)
            else:
                # Use Graph API to download with drive_id
                site_id = self.get_site_id()
                drive_id = file_info.get('drive_id')
                file_id = file_info['file_id']
                url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{file_id}/content"
                response = requests.get(url, headers=headers)
            
            response.raise_for_status()
            
            # Extract data from file content
            extracted_data = self.extractor.extract_from_file(
                file_info['file_path'],
                response.content
            )
            
            # Add metadata
            extracted_data.update({
                '_deal_name': file_info['deal_name'],
                '_deal_stage': file_info['deal_stage'],
                '_file_modified_date': file_info['modified_date'],
                '_file_size_mb': file_info['size_mb']
            })
            
            return extracted_data
            
        except Exception as e:
            print(f"  Error: {str(e)}")
            return None
    
    def process_all_deals(self, output_dir: str = "./extraction_output"):
        """Complete pipeline: discover, download, extract, and save"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Step 1: Discover files
        print("\n" + "="*50)
        print("STEP 1: Discovering Excel files in SharePoint")
        print("="*50)
        
        files = self.discover_excel_files()
        
        # Save file manifest
        manifest_path = os.path.join(output_dir, "file_manifest.json")
        with open(manifest_path, 'w') as f:
            json.dump(files, f, indent=2, default=str)
        print(f"\nFile manifest saved to: {manifest_path}")
        
        # Step 2: Process files in batches
        print("\n" + "="*50)
        print("STEP 2: Extracting data from Excel files")
        print("="*50)
        
        # Convert to format expected by batch processor
        file_list = []
        for file_info in files:
            # Download file content
            result = self.download_and_extract(file_info)
            if result:
                file_list.append(result)
        
        # Step 3: Export results
        print("\n" + "="*50)
        print("STEP 3: Exporting results")
        print("="*50)
        
        if file_list:
            # Export to CSV
            csv_path = os.path.join(
                output_dir,
                f"extraction_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            export_to_csv(file_list, csv_path)
            print(f"Results exported to: {csv_path}")
            
            # Generate summary statistics
            self._generate_summary_report(file_list, output_dir)
        
        return file_list
    
    def _generate_summary_report(self, results: List[Dict], output_dir: str):
        """Generate summary report of extraction"""
        summary = {
            'extraction_date': datetime.now().isoformat(),
            'total_files_processed': len(results),
            'files_by_stage': {},
            'extraction_statistics': {
                'total_fields_attempted': len(self.mappings),
                'average_fields_extracted': 0,
                'average_extraction_time': 0
            },
            'deals_processed': []
        }
        
        # Aggregate by stage
        for result in results:
            stage = result.get('_deal_stage', 'Unknown')
            if stage not in summary['files_by_stage']:
                summary['files_by_stage'][stage] = 0
            summary['files_by_stage'][stage] += 1
            
            # Add deal info
            summary['deals_processed'].append({
                'deal_name': result.get('_deal_name'),
                'stage': stage,
                'fields_extracted': result.get('_extraction_metadata', {}).get('successful', 0),
                'extraction_time': result.get('_extraction_metadata', {}).get('duration_seconds', 0)
            })
        
        # Calculate averages
        if results:
            total_fields = sum(r.get('_extraction_metadata', {}).get('successful', 0) for r in results)
            total_time = sum(r.get('_extraction_metadata', {}).get('duration_seconds', 0) for r in results)
            
            summary['extraction_statistics']['average_fields_extracted'] = total_fields / len(results)
            summary['extraction_statistics']['average_extraction_time'] = total_time / len(results)
        
        # Save summary
        summary_path = os.path.join(output_dir, "extraction_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\nSummary report saved to: {summary_path}")
        
        # Print summary to console
        print("\nExtraction Summary:")
        print(f"  - Total files processed: {summary['total_files_processed']}")
        print(f"  - Average fields extracted: {summary['extraction_statistics']['average_fields_extracted']:.0f}")
        print(f"  - Average extraction time: {summary['extraction_statistics']['average_extraction_time']:.2f}s")
        print("\n  Files by stage:")
        for stage, count in summary['files_by_stage'].items():
            print(f"    - {stage}: {count}")


def main():
    """Main entry point for integrated extraction"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Extract B&R Capital underwriting data from SharePoint"
    )
    parser.add_argument(
        "--reference-file",
        default=r"/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx",
        help="Path to Excel reference file"
    )
    parser.add_argument(
        "--output-dir",
        default="./sharepoint_extraction_output",
        help="Output directory for results"
    )
    parser.add_argument(
        "--client-secret",
        required=True,
        help="Azure AD client secret"
    )
    
    args = parser.parse_args()
    
    # Create extractor
    extractor = SharePointExcelExtractor(
        args.reference_file,
        args.client_secret
    )
    
    # Run complete pipeline
    results = extractor.process_all_deals(args.output_dir)
    
    print(f"\n{'='*50}")
    print(f"Extraction complete! Processed {len(results)} files.")
    print(f"Check {args.output_dir} for results.")


if __name__ == "__main__":
    # For testing, you can also run directly:
    client_secret = os.getenv("AZURE_CLIENT_SECRET", "YOUR_CLIENT_SECRET_HERE")
    reference_file = r"/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx"
    
    extractor = SharePointExcelExtractor(reference_file, client_secret)
    results = extractor.process_all_deals()