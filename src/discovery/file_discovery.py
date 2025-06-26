# src/discovery/file_discovery.py
import os
from typing import List, Dict, Optional
from datetime import datetime
import pandas as pd
import requests
import logging
from urllib.parse import quote

logger = logging.getLogger(__name__)

class SharePointFileDiscovery:
    """Discovers and filters Excel files in SharePoint"""
    
    def __init__(self, authenticator, settings):
        self.auth = authenticator
        self.settings = settings
        self.site_id = None
        self.real_estate_drive_id = None
        self.discovered_files = []
        
    def discover_files(self) -> pd.DataFrame:
        """Main method to discover all relevant files"""
        logger.info("Starting file discovery process")
        
        # Get site ID
        self.site_id = self.auth.get_site_id()
        
        # Get Real Estate library ID (not the default Documents library)
        self.real_estate_drive_id = self._get_real_estate_drive_id()
        
        if not self.real_estate_drive_id:
            logger.error("Could not find Real Estate document library")
            return pd.DataFrame()
        
        # Find and scan the Deals folder
        all_files = self._scan_deals_folder()
        
        # Convert to DataFrame
        df = pd.DataFrame(all_files)
        
        if df.empty:
            logger.warning("No files found")
            return df
        
        # Apply filters
        df = self._apply_filters(df)
        
        # Sort by last modified date
        df = df.sort_values('last_modified', ascending=False)
        
        # Add summary statistics
        self._log_summary(df)
        
        return df
    
    def _get_real_estate_drive_id(self) -> Optional[str]:
        """Get the ID of the Real Estate document library"""
        try:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
            response = requests.get(url, headers=self.auth.get_headers())
            
            if response.status_code == 200:
                drives = response.json().get('value', [])
                
                for drive in drives:
                    if drive.get('name') == 'Real Estate':
                        drive_id = drive.get('id')
                        logger.info(f"Found Real Estate library with ID: {drive_id}")
                        return drive_id
                
                logger.error("Real Estate document library not found")
                
                # Log all available drives for debugging
                logger.info("Available document libraries:")
                for drive in drives:
                    logger.info(f"  - {drive.get('name')} (ID: {drive.get('id')})")
                    
            else:
                logger.error(f"Failed to list drives: {response.status_code}")
                logger.error(response.text)
                
        except Exception as e:
            logger.error(f"Error getting Real Estate drive: {str(e)}")
            
        return None
    
    def _scan_deals_folder(self) -> List[Dict]:
        """Scan the Deals folder in Real Estate library"""
        all_files = []
        
        try:
            # First, get the root items of Real Estate library to find Deals folder
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.real_estate_drive_id}/root/children"
            response = requests.get(url, headers=self.auth.get_headers())
            
            if response.status_code != 200:
                logger.error(f"Failed to get Real Estate root items: {response.status_code}")
                return all_files
            
            items = response.json().get('value', [])
            deals_folder_id = None
            
            # Find the Deals folder
            for item in items:
                if item.get('folder') and item.get('name') == 'Deals':
                    deals_folder_id = item.get('id')
                    logger.info(f"Found Deals folder with ID: {deals_folder_id}")
                    break
            
            if not deals_folder_id:
                logger.error("Deals folder not found in Real Estate library")
                return all_files
            
            # Now scan the stage folders inside Deals
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.real_estate_drive_id}/items/{deals_folder_id}/children"
            
            # Handle pagination
            while url:
                response = requests.get(url, headers=self.auth.get_headers())
                
                if response.status_code != 200:
                    logger.error(f"Failed to get Deals folder contents: {response.status_code}")
                    break
                
                data = response.json()
                items = data.get('value', [])
                
                for item in items:
                    if item.get('folder'):
                        # This should be a stage folder
                        folder_name = item.get('name')
                        
                        # Check if it's one of our expected stage folders
                        is_stage_folder = any(folder_name.startswith(stage) for stage in self.settings.deal_stages)
                        
                        if is_stage_folder:
                            logger.info(f"Scanning stage folder: {folder_name}")
                            stage_files = self._scan_stage_folder(item.get('id'), folder_name)
                            all_files.extend(stage_files)
                        else:
                            logger.info(f"Skipping non-stage folder: {folder_name}")
                
                # Get next page URL if exists
                url = data.get('@odata.nextLink')
                
        except Exception as e:
            logger.error(f"Error scanning Deals folder: {str(e)}", exc_info=True)
            
        return all_files
    
    def _scan_stage_folder(self, stage_folder_id: str, stage_name: str) -> List[Dict]:
        """Scan a specific stage folder for deal folders and their UW Model folders"""
        files = []
        deals_processed = 0
        uw_folders_found = 0
        files_found = 0
        
        try:
            # Get all deal folders in the stage
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.real_estate_drive_id}/items/{stage_folder_id}/children"
            
            logger.info(f"Looking for deal folders in {stage_name}...")
            
            while url:
                response = requests.get(url, headers=self.auth.get_headers())
                
                if response.status_code != 200:
                    logger.error(f"Failed to get stage folder contents: {response.status_code}")
                    break
                
                data = response.json()
                items = data.get('value', [])
                
                for item in items:
                    if item.get('folder'):
                        # This is a deal folder
                        deal_name = item.get('name')
                        deal_folder_id = item.get('id')
                        deals_processed += 1
                        
                        if deals_processed % 10 == 0:
                            logger.info(f"  Progress: Processed {deals_processed} deals, found {uw_folders_found} UW Model folders, {files_found} files...")
                        
                        # Look specifically for UW Model folder within this deal
                        uw_files = self._scan_for_uw_model_folder(deal_folder_id, deal_name, stage_name)
                        if uw_files:
                            uw_folders_found += 1
                            files_found += len(uw_files)
                            files.extend(uw_files)
                
                # Get next page URL if exists
                url = data.get('@odata.nextLink')
                
        except Exception as e:
            logger.error(f"Error scanning stage folder {stage_name}: {str(e)}")
        
        logger.info(f"Completed {stage_name}: Processed {deals_processed} deals, found {uw_folders_found} UW Model folders with {files_found} total files")
        return files
    
    def _scan_for_uw_model_folder(self, deal_folder_id: str, deal_name: str, stage_name: str) -> List[Dict]:
        """Look for UW Model folder within a specific deal folder"""
        files = []
        
        try:
            # Get contents of the deal folder
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.real_estate_drive_id}/items/{deal_folder_id}/children"
            response = requests.get(url, headers=self.auth.get_headers())
            
            if response.status_code != 200:
                logger.debug(f"Failed to get contents of deal folder {deal_name}: {response.status_code}")
                return files
            
            items = response.json().get('value', [])
            
            # Look for UW Model folder
            uw_model_folder_id = None
            for item in items:
                if item.get('folder') and 'UW Model' in item.get('name', ''):
                    uw_model_folder_id = item.get('id')
                    uw_model_folder_name = item.get('name')
                    logger.debug(f"Found UW Model folder in {deal_name}: {uw_model_folder_name}")
                    break
            
            if not uw_model_folder_id:
                logger.debug(f"No UW Model folder found in deal: {deal_name}")
                return files
            
            # Scan files in the UW Model folder
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.real_estate_drive_id}/items/{uw_model_folder_id}/children"
            
            while url:
                response = requests.get(url, headers=self.auth.get_headers())
                
                if response.status_code != 200:
                    logger.warning(f"Failed to get UW Model folder contents for {deal_name}: {response.status_code}")
                    break
                
                data = response.json()
                items = data.get('value', [])
                
                for item in items:
                    if item.get('file'):
                        # This is a file in the UW Model folder
                        folder_path = f"Deals/{stage_name}/{deal_name}/UW Model"
                        file_info = self._extract_file_info(item, folder_path, stage_name, deal_name)
                        if file_info:
                            files.append(file_info)
                
                # Get next page URL if exists
                url = data.get('@odata.nextLink')
                
        except Exception as e:
            logger.error(f"Error scanning UW Model folder for deal {deal_name}: {str(e)}")
        
        return files
    
    def _extract_file_info(self, item: Dict, folder_path: str, deal_stage: str, deal_name: str) -> Optional[Dict]:
        """Extract relevant information from file item"""
        try:
            file_name = item['name']
            
            # Quick pre-filter on extension
            if not any(file_name.lower().endswith(ext.lower()) 
                      for ext in self.settings.file_filter.file_extensions):
                return None
            
            file_info = {
                'file_id': item['id'],
                'file_name': file_name,
                'file_path': f"{folder_path}/{file_name}",
                'deal_name': deal_name,  # Now we have the exact deal name
                'deal_stage': deal_stage,
                'stage_index': next((i for i, stage in enumerate(self.settings.deal_stages) 
                                   if deal_stage.startswith(stage)), -1),
                'size_mb': round(item.get('size', 0) / (1024 * 1024), 2),
                'last_modified': datetime.fromisoformat(
                    item['lastModifiedDateTime'].replace('Z', '+00:00')
                ),
                'created_date': datetime.fromisoformat(
                    item['createdDateTime'].replace('Z', '+00:00')
                ),
                'web_url': item.get('webUrl', ''),
                'download_url': item.get('@microsoft.graph.downloadUrl', ''),
                # Store the drive ID for later download
                'drive_id': self.real_estate_drive_id
            }
            
            return file_info
            
        except Exception as e:
            logger.error(f"Error extracting file info for {item.get('name', 'unknown')}: {str(e)}")
            return None
    
    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply filtering criteria to discovered files"""
        if df.empty:
            return df
            
        initial_count = len(df)
        
        # Filter by file name pattern
        include_pattern = self.settings.file_filter.include_pattern
        df = df[df['file_name'].str.contains(include_pattern, case=False, na=False)]
        
        # Exclude patterns
        for pattern in self.settings.file_filter.exclude_patterns:
            df = df[~df['file_name'].str.contains(pattern, case=False, na=False)]
        
        # Filter by modification date
        df = df[df['last_modified'] >= pd.Timestamp(self.settings.file_filter.cutoff_date, tz='UTC')]
        
        # Remove duplicates (keep most recent per deal)
        df = df.sort_values('last_modified', ascending=False)
        df = df.drop_duplicates(subset=['deal_name', 'deal_stage'], keep='first')
        
        final_count = len(df)
        logger.info(f"Filtered files: {initial_count} -> {final_count}")
        
        return df
    
    def _log_summary(self, df: pd.DataFrame):
        """Log summary statistics"""
        if df.empty:
            logger.info("No files found matching criteria")
            return
            
        logger.info("\n=== File Discovery Summary ===")
        logger.info(f"Total files found: {len(df)}")
        logger.info(f"Date range: {df['last_modified'].min()} to {df['last_modified'].max()}")
        logger.info(f"Total size: {df['size_mb'].sum():.1f} MB")
        
        # Files by stage
        stage_counts = df['deal_stage'].value_counts().sort_index()
        logger.info("\nFiles by stage:")
        for stage, count in stage_counts.items():
            logger.info(f"  {stage}: {count}")
        
        # Top 5 most recent files
        logger.info("\nMost recent files:")
        recent_files = df.nlargest(5, 'last_modified')[['deal_name', 'deal_stage', 'last_modified', 'size_mb']]
        for _, row in recent_files.iterrows():
            logger.info(f"  {row['deal_name']} | {row['deal_stage']} | {row['last_modified'].strftime('%Y-%m-%d')} | {row['size_mb']:.1f} MB")