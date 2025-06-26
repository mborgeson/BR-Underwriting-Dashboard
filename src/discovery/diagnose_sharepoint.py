# src/discovery/diagnose_sharepoint.py
import logging
import requests
import json
from pathlib import Path
import sys
from urllib.parse import quote, unquote

# Add project root to path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import settings
from src.auth.sharepoint_auth import SharePointAuthenticator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SharePointDiagnostics:
    """Diagnostic tool to explore SharePoint structure"""
    
    def __init__(self, authenticator):
        self.auth = authenticator
        self.site_id = None
        
    def run_diagnostics(self):
        """Run complete diagnostics"""
        logger.info("=== SharePoint Diagnostics ===")
        
        # 1. Get site information
        self.get_site_info()
        
        # 2. List all drives
        self.list_drives()
        
        # 3. Explore root folders
        self.explore_root()
        
        # 4. Try different path formats
        self.test_path_formats()
    
    def get_site_info(self):
        """Get detailed site information"""
        logger.info("\n1. Getting Site Information...")
        
        try:
            # Get site by path
            url = f"https://graph.microsoft.com/v1.0/sites/{settings.sharepoint.hostname}:/sites/{settings.sharepoint.site_name}"
            response = requests.get(url, headers=self.auth.get_headers())
            
            if response.status_code == 200:
                site_data = response.json()
                self.site_id = site_data['id']
                logger.info(f"Site Name: {site_data.get('displayName', 'N/A')}")
                logger.info(f"Site ID: {self.site_id}")
                logger.info(f"Web URL: {site_data.get('webUrl', 'N/A')}")
                
                # Pretty print full site data
                logger.info("Full site data:")
                print(json.dumps(site_data, indent=2))
            else:
                logger.error(f"Failed to get site info: {response.status_code}")
                logger.error(response.text)
                
        except Exception as e:
            logger.error(f"Error getting site info: {str(e)}")
    
    def list_drives(self):
        """List all document libraries (drives) in the site"""
        logger.info("\n2. Listing All Document Libraries...")
        
        try:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
            response = requests.get(url, headers=self.auth.get_headers())
            
            if response.status_code == 200:
                drives = response.json().get('value', [])
                logger.info(f"Found {len(drives)} document libraries:")
                
                for drive in drives:
                    logger.info(f"\nDrive Name: {drive.get('name', 'N/A')}")
                    logger.info(f"Drive ID: {drive.get('id', 'N/A')}")
                    logger.info(f"Drive Type: {drive.get('driveType', 'N/A')}")
                    logger.info(f"Web URL: {drive.get('webUrl', 'N/A')}")
                    
                    # Store the main documents library ID
                    if drive.get('name') == 'Documents' or 'document' in drive.get('name', '').lower():
                        self.documents_drive_id = drive.get('id')
                        logger.info("*** This appears to be the main documents library ***")
                        
            else:
                logger.error(f"Failed to list drives: {response.status_code}")
                logger.error(response.text)
                
        except Exception as e:
            logger.error(f"Error listing drives: {str(e)}")
    
    def explore_root(self):
        """Explore root folders in the main drive"""
        logger.info("\n3. Exploring Root Folders...")
        
        if not hasattr(self, 'documents_drive_id'):
            logger.warning("No documents drive found, trying default drive")
            # Try to get default drive
            try:
                url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive"
                response = requests.get(url, headers=self.auth.get_headers())
                if response.status_code == 200:
                    self.documents_drive_id = response.json().get('id')
                else:
                    logger.error("Could not get default drive")
                    return
            except:
                return
        
        try:
            # List root items
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.documents_drive_id}/root/children"
            response = requests.get(url, headers=self.auth.get_headers())
            
            if response.status_code == 200:
                items = response.json().get('value', [])
                logger.info(f"Found {len(items)} items in root:")
                
                for item in items:
                    item_type = "Folder" if item.get('folder') else "File"
                    logger.info(f"\n{item_type}: {item.get('name', 'N/A')}")
                    logger.info(f"  ID: {item.get('id', 'N/A')}")
                    logger.info(f"  Path: {item.get('parentReference', {}).get('path', 'N/A')}")
                    
                    # If it's "Real Estate" folder, explore it
                    if item.get('name') == 'Real Estate' and item.get('folder'):
                        self.explore_folder(item.get('id'), item.get('name'))
                        
            else:
                logger.error(f"Failed to explore root: {response.status_code}")
                logger.error(response.text)
                
        except Exception as e:
            logger.error(f"Error exploring root: {str(e)}")
    
    def explore_folder(self, folder_id, folder_name):
        """Explore a specific folder"""
        logger.info(f"\n  Exploring folder: {folder_name}")
        
        try:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.documents_drive_id}/items/{folder_id}/children"
            response = requests.get(url, headers=self.auth.get_headers())
            
            if response.status_code == 200:
                items = response.json().get('value', [])
                logger.info(f"  Found {len(items)} items in {folder_name}:")
                
                for item in items[:10]:  # Limit to first 10 items
                    item_type = "Folder" if item.get('folder') else "File"
                    logger.info(f"    {item_type}: {item.get('name', 'N/A')}")
                    
                    # If it's "Deals" folder, explore it further
                    if item.get('name') == 'Deals' and item.get('folder'):
                        self.explore_deals_folder(item.get('id'))
                        
            else:
                logger.error(f"Failed to explore folder {folder_name}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error exploring folder: {str(e)}")
    
    def explore_deals_folder(self, deals_folder_id):
        """Explore the Deals folder specifically"""
        logger.info("\n    Exploring Deals folder...")
        
        try:
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.documents_drive_id}/items/{deals_folder_id}/children"
            response = requests.get(url, headers=self.auth.get_headers())
            
            if response.status_code == 200:
                items = response.json().get('value', [])
                logger.info(f"    Found {len(items)} items in Deals folder:")
                
                for item in items:
                    if item.get('folder'):
                        logger.info(f"      Stage Folder: {item.get('name', 'N/A')}")
                        logger.info(f"        ID: {item.get('id', 'N/A')}")
                        
        except Exception as e:
            logger.error(f"Error exploring Deals folder: {str(e)}")
    
    def test_path_formats(self):
        """Test different path formats to access folders"""
        logger.info("\n4. Testing Different Path Formats...")
        
        test_paths = [
            "/Real Estate/Deals",
            "Real Estate/Deals",
            "/sites/BRCapital-Internal/Real Estate/Deals",
            "Shared Documents/Real Estate/Deals",
            "/Shared Documents/Real Estate/Deals",
        ]
        
        for path in test_paths:
            logger.info(f"\nTesting path: {path}")
            
            try:
                # Try path-based access
                encoded_path = quote(path, safe='')
                url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root:/{encoded_path}"
                response = requests.get(url, headers=self.auth.get_headers())
                
                if response.status_code == 200:
                    logger.info(f"  SUCCESS! Path format works: {path}")
                    item_data = response.json()
                    logger.info(f"  Item type: {'Folder' if item_data.get('folder') else 'File'}")
                    logger.info(f"  Item ID: {item_data.get('id')}")
                    
                    # Try to list children
                    children_url = f"{url}:/children"
                    children_response = requests.get(children_url, headers=self.auth.get_headers())
                    if children_response.status_code == 200:
                        children = children_response.json().get('value', [])
                        logger.info(f"  Found {len(children)} children")
                        for child in children[:5]:
                            logger.info(f"    - {child.get('name')}")
                else:
                    logger.info(f"  Failed with status: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"  Error: {str(e)}")

def main():
    """Run diagnostics"""
    try:
        # Initialize authenticator
        auth = SharePointAuthenticator(settings)
        
        # Run diagnostics
        diagnostics = SharePointDiagnostics(auth)
        diagnostics.run_diagnostics()
        
    except Exception as e:
        logger.error(f"Diagnostics failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()