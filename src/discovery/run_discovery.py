# src/discovery/run_discovery.py
import logging
import pandas as pd
from datetime import datetime
import json
import sys
import os
from pathlib import Path

# Add project root to path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from src.config.settings import settings, ConfigurationError
    from src.auth.sharepoint_auth import SharePointAuthenticator
    from src.discovery.file_discovery import SharePointFileDiscovery
except ImportError as e:
    print(f"Import error: {e}")
    print(f"Project root: {project_root}")
    raise

# Configure logging
log_dir = project_root / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f'discovery_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def check_configuration():
    """Check if configuration is properly loaded"""
    logger.info("Checking configuration...")
    logger.info(f"Azure Client ID: {settings.azure.client_id[:10]}..." if settings.azure.client_id else "Azure Client ID: NOT SET")
    logger.info(f"Azure Tenant ID: {settings.azure.tenant_id[:10]}..." if settings.azure.tenant_id else "Azure Tenant ID: NOT SET")
    logger.info(f"SharePoint Site: {settings.sharepoint.site_name}")
    logger.info(f"SharePoint Hostname: {settings.sharepoint.hostname}")

def run_file_discovery():
    """Main function to run file discovery"""
    try:
        logger.info("=== Starting B&R Capital File Discovery ===")
        
        # Check configuration
        check_configuration()
        
        # Initialize authenticator
        logger.info("Initializing authenticator...")
        auth = SharePointAuthenticator(settings)
        
        # Test authentication
        logger.info("Testing authentication...")
        token = auth.get_access_token()
        logger.info("Authentication successful")
        
        # Initialize file discovery
        discovery = SharePointFileDiscovery(auth, settings)
        
        # Discover files
        logger.info("Starting file discovery...")
        files_df = discovery.discover_files()
        
        if files_df.empty:
            logger.warning("No files found matching criteria")
            return None
        
        # Save results
        output_dir = project_root / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Save as CSV
        csv_path = output_dir / f"discovered_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        files_df.to_csv(csv_path, index=False)
        logger.info(f"Results saved to: {csv_path}")
        
        # Save as JSON for detailed review
        json_path = output_dir / f"discovered_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        files_df.to_json(json_path, orient='records', date_format='iso', indent=2)
        
        # Print summary to console
        print("\n" + "="*60)
        print("DISCOVERY RESULTS SUMMARY")
        print("="*60)
        print(f"Total files found: {len(files_df)}")
        
        print("\nFiles by stage:")
        stage_counts = files_df['deal_stage'].value_counts().sort_index()
        for stage, count in stage_counts.items():
            print(f"  {stage}: {count} files")
        
        print("\nMost recent files:")
        recent = files_df.nlargest(10, 'last_modified')[['deal_name', 'file_name', 'deal_stage', 'last_modified']]
        print(recent.to_string(index=False))
        
        print(f"\nFull results saved to: {csv_path}")
        
        return files_df
        
    except ConfigurationError as e:
        logger.error(f"Configuration error: {str(e)}")
        print(f"\nConfiguration Error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Discovery failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    files = run_file_discovery()