# src/config/settings.py
import os
import sys
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Find the project root directory
def find_project_root():
    """Find the project root by looking for .env file"""
    current = Path(__file__).resolve()
    
    # Try multiple potential locations
    for parent in current.parents:
        if (parent / '.env').exists():
            return parent
        if (parent / 'src').exists() and (parent / '.env.template').exists():
            return parent
    
    # Default to two levels up from this file
    return current.parent.parent

# Load environment variables with explicit path
PROJECT_ROOT = find_project_root()
ENV_PATH = PROJECT_ROOT / '.env'

# Try to load .env file
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
    print(f"Loaded .env from: {ENV_PATH}")
else:
    print(f"WARNING: .env file not found at {ENV_PATH}")
    print(f"Please create it from .env.template")

@dataclass
class AzureConfig:
    """Azure AD configuration settings"""
    client_id: str
    client_secret: str
    tenant_id: str
    authority: str
    scopes: List[str]

@dataclass
class SharePointConfig:
    """SharePoint configuration settings"""
    site_name: str
    hostname: str
    deals_folder: str
    site_url: str
    graph_site_id: Optional[str] = None

@dataclass
class FileFilterConfig:
    """File filtering criteria configuration"""
    include_pattern: str
    exclude_patterns: List[str]
    file_extensions: List[str]
    cutoff_date: datetime

class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing"""
    pass

class Settings:
    """Central configuration management"""
    
    def __init__(self):
        # Validate required environment variables
        self._validate_env_vars()
        
        # Azure AD Configuration
        self.azure = AzureConfig(
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            authority=f"https://login.microsoftonline.com/{os.getenv('AZURE_TENANT_ID')}",
            scopes=["https://graph.microsoft.com/.default"]
        )
        
        # SharePoint Configuration
        self.sharepoint = SharePointConfig(
            site_name=os.getenv("SHAREPOINT_SITE", "BRCapital-Internal"),
            hostname=os.getenv("SHAREPOINT_HOSTNAME", "bandrcapital.sharepoint.com"),
            deals_folder=os.getenv("DEALS_FOLDER", "Real Estate/Deals"),
            site_url=f"https://{os.getenv('SHAREPOINT_HOSTNAME', 'bandrcapital.sharepoint.com')}/sites/{os.getenv('SHAREPOINT_SITE', 'BRCapital-Internal')}"
        )
        
        # File Filter Configuration
        self.file_filter = FileFilterConfig(
            include_pattern=os.getenv("FILE_PATTERN", "UW Model vCurrent"),
            exclude_patterns=os.getenv("EXCLUDE_PATTERNS", "Speedboat,vOld").split(","),
            file_extensions=os.getenv("FILE_EXTENSIONS", ".xlsb,.xlsm").split(","),
            cutoff_date=datetime.strptime(
                os.getenv("CUTOFF_DATE", "2024-07-15"), 
                "%Y-%m-%d"
            )
        )
        
        # Deal Stage Folders
        self.deal_stages = [
            "0) Dead Deals",
            "1) Initial UW and Review",
            "2) Active UW and Review",
            "3) Deals Under Contract",
            "4) Closed Deals",
            "5) Realized Deals"
        ]
    
    def _validate_env_vars(self):
        """Validate that required environment variables are set"""
        required_vars = [
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET", 
            "AZURE_TENANT_ID"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            error_msg = f"Missing required environment variables: {', '.join(missing_vars)}\n"
            error_msg += f"Please ensure .env file exists at: {ENV_PATH}\n"
            error_msg += "You can create it by copying .env.template and filling in the values."
            raise ConfigurationError(error_msg)

# Create settings instance
try:
    settings = Settings()
except ConfigurationError as e:
    print(f"\nCONFIGURATION ERROR:\n{str(e)}")
    sys.exit(1)