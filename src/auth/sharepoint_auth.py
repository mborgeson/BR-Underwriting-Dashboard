# src/auth/sharepoint_auth.py
import time
from typing import Dict, Optional
from msal import ConfidentialClientApplication
import requests
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)

class SharePointAuthenticator:
    """Handles Azure AD authentication for SharePoint access via Microsoft Graph API"""
    
    def __init__(self, settings):
        self.settings = settings
        self.app = ConfidentialClientApplication(
            client_id=settings.azure.client_id,
            client_credential=settings.azure.client_secret,
            authority=settings.azure.authority
        )
        self._token_cache = None
        self._token_expiry = None
        
    def get_access_token(self) -> str:
        """Get valid access token, refreshing if necessary"""
        if self._is_token_valid():
            return self._token_cache['access_token']
        
        logger.info("Acquiring new access token")
        token = self._acquire_token()
        self._cache_token(token)
        return token['access_token']
    
    def _acquire_token(self) -> Dict:
        """Acquire new token from Azure AD"""
        try:
            result = self.app.acquire_token_for_client(
                scopes=self.settings.azure.scopes
            )
            
            if "access_token" in result:
                logger.info("Successfully acquired access token")
                return result
            else:
                error_msg = f"Failed to acquire token: {result.get('error_description', 'Unknown error')}"
                logger.error(error_msg)
                raise Exception(error_msg)
                
        except Exception as e:
            logger.error(f"Error acquiring token: {str(e)}")
            raise
    
    def _cache_token(self, token: Dict):
        """Cache token and calculate expiry time"""
        self._token_cache = token
        # Set expiry 5 minutes before actual expiry for safety
        expires_in = token.get('expires_in', 3600) - 300
        self._token_expiry = datetime.now() + timedelta(seconds=expires_in)
        
    def _is_token_valid(self) -> bool:
        """Check if cached token is still valid"""
        if not self._token_cache or not self._token_expiry:
            return False
        return datetime.now() < self._token_expiry
    
    def get_headers(self) -> Dict[str, str]:
        """Get headers with authorization token"""
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    
    def get_site_id(self) -> str:
        """Get SharePoint site ID from Microsoft Graph"""
        if self.settings.sharepoint.graph_site_id:
            return self.settings.sharepoint.graph_site_id
            
        url = f"https://graph.microsoft.com/v1.0/sites/{self.settings.sharepoint.hostname}:/sites/{self.settings.sharepoint.site_name}"
        
        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()
            site_data = response.json()
            site_id = site_data['id']
            
            # Cache the site ID
            self.settings.sharepoint.graph_site_id = site_id
            logger.info(f"Retrieved site ID: {site_id}")
            return site_id
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get site ID: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise