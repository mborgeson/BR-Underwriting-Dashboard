#!/usr/bin/env python3
"""
Graph API File Streaming Extractor

Extracts Excel data using Microsoft Graph API file streaming without downloads.
Supports .xlsb files through direct content streaming to memory.
"""

import io
import json
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import structlog
from pathlib import Path

from ..data_extraction.excel_extraction_system import ExcelDataExtractor

logger = structlog.get_logger().bind(component="GraphAPIExtractor")

class GraphAPIFileExtractor:
    """Extracts Excel data via Graph API streaming without downloads"""
    
    def __init__(self, client_id: str, client_secret: str, tenant_id: str = "383e5745-a469-4712-aaa9-f7d79c981e10"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        
        # Authentication
        self.access_token = None
        self.token_expires_at = None
        
        # Graph API endpoints
        self.auth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        self.graph_base = "https://graph.microsoft.com/v1.0"
        
        logger.info("graph_api_extractor_initialized", 
                   client_id=client_id, tenant_id=tenant_id)
    
    def authenticate(self) -> bool:
        """Authenticate with Microsoft Graph API"""
        try:
            logger.info("authenticating_with_graph_api")
            
            # Request access token
            token_data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': 'https://graph.microsoft.com/.default'
            }
            
            response = requests.post(self.auth_url, data=token_data, timeout=30)
            response.raise_for_status()
            
            token_info = response.json()
            self.access_token = token_info['access_token']
            
            # Calculate expiry time (subtract 5 minutes for safety)
            expires_in = token_info.get('expires_in', 3600)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 300)
            
            logger.info("graph_api_authentication_successful", 
                       expires_at=self.token_expires_at.isoformat())
            return True
            
        except Exception as e:
            logger.error("graph_api_authentication_failed", error=str(e))
            return False
    
    def _ensure_authenticated(self) -> bool:
        """Ensure we have a valid access token"""
        if not self.access_token or datetime.now() >= self.token_expires_at:
            logger.info("token_expired_refreshing")
            return self.authenticate()
        return True
    
    def _make_graph_request(self, endpoint: str, method: str = "GET", 
                           stream: bool = False, **kwargs) -> requests.Response:
        """Make authenticated request to Graph API"""
        if not self._ensure_authenticated():
            raise Exception("Failed to authenticate with Graph API")
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        url = f"{self.graph_base}{endpoint}"
        response = requests.request(method, url, headers=headers, stream=stream, **kwargs)
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            logger.warning("rate_limited_waiting", retry_after=retry_after)
            time.sleep(retry_after)
            return self._make_graph_request(endpoint, method, stream, **kwargs)
        
        response.raise_for_status()
        return response
    
    def get_site_info(self, site_url: str = "bandrcapital.sharepoint.com:/sites/BRCapital-Internal") -> Dict[str, str]:
        """Get SharePoint site information"""
        try:
            logger.info("getting_site_info", site_url=site_url)
            
            # Get site by URL - Graph API requires hostname:/sites/sitename format
            site_endpoint = f"/sites/{site_url}"
            response = self._make_graph_request(site_endpoint)
            site_data = response.json()
            
            site_id = site_data['id']
            logger.info("site_info_retrieved", site_id=site_id)
            
            return {
                'site_id': site_id,
                'site_name': site_data.get('displayName', ''),
                'web_url': site_data.get('webUrl', '')
            }
            
        except Exception as e:
            logger.error("get_site_info_failed", error=str(e))
            raise
    
    def get_drive_info(self, site_id: str, drive_name: str = "Real Estate") -> Dict[str, str]:
        """Get drive information by name"""
        try:
            logger.info("getting_drive_info", site_id=site_id, drive_name=drive_name)
            
            # List all drives
            drives_endpoint = f"/sites/{site_id}/drives"
            response = self._make_graph_request(drives_endpoint)
            drives_data = response.json()
            
            # Find drive by name
            for drive in drives_data.get('value', []):
                if drive.get('name') == drive_name:
                    drive_id = drive['id']
                    logger.info("drive_found", drive_id=drive_id, drive_name=drive_name)
                    
                    return {
                        'drive_id': drive_id,
                        'drive_name': drive['name'],
                        'drive_type': drive.get('driveType', '')
                    }
            
            raise Exception(f"Drive '{drive_name}' not found")
            
        except Exception as e:
            logger.error("get_drive_info_failed", error=str(e))
            raise
    
    def discover_uw_files(self, site_id: str, drive_id: str) -> List[Dict[str, Any]]:
        """Discover UW model files in the drive"""
        try:
            logger.info("discovering_uw_files", site_id=site_id, drive_id=drive_id)
            
            # Search for UW model files
            search_query = "UW Model vCurrent.xlsb"
            search_endpoint = f"/sites/{site_id}/drives/{drive_id}/root/search(q='{search_query}')"
            
            response = self._make_graph_request(search_endpoint)
            search_results = response.json()
            
            discovered_files = []
            
            for item in search_results.get('value', []):
                # Extract deal info from path
                file_path = item.get('parentReference', {}).get('path', '')
                deal_stage = self._extract_deal_stage_from_path(file_path)
                deal_name = self._extract_deal_name_from_path(file_path)
                
                file_info = {
                    'file_id': item['id'],
                    'file_name': item['name'],
                    'file_path': file_path,
                    'deal_name': deal_name,
                    'deal_stage': deal_stage,
                    'size_mb': round(item.get('size', 0) / (1024 * 1024), 2),
                    'last_modified': item.get('lastModifiedDateTime'),
                    'created_date': item.get('createdDateTime'),
                    'web_url': item.get('webUrl'),
                    'site_id': site_id,
                    'drive_id': drive_id,
                    'etag': item.get('eTag', '')
                }
                
                discovered_files.append(file_info)
            
            logger.info("uw_files_discovered", count=len(discovered_files))
            return discovered_files
            
        except Exception as e:
            logger.error("discover_uw_files_failed", error=str(e))
            raise
    
    def stream_file_content(self, site_id: str, drive_id: str, file_id: str) -> io.BytesIO:
        """Stream file content directly to memory"""
        try:
            logger.debug("streaming_file_content", file_id=file_id)
            
            # Get file content
            content_endpoint = f"/sites/{site_id}/drives/{drive_id}/items/{file_id}/content"
            response = self._make_graph_request(content_endpoint, stream=True)
            
            # Stream to memory buffer
            buffer = io.BytesIO()
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    buffer.write(chunk)
            
            buffer.seek(0)
            logger.debug("file_content_streamed", size_bytes=len(buffer.getvalue()))
            
            return buffer
            
        except Exception as e:
            logger.error("stream_file_content_failed", file_id=file_id, error=str(e))
            raise
    
    def extract_from_file_info(self, file_info: Dict[str, Any], 
                              extractor: ExcelDataExtractor) -> Optional[Dict[str, Any]]:
        """Extract data from a file using Graph API streaming"""
        try:
            file_name = file_info['file_name']
            file_id = file_info['file_id']
            
            logger.info("extracting_from_file", file_name=file_name, file_id=file_id)
            
            # Stream file content
            file_buffer = self.stream_file_content(
                file_info['site_id'], 
                file_info['drive_id'], 
                file_id
            )
            
            # Create temporary file for pyxlsb
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.xlsb', delete=True) as temp_file:
                temp_file.write(file_buffer.getvalue())
                temp_file.flush()
                
                # Extract data using existing logic
                extracted_data = extractor.extract_from_file(temp_file.name, None)
                
                # Add Graph API metadata
                extracted_data.update({
                    '_file_id': file_id,
                    '_file_name': file_name,
                    '_file_path': file_info.get('file_path', ''),
                    '_extraction_timestamp': datetime.now().isoformat(),
                    '_deal_name': file_info.get('deal_name'),
                    '_deal_stage': file_info.get('deal_stage'),
                    '_file_modified_date': file_info.get('last_modified'),
                    '_file_size_mb': file_info.get('size_mb'),
                    '_graph_api_etag': file_info.get('etag', ''),
                    '_extraction_method': 'graph_api_streaming'
                })
                
                logger.info("extraction_completed", 
                           file_name=file_name,
                           fields_extracted=len([v for v in extracted_data.values() if v is not None]))
                
                return extracted_data
                
        except Exception as e:
            logger.error("extract_from_file_failed", 
                        file_name=file_info.get('file_name', 'unknown'),
                        error=str(e))
            return None
    
    def _extract_deal_stage_from_path(self, file_path: str) -> str:
        """Extract deal stage from SharePoint file path"""
        path_lower = file_path.lower()
        
        if '0) dead deals' in path_lower:
            return '0) Dead Deals'
        elif '1) initial uw and review' in path_lower:
            return '1) Initial UW and Review'
        elif '2) active uw and review' in path_lower:
            return '2) Active UW and Review'
        elif '3) under contract' in path_lower:
            return '3) Under Contract'
        elif '4) closed deals' in path_lower:
            return '4) Closed Deals'
        elif '5) realized deals' in path_lower:
            return '5) Realized Deals'
        else:
            return 'Unknown Stage'
    
    def _extract_deal_name_from_path(self, file_path: str) -> str:
        """Extract deal name from SharePoint file path"""
        try:
            # Path format: /drives/.../Deals/Stage/DealName/UW Model/filename
            path_parts = file_path.split('/')
            
            # Find the deal name (usually before 'UW Model')
            for i, part in enumerate(path_parts):
                if 'uw model' in part.lower() and i > 0:
                    return path_parts[i-1]
            
            # Fallback: use the part after stage
            for i, part in enumerate(path_parts):
                if any(stage in part.lower() for stage in ['dead deals', 'initial uw', 'active uw', 'under contract', 'closed deals', 'realized']):
                    if i + 1 < len(path_parts):
                        return path_parts[i + 1]
            
            return 'Unknown Deal'
            
        except Exception:
            return 'Unknown Deal'