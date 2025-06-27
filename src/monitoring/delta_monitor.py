#!/usr/bin/env python3
"""
Delta Monitor Service

Real-time monitoring of SharePoint files using Graph API Delta Queries.
Detects changes and triggers extractions without polling individual files.
"""

import json
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
import structlog
from pathlib import Path

from ..extraction.graph_api_extractor import GraphAPIFileExtractor
from .alerting import AlertingService

logger = structlog.get_logger().bind(component="DeltaMonitor")

class DeltaMonitorService:
    """Monitors SharePoint for file changes using Delta Queries"""
    
    def __init__(self, client_id: str, client_secret: str, tenant_id: str = "383e5745-a469-4712-aaa9-f7d79c981e10"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        
        # Components
        self.graph_extractor = GraphAPIFileExtractor(client_id, client_secret, tenant_id)
        self.alerting = AlertingService()
        
        # Delta monitoring state
        self.delta_token = None
        self.site_id = None
        self.drive_id = None
        self.is_monitoring = False
        self.monitor_thread = None
        
        # Configuration
        self.check_interval = 30  # seconds
        self.retry_interval = 1800  # 30 minutes for failed states
        
        # File tracking
        self.known_files = {}  # file_id -> file_info
        self.last_check_time = None
        
        # Callbacks
        self.on_file_changed = None
        self.on_file_added = None
        self.on_file_deleted = None
        
        logger.info("delta_monitor_initialized", check_interval=self.check_interval)
    
    def initialize(self, site_url: str = "bandrcapital.sharepoint.com:/sites/BRCapital-Internal", 
                   drive_name: str = "Real Estate") -> bool:
        """Initialize monitoring with SharePoint site/drive info"""
        try:
            logger.info("initializing_delta_monitor", site_url=site_url, drive_name=drive_name)
            
            # Authenticate
            if not self.graph_extractor.authenticate():
                raise Exception("Failed to authenticate")
            
            # Get site and drive info
            site_info = self.graph_extractor.get_site_info(site_url)
            drive_info = self.graph_extractor.get_drive_info(site_info['site_id'], drive_name)
            
            self.site_id = site_info['site_id']
            self.drive_id = drive_info['drive_id']
            
            # Initialize delta token with current state
            self._initialize_delta_baseline()
            
            logger.info("delta_monitor_initialized_successfully", 
                       site_id=self.site_id, drive_id=self.drive_id)
            return True
            
        except Exception as e:
            logger.error("delta_monitor_initialization_failed", error=str(e))
            self.alerting.send_alert(
                "Delta Monitor Initialization Failed", 
                f"Failed to initialize monitoring: {e}",
                "error"
            )
            return False
    
    def _initialize_delta_baseline(self):
        """Get initial delta token and current file state"""
        try:
            logger.info("initializing_delta_baseline")
            
            # Get current state using delta query
            delta_endpoint = f"/sites/{self.site_id}/drives/{self.drive_id}/root/delta"
            response = self.graph_extractor._make_graph_request(delta_endpoint)
            delta_data = response.json()
            
            # Store delta token for future queries
            self.delta_token = delta_data.get('@odata.deltaLink', '').split('token=')[-1]
            
            # Build baseline of current UW model files
            current_files = {}
            for item in delta_data.get('value', []):
                if self._is_uw_model_file(item):
                    file_info = self._build_file_info(item)
                    current_files[item['id']] = file_info
            
            self.known_files = current_files
            self.last_check_time = datetime.now()
            
            logger.info("delta_baseline_established", 
                       uw_files_count=len(current_files),
                       delta_token_length=len(self.delta_token) if self.delta_token else 0)
            
        except Exception as e:
            logger.error("delta_baseline_initialization_failed", error=str(e))
            raise
    
    def set_change_handlers(self, on_file_changed: Callable = None, 
                           on_file_added: Callable = None, 
                           on_file_deleted: Callable = None):
        """Set callback functions for file change events"""
        self.on_file_changed = on_file_changed
        self.on_file_added = on_file_added
        self.on_file_deleted = on_file_deleted
        
        logger.info("change_handlers_set", 
                   has_changed_handler=on_file_changed is not None,
                   has_added_handler=on_file_added is not None,
                   has_deleted_handler=on_file_deleted is not None)
    
    def start_monitoring(self):
        """Start real-time delta monitoring in background thread"""
        if self.is_monitoring:
            logger.warning("monitoring_already_active")
            return
        
        logger.info("starting_delta_monitoring")
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        self.alerting.send_alert(
            "Delta Monitoring Started",
            f"Real-time monitoring started for SharePoint drive. Check interval: {self.check_interval}s",
            "info"
        )
    
    def stop_monitoring(self):
        """Stop delta monitoring"""
        logger.info("stopping_delta_monitoring")
        self.is_monitoring = False
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        
        self.alerting.send_alert(
            "Delta Monitoring Stopped",
            "Real-time monitoring has been stopped",
            "info"
        )
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        consecutive_failures = 0
        last_failure_alert = None
        
        while self.is_monitoring:
            try:
                start_time = datetime.now()
                changes = self._check_for_changes()
                check_duration = (datetime.now() - start_time).total_seconds()
                
                if changes:
                    logger.info("delta_check_completed", 
                               changes_detected=len(changes),
                               check_duration_seconds=round(check_duration, 2))
                    self._process_changes(changes)
                else:
                    logger.debug("delta_check_completed_no_changes",
                                check_duration_seconds=round(check_duration, 2))
                
                # Reset failure counter on success
                consecutive_failures = 0
                last_failure_alert = None
                
                # Wait for next check
                time.sleep(self.check_interval)
                
            except Exception as e:
                consecutive_failures += 1
                error_msg = str(e)
                
                logger.error("delta_check_failed", 
                           error=error_msg, 
                           consecutive_failures=consecutive_failures)
                
                # Send alert on first failure or every 30 minutes
                now = datetime.now()
                if (last_failure_alert is None or 
                    (now - last_failure_alert).total_seconds() >= self.retry_interval):
                    
                    self.alerting.send_alert(
                        f"Delta Monitoring Error (Failure #{consecutive_failures})",
                        f"Delta query failed: {error_msg}\nWill retry every 30 minutes until resolved.",
                        "error"
                    )
                    last_failure_alert = now
                
                # Wait before retry
                time.sleep(self.retry_interval)
    
    def _check_for_changes(self) -> List[Dict[str, Any]]:
        """Check for changes using delta query"""
        if not self.delta_token:
            logger.warning("no_delta_token_reinitializing")
            self._initialize_delta_baseline()
            return []
        
        try:
            # Use stored delta token
            delta_endpoint = f"/sites/{self.site_id}/drives/{self.drive_id}/root/delta"
            params = {'token': self.delta_token}
            
            response = self.graph_extractor._make_graph_request(delta_endpoint, params=params)
            delta_data = response.json()
            
            # Update delta token for next query
            next_delta_link = delta_data.get('@odata.deltaLink', '')
            if next_delta_link:
                self.delta_token = next_delta_link.split('token=')[-1]
            
            # Filter for UW model file changes
            uw_changes = []
            for item in delta_data.get('value', []):
                if self._is_uw_model_file(item) or item['id'] in self.known_files:
                    uw_changes.append(item)
            
            return uw_changes
            
        except Exception as e:
            # Handle delta token invalidation
            if "410" in str(e) or "invalid" in str(e).lower():
                logger.warning("delta_token_invalidated_reinitializing")
                self._initialize_delta_baseline()
                return []
            raise
    
    def _process_changes(self, changes: List[Dict[str, Any]]):
        """Process detected changes and trigger appropriate actions"""
        for item in changes:
            file_id = item['id']
            
            # Check if file was deleted
            if item.get('deleted'):
                if file_id in self.known_files:
                    file_info = self.known_files[file_id]
                    logger.info("file_deleted", file_name=file_info.get('file_name', 'unknown'))
                    
                    if self.on_file_deleted:
                        self.on_file_deleted(file_info)
                    
                    del self.known_files[file_id]
                continue
            
            # Build current file info
            file_info = self._build_file_info(item)
            
            # Check if this is a new file
            if file_id not in self.known_files:
                logger.info("new_file_detected", 
                           file_name=file_info['file_name'],
                           deal_name=file_info['deal_name'])
                
                self.known_files[file_id] = file_info
                
                if self.on_file_added:
                    self.on_file_added(file_info)
            
            # Check if existing file was modified
            else:
                old_info = self.known_files[file_id]
                
                # Compare modification times
                old_modified = old_info.get('last_modified', '')
                new_modified = file_info.get('last_modified', '')
                
                if new_modified != old_modified:
                    logger.info("file_modified", 
                               file_name=file_info['file_name'],
                               deal_name=file_info['deal_name'],
                               old_modified=old_modified,
                               new_modified=new_modified)
                    
                    self.known_files[file_id] = file_info
                    
                    if self.on_file_changed:
                        self.on_file_changed(file_info, old_info)
    
    def _is_uw_model_file(self, item: Dict[str, Any]) -> bool:
        """Check if item is a UW model file"""
        file_name = item.get('name', '').lower()
        return (file_name.endswith('.xlsb') and 
                'uw model vcurrent' in file_name)
    
    def _build_file_info(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Build standardized file info from Graph API item"""
        file_path = item.get('parentReference', {}).get('path', '')
        
        return {
            'file_id': item['id'],
            'file_name': item['name'],
            'file_path': file_path,
            'deal_name': self.graph_extractor._extract_deal_name_from_path(file_path),
            'deal_stage': self.graph_extractor._extract_deal_stage_from_path(file_path),
            'size_mb': round(item.get('size', 0) / (1024 * 1024), 2),
            'last_modified': item.get('lastModifiedDateTime'),
            'created_date': item.get('createdDateTime'),
            'web_url': item.get('webUrl'),
            'site_id': self.site_id,
            'drive_id': self.drive_id,
            'etag': item.get('eTag', '')
        }
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status"""
        return {
            'is_monitoring': self.is_monitoring,
            'check_interval': self.check_interval,
            'files_tracked': len(self.known_files),
            'last_check': self.last_check_time.isoformat() if self.last_check_time else None,
            'has_delta_token': self.delta_token is not None,
            'site_id': self.site_id,
            'drive_id': self.drive_id
        }
    
    def force_refresh(self) -> bool:
        """Force a refresh of the baseline and delta token"""
        try:
            logger.info("forcing_delta_refresh")
            self._initialize_delta_baseline()
            return True
        except Exception as e:
            logger.error("force_refresh_failed", error=str(e))
            return False