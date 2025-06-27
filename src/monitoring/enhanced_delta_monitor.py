#!/usr/bin/env python3
"""
Enhanced Delta Monitor Service with Proper Criteria Filtering and Dashboard Integration

Includes:
1. Original project criteria filtering (July 15, 2024 date filter)
2. Real-time dashboard integration for live updates
3. Enhanced file validation
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

logger = structlog.get_logger().bind(component="EnhancedDeltaMonitor")

class EnhancedDeltaMonitorService:
    """Enhanced monitoring with proper criteria filtering and dashboard integration"""
    
    def __init__(self, client_id: str, client_secret: str, tenant_id: str = "383e5745-a469-4712-aaa9-f7d79c981e10"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        
        # Components
        self.graph_extractor = GraphAPIFileExtractor(client_id, client_secret, tenant_id)
        self.alerting = AlertingService()
        
        # Enhanced filtering criteria (from original project requirements)
        self.cutoff_date = datetime(2024, 7, 15)  # July 15, 2024
        
        # Delta monitoring state
        self.delta_token = None
        self.site_id = None
        self.drive_id = None
        self.is_monitoring = False
        self.monitor_thread = None
        
        # Configuration
        self.check_interval = 30  # seconds
        self.retry_interval = 1800  # 30 minutes for failed states
        
        # File tracking with enhanced validation
        self.known_files = {}  # file_id -> file_info
        self.last_check_time = None
        
        # Callbacks
        self.on_file_changed = None
        self.on_file_added = None
        self.on_file_deleted = None
        
        # Dashboard integration callbacks
        self.on_dashboard_update = None
        self.dashboard_update_queue = []
        
        logger.info("enhanced_delta_monitor_initialized", 
                   check_interval=self.check_interval,
                   cutoff_date=self.cutoff_date.isoformat())
    
    def set_dashboard_integration(self, update_callback: Callable = None):
        """Set dashboard integration callback for real-time updates"""
        self.on_dashboard_update = update_callback
        logger.info("dashboard_integration_enabled", has_callback=update_callback is not None)
    
    def initialize(self, site_url: str = "bandrcapital.sharepoint.com:/sites/BRCapital-Internal", 
                   drive_name: str = "Real Estate") -> bool:
        """Initialize monitoring with enhanced criteria validation"""
        try:
            logger.info("initializing_enhanced_delta_monitor", site_url=site_url, drive_name=drive_name)
            
            # Authenticate
            if not self.graph_extractor.authenticate():
                raise Exception("Failed to authenticate with Graph API")
            
            # Get site and drive info
            site_info = self.graph_extractor.get_site_info(site_url)
            drive_info = self.graph_extractor.get_drive_info(site_info['site_id'], drive_name)
            
            self.site_id = site_info['site_id']
            self.drive_id = drive_info['drive_id']
            
            # Initialize delta baseline with enhanced filtering
            self._initialize_enhanced_delta_baseline()
            
            logger.info("enhanced_delta_monitor_initialized_successfully",
                       site_id=self.site_id,
                       drive_id=self.drive_id,
                       baseline_files=len(self.known_files))
            
            return True
            
        except Exception as e:
            logger.error("enhanced_delta_monitor_initialization_failed", error=str(e))
            self.alerting.send_alert(
                "Enhanced Delta Monitor Initialization Failed", 
                f"Failed to initialize enhanced monitoring: {e}",
                "error"
            )
            return False
    
    def _initialize_enhanced_delta_baseline(self):
        """Get initial delta token and current file state with enhanced filtering"""
        try:
            logger.info("initializing_enhanced_delta_baseline")
            
            # Get current state using delta query
            delta_endpoint = f"/sites/{self.site_id}/drives/{self.drive_id}/root/delta"
            response = self.graph_extractor._make_graph_request(delta_endpoint)
            delta_data = response.json()
            
            # Store delta token for future queries
            self.delta_token = delta_data.get('@odata.deltaLink', '').split('token=')[-1]
            
            # Build baseline of current UW model files with enhanced criteria
            current_files = {}
            total_files_found = 0
            criteria_compliant_files = 0
            
            for item in delta_data.get('value', []):
                total_files_found += 1
                
                if self._meets_all_criteria(item):
                    criteria_compliant_files += 1
                    file_info = self._build_enhanced_file_info(item)
                    current_files[item['id']] = file_info
            
            self.known_files = current_files
            self.last_check_time = datetime.now()
            
            logger.info("enhanced_delta_baseline_established", 
                       total_files_scanned=total_files_found,
                       criteria_compliant_files=criteria_compliant_files,
                       uw_files_tracking=len(current_files),
                       delta_token_length=len(self.delta_token) if self.delta_token else 0)
            
            # Send enhanced baseline summary
            self.alerting.send_alert(
                "Enhanced Monitoring Baseline Established",
                f"Scanned {total_files_found} total files\\n"
                f"Found {criteria_compliant_files} files meeting all criteria\\n"
                f"Tracking {len(current_files)} UW model files for changes\\n"
                f"Cutoff date: {self.cutoff_date.strftime('%B %d, %Y')}",
                "info"
            )
            
        except Exception as e:
            logger.error("enhanced_delta_baseline_initialization_failed", error=str(e))
            raise
    
    def _meets_all_criteria(self, item: Dict[str, Any]) -> bool:
        """Enhanced criteria validation based on original project requirements"""
        
        # 1. Basic UW model file check
        if not self._is_uw_model_file(item):
            return False
        
        # 2. Date filter - modified after July 15, 2024
        if not self._meets_date_criteria(item):
            return False
        
        # 3. Location filter - must be in Deals folder structure
        if not self._meets_location_criteria(item):
            return False
        
        # 4. Deal stage filter - all stages allowed (Dead Deals, Initial UW, Active UW, etc.)
        # This is implicitly handled by location criteria
        
        return True
    
    def _is_uw_model_file(self, item: Dict[str, Any]) -> bool:
        """Check if item is a UW model file"""
        file_name = item.get('name', '').lower()
        return (file_name.endswith('.xlsb') and 
                'uw model vcurrent' in file_name)
    
    def _meets_date_criteria(self, item: Dict[str, Any]) -> bool:
        """Check if file was modified after July 15, 2024"""
        try:
            last_modified_str = item.get('lastModifiedDateTime', '')
            if not last_modified_str:
                return False
            
            # Parse the ISO datetime string
            last_modified = datetime.fromisoformat(last_modified_str.replace('Z', '+00:00'))
            
            # Convert to timezone-naive for comparison
            last_modified_naive = last_modified.replace(tzinfo=None)
            
            return last_modified_naive >= self.cutoff_date
            
        except Exception as e:
            logger.warning("date_criteria_check_failed", 
                          file_name=item.get('name', 'unknown'),
                          error=str(e))
            return False
    
    def _meets_location_criteria(self, item: Dict[str, Any]) -> bool:
        """Check if file is in proper Deals folder structure"""
        try:
            file_path = item.get('parentReference', {}).get('path', '').lower()
            
            # Must be in Real Estate drive and Deals folder
            return ('real estate' in file_path and 'deals' in file_path)
            
        except Exception:
            return False
    
    def _build_enhanced_file_info(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Build enhanced file info with validation status"""
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
            'etag': item.get('eTag', ''),
            
            # Enhanced validation flags
            'meets_date_criteria': self._meets_date_criteria(item),
            'meets_location_criteria': self._meets_location_criteria(item),
            'criteria_compliant': True  # Only compliant files are tracked
        }
    
    def set_change_handlers(self, on_file_changed: Callable = None, 
                           on_file_added: Callable = None, 
                           on_file_deleted: Callable = None):
        """Set callback functions for file change events"""
        self.on_file_changed = on_file_changed
        self.on_file_added = on_file_added
        self.on_file_deleted = on_file_deleted
        
        logger.info("enhanced_change_handlers_set", 
                   has_changed_handler=on_file_changed is not None,
                   has_added_handler=on_file_added is not None,
                   has_deleted_handler=on_file_deleted is not None)
    
    def start_monitoring(self):
        """Start enhanced real-time delta monitoring"""
        if self.is_monitoring:
            logger.warning("enhanced_monitoring_already_active")
            return
        
        logger.info("starting_enhanced_delta_monitoring")
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._enhanced_monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        self.alerting.send_alert(
            "Enhanced Delta Monitoring Started",
            f"Real-time monitoring started with enhanced criteria filtering\\n"
            f"• Check interval: {self.check_interval}s\\n"
            f"• Date filter: Modified after {self.cutoff_date.strftime('%B %d, %Y')}\\n"
            f"• Tracking {len(self.known_files)} compliant files\\n"
            f"• Dashboard integration: {'Enabled' if self.on_dashboard_update else 'Disabled'}",
            "info"
        )
    
    def _enhanced_monitor_loop(self):
        """Enhanced monitoring loop with dashboard integration"""
        consecutive_failures = 0
        last_failure_alert = None
        
        while self.is_monitoring:
            try:
                start_time = datetime.now()
                changes = self._check_for_enhanced_changes()
                check_duration = (datetime.now() - start_time).total_seconds()
                
                if changes:
                    logger.info("enhanced_delta_check_completed", 
                               changes_detected=len(changes),
                               check_duration_seconds=round(check_duration, 2))
                    self._process_enhanced_changes(changes)
                    
                    # Dashboard integration - notify of changes
                    if self.on_dashboard_update:
                        self._notify_dashboard_changes(changes)
                else:
                    logger.debug("enhanced_delta_check_completed_no_changes",
                                check_duration_seconds=round(check_duration, 2))
                
                # Reset failure counter on success
                consecutive_failures = 0
                last_failure_alert = None
                
                # Wait for next check
                time.sleep(self.check_interval)
                
            except Exception as e:
                consecutive_failures += 1
                error_msg = str(e)
                
                logger.error("enhanced_delta_check_failed", 
                           error=error_msg, 
                           consecutive_failures=consecutive_failures)
                
                # Send alert on first failure or every 30 minutes
                now = datetime.now()
                if (last_failure_alert is None or 
                    (now - last_failure_alert).total_seconds() >= self.retry_interval):
                    
                    self.alerting.send_alert(
                        f"Enhanced Delta Monitoring Error (Failure #{consecutive_failures})",
                        f"Enhanced delta query failed: {error_msg}\\nWill retry every 30 minutes until resolved.",
                        "error"
                    )
                    last_failure_alert = now
                
                # Wait before retry
                time.sleep(min(30, consecutive_failures * 5))
    
    def _check_for_enhanced_changes(self) -> List[Dict[str, Any]]:
        """Check for changes using enhanced criteria filtering"""
        try:
            # Use existing delta token to get only changes since last check
            delta_endpoint = f"/sites/{self.site_id}/drives/{self.drive_id}/root/delta"
            params = {'token': self.delta_token}
            response = self.graph_extractor._make_graph_request(delta_endpoint, params=params)
            
            delta_data = response.json()
            
            # Update delta token for next check
            new_delta_link = delta_data.get('@odata.deltaLink', '')
            if new_delta_link:
                self.delta_token = new_delta_link.split('token=')[-1]
            
            # Filter changes to only include criteria-compliant files or known files
            uw_changes = []
            for item in delta_data.get('value', []):
                # Include if: 1) meets all criteria, or 2) is a known file (for deletion tracking)
                if self._meets_all_criteria(item) or item['id'] in self.known_files:
                    uw_changes.append(item)
            
            self.last_check_time = datetime.now()
            return uw_changes
            
        except Exception as e:
            # Handle delta token invalidation
            if "410" in str(e) or "invalid" in str(e).lower():
                logger.warning("enhanced_delta_token_invalidated_reinitializing")
                self._initialize_enhanced_delta_baseline()
                return []
            raise
    
    def _process_enhanced_changes(self, changes: List[Dict[str, Any]]):
        """Process detected changes with enhanced validation"""
        for item in changes:
            file_id = item['id']
            
            # Check if file was deleted
            if item.get('deleted'):
                if file_id in self.known_files:
                    file_info = self.known_files[file_id]
                    logger.info("criteria_compliant_file_deleted", 
                               file_name=file_info.get('file_name', 'unknown'),
                               deal_name=file_info.get('deal_name', 'unknown'))
                    
                    if self.on_file_deleted:
                        self.on_file_deleted(file_info)
                    
                    del self.known_files[file_id]
                continue
            
            # Build current file info with enhanced validation
            file_info = self._build_enhanced_file_info(item)
            
            # Check if this is a new criteria-compliant file
            if file_id not in self.known_files:
                logger.info("new_criteria_compliant_file_detected", 
                           file_name=file_info['file_name'],
                           deal_name=file_info['deal_name'],
                           last_modified=file_info['last_modified'])
                
                self.known_files[file_id] = file_info
                
                if self.on_file_added:
                    self.on_file_added(file_info)
                    
            else:
                # File was modified
                old_file_info = self.known_files[file_id]
                
                # Check if the modification is significant (etag changed)
                if old_file_info.get('etag') != file_info.get('etag'):
                    logger.info("criteria_compliant_file_modified", 
                               file_name=file_info['file_name'],
                               deal_name=file_info['deal_name'],
                               old_modified=old_file_info.get('last_modified'),
                               new_modified=file_info['last_modified'])
                    
                    self.known_files[file_id] = file_info
                    
                    if self.on_file_changed:
                        self.on_file_changed(file_info)
    
    def _notify_dashboard_changes(self, changes: List[Dict[str, Any]]):
        """Notify dashboard of real-time changes"""
        if not self.on_dashboard_update:
            return
        
        try:
            dashboard_event = {
                'event_type': 'file_changes_detected',
                'timestamp': datetime.now().isoformat(),
                'changes_count': len(changes),
                'changes': [
                    {
                        'file_id': item['id'],
                        'file_name': item.get('name'),
                        'change_type': 'deleted' if item.get('deleted') else 'modified',
                        'deal_name': self.graph_extractor._extract_deal_name_from_path(
                            item.get('parentReference', {}).get('path', '')
                        )
                    }
                    for item in changes
                ],
                'total_files_tracked': len(self.known_files)
            }
            
            # Queue dashboard update
            self.dashboard_update_queue.append(dashboard_event)
            
            # Call dashboard update callback
            self.on_dashboard_update(dashboard_event)
            
            logger.info("dashboard_notified_of_changes", 
                       changes_count=len(changes),
                       total_tracked=len(self.known_files))
            
        except Exception as e:
            logger.error("dashboard_notification_failed", error=str(e))
    
    def get_enhanced_monitoring_status(self) -> Dict[str, Any]:
        """Get enhanced monitoring status with criteria compliance info"""
        return {
            'is_monitoring': self.is_monitoring,
            'check_interval': self.check_interval,
            'files_tracked': len(self.known_files),
            'criteria_cutoff_date': self.cutoff_date.isoformat(),
            'last_check': self.last_check_time.isoformat() if self.last_check_time else None,
            'has_delta_token': self.delta_token is not None,
            'site_id': self.site_id,
            'drive_id': self.drive_id,
            'dashboard_integration_enabled': self.on_dashboard_update is not None,
            'dashboard_updates_queued': len(self.dashboard_update_queue)
        }
    
    def get_dashboard_update_queue(self) -> List[Dict[str, Any]]:
        """Get queued dashboard updates and clear the queue"""
        updates = self.dashboard_update_queue.copy()
        self.dashboard_update_queue.clear()
        return updates
    
    def stop_monitoring(self):
        """Stop enhanced delta monitoring"""
        logger.info("stopping_enhanced_delta_monitoring")
        self.is_monitoring = False
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        
        self.alerting.send_alert(
            "Enhanced Delta Monitoring Stopped",
            "Enhanced real-time monitoring has been stopped",
            "info"
        )
    
    def force_refresh(self) -> bool:
        """Force a refresh of the enhanced baseline and delta token"""
        try:
            logger.info("forcing_enhanced_delta_refresh")
            self._initialize_enhanced_delta_baseline()
            return True
        except Exception as e:
            logger.error("enhanced_force_refresh_failed", error=str(e))
            return False