#!/usr/bin/env python3
"""
Complete Enhanced Real-time Workflow with Dashboard Integration

This is the upgraded orchestrator that:
1. Performs initial extraction of all 41 files
2. Switches to enhanced real-time delta monitoring with criteria filtering
3. Provides real-time dashboard updates via WebSocket
4. Processes changes as they happen with proper validation
5. Loads data directly to expanded database schema

No permanent file downloads - everything streams through memory.
Includes proper criteria filtering (July 15, 2024 cutoff) and dashboard integration.
"""

import os
import sys
import argparse
import signal
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.extraction.graph_api_extractor import GraphAPIFileExtractor
from src.monitoring.enhanced_delta_monitor import EnhancedDeltaMonitorService
from src.monitoring.dashboard_integration import DashboardIntegrationService
from src.monitoring.alerting import AlertingService
from src.data_extraction.excel_extraction_system import CellMappingParser, ExcelDataExtractor
from src.database.expanded_data_loader import ExpandedDataLoader
from src.database.connection import initialize_database, DatabaseConfig
import structlog

# Configure logging
logging = structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger().bind(component="EnhancedRealtimeWorkflow")

class EnhancedRealtimeExtractionWorkflow:
    """Complete enhanced real-time extraction workflow with dashboard integration"""
    
    def __init__(self, client_id: str, client_secret: str, reference_file: str, tenant_id: str = "383e5745-a469-4712-aaa9-f7d79c981e10"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.reference_file = reference_file
        self.tenant_id = tenant_id
        
        # Components
        self.graph_extractor = GraphAPIFileExtractor(client_id, client_secret, tenant_id)
        self.enhanced_monitor = EnhancedDeltaMonitorService(client_id, client_secret, tenant_id)
        self.dashboard_integration = DashboardIntegrationService()
        self.alerting = AlertingService()
        
        # Excel processing
        self.cell_parser = CellMappingParser(reference_file)
        self.mappings = self.cell_parser.load_mappings()
        self.excel_extractor = ExcelDataExtractor(self.mappings)
        
        # Database  
        self.data_loader = ExpandedDataLoader()
        config = DatabaseConfig()
        initialize_database(config)
        
        # State
        self.is_running = False
        self.initial_extraction_complete = False
        
        # Set up dashboard integration
        self.dashboard_integration.set_monitoring_service(self.enhanced_monitor)
        self.enhanced_monitor.set_dashboard_integration(self.dashboard_integration.handle_monitoring_event)
        
        logger.info("enhanced_realtime_workflow_initialized",
                   mappings_count=len(self.mappings),
                   client_id=client_id,
                   dashboard_integration_enabled=True)
    
    def run_initial_extraction(self, site_url: str = "bandrcapital.sharepoint.com:/sites/BRCapital-Internal", 
                              drive_name: str = "Real Estate") -> bool:
        """Perform initial extraction of all UW model files with enhanced criteria"""
        try:
            logger.info("starting_enhanced_initial_extraction", site_url=site_url, drive_name=drive_name)
            
            self.alerting.send_alert(
                "Enhanced Initial Extraction Started",
                "Beginning initial extraction with enhanced criteria filtering (July 15, 2024 cutoff)",
                "info"
            )
            
            # Notify dashboard of extraction start
            self.dashboard_integration.send_status_update({
                'phase': 'initial_extraction',
                'status': 'starting',
                'criteria_cutoff_date': '2024-07-15',
                'total_fields_per_file': len(self.mappings)
            })
            
            # Authenticate and get site/drive info
            if not self.graph_extractor.authenticate():
                raise Exception("Authentication failed")
            
            site_info = self.graph_extractor.get_site_info(site_url)
            drive_info = self.graph_extractor.get_drive_info(site_info['site_id'], drive_name)
            
            # Discover all UW files
            discovered_files = self.graph_extractor.discover_uw_files(
                site_info['site_id'], 
                drive_info['drive_id']
            )
            
            logger.info("files_discovered_for_enhanced_extraction", count=len(discovered_files))
            
            # Show breakdown by stage
            from collections import Counter
            stages = Counter(f['deal_stage'] for f in discovered_files)
            
            stage_summary = "Files by stage:\n"
            for stage, count in stages.most_common():
                stage_summary += f"  • {stage}: {count} files\n"
            
            self.alerting.send_alert(
                f"Discovered {len(discovered_files)} Files",
                stage_summary.strip(),
                "info"
            )
            
            # Dashboard update with discovery results
            self.dashboard_integration.send_status_update({
                'phase': 'initial_extraction',
                'status': 'files_discovered',
                'total_files': len(discovered_files),
                'files_by_stage': dict(stages)
            })
            
            # Extract data from each file
            successful = 0
            failed = 0
            
            for i, file_info in enumerate(discovered_files, 1):
                file_name = file_info['file_name']
                deal_name = file_info['deal_name']
                
                logger.info("processing_file_enhanced_extraction",
                           file_num=i,
                           total_files=len(discovered_files),
                           file_name=file_name,
                           deal_name=deal_name)
                
                try:
                    start_time = datetime.now()
                    
                    # Extract data using Graph API streaming
                    extracted_data = self.graph_extractor.extract_from_file_info(
                        file_info, self.excel_extractor
                    )
                    
                    if extracted_data:
                        # Load to expanded database
                        extraction_id = self.data_loader.load_complete_extraction_data(
                            extraction_data=extracted_data,
                            deal_stage=file_info['deal_stage'],
                            metadata={
                                'total_fields': len(self.mappings),
                                'successful': len([v for v in extracted_data.values() if v is not None]),
                                'duration_seconds': (datetime.now() - start_time).total_seconds(),
                                'extraction_method': 'enhanced_graph_api_streaming'
                            }
                        )
                        
                        if extraction_id:
                            successful += 1
                            duration = (datetime.now() - start_time).total_seconds()
                            
                            logger.info("enhanced_file_extraction_successful",
                                       file_name=file_name,
                                       extraction_id=extraction_id,
                                       duration_seconds=round(duration, 2))
                            
                            # Dashboard update for successful extraction
                            self.dashboard_integration.send_extraction_update(
                                file_info, {
                                    'status': 'success',
                                    'extraction_id': extraction_id,
                                    'duration_seconds': duration,
                                    'successful_extractions': successful,
                                    'progress': f"{i}/{len(discovered_files)}"
                                }
                            )
                            
                            self.alerting.send_extraction_notification(
                                file_info, 'success', duration
                            )
                        else:
                            failed += 1
                            logger.error("enhanced_database_load_failed", file_name=file_name)
                    else:
                        failed += 1
                        logger.error("enhanced_extraction_failed", file_name=file_name)
                
                except Exception as e:
                    failed += 1
                    logger.error("enhanced_file_processing_failed", 
                                file_name=file_name, error=str(e))
                    
                    # Dashboard error notification
                    self.dashboard_integration.send_error_notification(
                        "extraction_error", str(e), {'file_name': file_name, 'deal_name': deal_name}
                    )
                    
                    self.alerting.send_extraction_notification(
                        file_info, str(e)
                    )
                
                # Progress update every 5 files for enhanced monitoring
                if i % 5 == 0:
                    progress_msg = f"Progress: {i}/{len(discovered_files)} files processed"
                    logger.info("enhanced_extraction_progress", 
                               processed=i, 
                               total=len(discovered_files),
                               successful=successful,
                               failed=failed)
                    
                    # Dashboard progress update
                    self.dashboard_integration.send_status_update({
                        'phase': 'initial_extraction',
                        'status': 'in_progress',
                        'processed': i,
                        'total': len(discovered_files),
                        'successful': successful,
                        'failed': failed,
                        'progress_percent': round((i / len(discovered_files)) * 100, 1)
                    })
                    
                    self.alerting.send_alert(
                        "Enhanced Extraction Progress",
                        f"{progress_msg}\nSuccessful: {successful}, Failed: {failed}",
                        "info"
                    )
            
            # Final summary
            success_rate = (successful / len(discovered_files)) * 100 if discovered_files else 0
            
            logger.info("enhanced_initial_extraction_completed",
                       total_files=len(discovered_files),
                       successful=successful,
                       failed=failed,
                       success_rate=round(success_rate, 1))
            
            # Dashboard completion update
            self.dashboard_integration.send_status_update({
                'phase': 'initial_extraction',
                'status': 'completed',
                'total_files': len(discovered_files),
                'successful': successful,
                'failed': failed,
                'success_rate': success_rate
            })
            
            self.alerting.send_alert(
                "Enhanced Initial Extraction Completed",
                f"Processed {len(discovered_files)} files\n"
                f"✅ Successful: {successful}\n"
                f"❌ Failed: {failed}\n"
                f"Success Rate: {success_rate:.1f}%",
                "success" if success_rate > 90 else "warning"
            )
            
            self.initial_extraction_complete = True
            return True
            
        except Exception as e:
            logger.error("enhanced_initial_extraction_failed", error=str(e))
            
            # Dashboard error notification
            self.dashboard_integration.send_error_notification(
                "initial_extraction_error", str(e)
            )
            
            self.alerting.send_alert(
                "Enhanced Initial Extraction Failed",
                f"Initial extraction failed: {e}",
                "error"
            )
            return False
    
    def start_enhanced_realtime_monitoring(self, site_url: str = "bandrcapital.sharepoint.com:/sites/BRCapital-Internal", 
                                         drive_name: str = "Real Estate") -> bool:
        """Start enhanced real-time delta monitoring with dashboard integration"""
        try:
            logger.info("starting_enhanced_realtime_monitoring")
            
            # Start dashboard integration server
            self.dashboard_integration.start_integration_server()
            
            # Initialize enhanced delta monitor
            if not self.enhanced_monitor.initialize(site_url, drive_name):
                raise Exception("Failed to initialize enhanced delta monitor")
            
            # Set up enhanced change handlers
            self.enhanced_monitor.set_change_handlers(
                on_file_changed=self._handle_enhanced_file_changed,
                on_file_added=self._handle_enhanced_file_added,
                on_file_deleted=self._handle_enhanced_file_deleted
            )
            
            # Start enhanced monitoring
            self.enhanced_monitor.start_monitoring()
            self.is_running = True
            
            # Dashboard status update
            self.dashboard_integration.send_status_update({
                'phase': 'realtime_monitoring',
                'status': 'active',
                'criteria_filtering': True,
                'cutoff_date': '2024-07-15',
                'files_tracked': len(self.enhanced_monitor.known_files),
                'check_interval_seconds': self.enhanced_monitor.check_interval
            })
            
            logger.info("enhanced_realtime_monitoring_started")
            return True
            
        except Exception as e:
            logger.error("enhanced_realtime_monitoring_start_failed", error=str(e))
            
            # Dashboard error notification
            self.dashboard_integration.send_error_notification(
                "monitoring_start_error", str(e)
            )
            
            self.alerting.send_alert(
                "Enhanced Real-time Monitoring Failed to Start",
                f"Failed to start enhanced monitoring: {e}",
                "error"
            )
            return False
    
    def _handle_enhanced_file_changed(self, file_info: Dict[str, Any]):
        """Handle when an existing UW model file is modified (enhanced version)"""
        file_name = file_info['file_name']
        deal_name = file_info['deal_name']
        
        logger.info("enhanced_file_change_detected", 
                   file_name=file_name, 
                   deal_name=deal_name,
                   last_modified=file_info.get('last_modified'))
        
        self.alerting.send_alert(
            "Enhanced File Change Detected",
            f"UW model file modified: {file_name} ({deal_name})\nTriggering enhanced re-extraction...",
            "info"
        )
        
        # Dashboard change notification
        self.dashboard_integration.send_status_update({
            'event_type': 'file_modified',
            'file_name': file_name,
            'deal_name': deal_name,
            'trigger_reason': 'File Modified'
        })
        
        # Trigger enhanced re-extraction
        self._extract_single_file_enhanced(file_info, "Enhanced File Modified")
    
    def _handle_enhanced_file_added(self, file_info: Dict[str, Any]):
        """Handle when a new UW model file is added (enhanced version)"""
        file_name = file_info['file_name']
        deal_name = file_info['deal_name']
        
        logger.info("enhanced_new_file_detected", 
                   file_name=file_name, 
                   deal_name=deal_name,
                   meets_criteria=file_info.get('criteria_compliant', False))
        
        self.alerting.send_alert(
            "Enhanced New UW Model File Detected",
            f"New criteria-compliant file added: {file_name} ({deal_name})\nTriggering enhanced extraction...",
            "info"
        )
        
        # Dashboard new file notification
        self.dashboard_integration.send_status_update({
            'event_type': 'file_added',
            'file_name': file_name,
            'deal_name': deal_name,
            'criteria_compliant': file_info.get('criteria_compliant', False)
        })
        
        # Extract data from new file
        self._extract_single_file_enhanced(file_info, "Enhanced New File")
    
    def _handle_enhanced_file_deleted(self, file_info: Dict[str, Any]):
        """Handle when a UW model file is deleted (enhanced version)"""
        file_name = file_info['file_name']
        deal_name = file_info['deal_name']
        
        logger.info("enhanced_file_deleted", 
                   file_name=file_name, 
                   deal_name=deal_name)
        
        # Dashboard deletion notification
        self.dashboard_integration.send_status_update({
            'event_type': 'file_deleted',
            'file_name': file_name,
            'deal_name': deal_name
        })
        
        self.alerting.send_alert(
            "Enhanced UW Model File Deleted",
            f"Criteria-compliant file deleted: {file_name} ({deal_name})\nData remains in database for historical tracking.",
            "warning"
        )
    
    def _extract_single_file_enhanced(self, file_info: Dict[str, Any], trigger_reason: str):
        """Extract data from a single file with enhanced processing"""
        file_name = file_info['file_name']
        
        try:
            start_time = datetime.now()
            
            # Extract data
            extracted_data = self.graph_extractor.extract_from_file_info(
                file_info, self.excel_extractor
            )
            
            if extracted_data:
                # Load to expanded database
                extraction_id = self.data_loader.load_complete_extraction_data(
                    extraction_data=extracted_data,
                    deal_stage=file_info['deal_stage'],
                    metadata={
                        'total_fields': len(self.mappings),
                        'successful': len([v for v in extracted_data.values() if v is not None]),
                        'duration_seconds': (datetime.now() - start_time).total_seconds(),
                        'extraction_method': 'enhanced_graph_api_streaming_realtime',
                        'trigger_reason': trigger_reason,
                        'criteria_compliant': file_info.get('criteria_compliant', True)
                    }
                )
                
                if extraction_id:
                    duration = (datetime.now() - start_time).total_seconds()
                    
                    logger.info("enhanced_realtime_extraction_successful",
                               file_name=file_name,
                               extraction_id=extraction_id,
                               trigger_reason=trigger_reason,
                               duration_seconds=round(duration, 2))
                    
                    # Dashboard extraction completion
                    self.dashboard_integration.send_extraction_update(
                        file_info, {
                            'status': 'success',
                            'extraction_id': extraction_id,
                            'duration_seconds': duration,
                            'trigger_reason': trigger_reason
                        }
                    )
                    
                    self.alerting.send_extraction_notification(
                        file_info, 'success', duration
                    )
                else:
                    logger.error("enhanced_realtime_database_load_failed", file_name=file_name)
                    
                    # Dashboard error notification
                    self.dashboard_integration.send_error_notification(
                        "database_load_error", "Failed to load extraction data to database", 
                        {'file_name': file_name, 'trigger_reason': trigger_reason}
                    )
            else:
                logger.error("enhanced_realtime_extraction_failed", file_name=file_name)
                
                # Dashboard extraction failure notification
                self.dashboard_integration.send_error_notification(
                    "extraction_error", "Failed to extract data from file", 
                    {'file_name': file_name, 'trigger_reason': trigger_reason}
                )
        
        except Exception as e:
            logger.error("enhanced_realtime_extraction_error", 
                        file_name=file_name, 
                        trigger_reason=trigger_reason,
                        error=str(e))
            
            # Dashboard error notification
            self.dashboard_integration.send_error_notification(
                "extraction_error", str(e), 
                {'file_name': file_name, 'trigger_reason': trigger_reason}
            )
            
            self.alerting.send_extraction_notification(
                file_info, str(e)
            )
    
    def stop(self):
        """Stop the enhanced workflow"""
        logger.info("stopping_enhanced_realtime_workflow")
        
        self.is_running = False
        self.enhanced_monitor.stop_monitoring()
        self.dashboard_integration.stop_integration_server()
        
        self.alerting.send_alert(
            "Enhanced Real-time Workflow Stopped",
            "Enhanced real-time extraction workflow with dashboard integration has been stopped",
            "info"
        )
    
    def get_enhanced_status(self) -> Dict[str, Any]:
        """Get current enhanced workflow status"""
        monitor_status = self.enhanced_monitor.get_enhanced_monitoring_status()
        dashboard_status = self.dashboard_integration.get_integration_status()
        
        return {
            'workflow_running': self.is_running,
            'initial_extraction_complete': self.initial_extraction_complete,
            'enhanced_monitoring': monitor_status,
            'dashboard_integration': dashboard_status,
            'mappings_loaded': len(self.mappings),
            'criteria_filtering_enabled': True,
            'cutoff_date': '2024-07-15',
            'last_status_check': datetime.now().isoformat()
        }


def main():
    """Main entry point for enhanced workflow"""
    parser = argparse.ArgumentParser(description="B&R Enhanced Real-time Extraction Workflow with Dashboard Integration")
    parser.add_argument('--client-id', required=True, help='Azure AD client ID')
    parser.add_argument('--client-secret', required=True, help='Azure AD client secret') 
    parser.add_argument('--reference-file', required=True, help='Path to cell mapping reference file')
    parser.add_argument('--tenant-id', default='383e5745-a469-4712-aaa9-f7d79c981e10', help='Azure AD tenant ID')
    parser.add_argument('--site-url', default='bandrcapital.sharepoint.com:/sites/BRCapital-Internal', help='SharePoint site URL')
    parser.add_argument('--drive-name', default='Real Estate', help='SharePoint drive name')
    parser.add_argument('--skip-initial', action='store_true', help='Skip initial extraction (monitoring only)')
    parser.add_argument('--dashboard-port', type=int, default=8765, help='Dashboard WebSocket port')
    
    args = parser.parse_args()
    
    # Create enhanced workflow
    workflow = EnhancedRealtimeExtractionWorkflow(
        args.client_id,
        args.client_secret, 
        args.reference_file,
        args.tenant_id
    )
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print("\nShutdown signal received...")
        workflow.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        print("🚀 Starting B&R Enhanced Real-time Extraction Workflow")
        print("   ✨ With Dashboard Integration & Criteria Filtering")
        print("=" * 70)
        print(f"📊 Dashboard WebSocket Server: ws://localhost:{args.dashboard_port}")
        print(f"📅 Criteria Filter: Files modified after July 15, 2024")
        print(f"📈 Database: All 1,140 fields supported")
        
        # Phase 1: Initial extraction (if not skipped)
        if not args.skip_initial:
            print("\n📦 Phase 1: Enhanced Initial Extraction")
            success = workflow.run_initial_extraction(args.site_url, args.drive_name)
            
            if not success:
                print("❌ Enhanced initial extraction failed. Check logs for details.")
                return 1
        
        # Phase 2: Enhanced real-time monitoring with dashboard integration
        print("\n🔄 Phase 2: Enhanced Real-time Monitoring with Dashboard Integration")
        success = workflow.start_enhanced_realtime_monitoring(args.site_url, args.drive_name)
        
        if not success:
            print("❌ Enhanced real-time monitoring failed to start. Check logs for details.")
            return 1
        
        print("✅ Enhanced real-time monitoring active!")
        print("   • Enhanced criteria filtering (July 15, 2024 cutoff)")
        print("   • Real-time dashboard updates via WebSocket")
        print("   • Monitoring SharePoint for file changes every 30 seconds")
        print("   • Changes trigger automatic data extraction")
        print("   • All 1,140 fields supported in expanded database")
        print("   • Press Ctrl+C to stop")
        print("\n" + "=" * 70)
        
        # Keep running until interrupted
        while workflow.is_running:
            import time
            time.sleep(10)
            
            # Periodic enhanced status check
            status = workflow.get_enhanced_status()
            if status['enhanced_monitoring']['files_tracked'] > 0:
                print(f"📊 Enhanced Monitoring: {status['enhanced_monitoring']['files_tracked']} criteria-compliant files tracked")
                print(f"🔗 Dashboard Clients: {status['dashboard_integration']['connected_clients']} connected")
                print(f"⏰ Last Check: {status['enhanced_monitoring']['last_check']}")
        
        return 0
        
    except Exception as e:
        logger.error("enhanced_workflow_error", error=str(e))
        print(f"❌ Enhanced workflow error: {e}")
        return 1


if __name__ == "__main__":
    import time
    sys.exit(main())
