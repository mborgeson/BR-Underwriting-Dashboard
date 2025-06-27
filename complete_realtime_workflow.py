#!/usr/bin/env python3
"""
Complete Real-time Workflow with Graph API Streaming

This is the main orchestrator that:
1. Performs initial extraction of all 41 files
2. Switches to real-time delta monitoring 
3. Processes changes as they happen
4. Loads data directly to database

No permanent file downloads - everything streams through memory.
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
from src.monitoring.delta_monitor import DeltaMonitorService
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

logger = structlog.get_logger().bind(component="RealtimeWorkflow")

class RealtimeExtractionWorkflow:
    """Complete real-time extraction workflow orchestrator"""
    
    def __init__(self, client_id: str, client_secret: str, reference_file: str, tenant_id: str = "383e5745-a469-4712-aaa9-f7d79c981e10"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.reference_file = reference_file
        self.tenant_id = tenant_id
        
        # Components
        self.graph_extractor = GraphAPIFileExtractor(client_id, client_secret, tenant_id)
        self.delta_monitor = DeltaMonitorService(client_id, client_secret, tenant_id)
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
        
        logger.info("realtime_workflow_initialized",
                   mappings_count=len(self.mappings),
                   client_id=client_id)
    
    def run_initial_extraction(self, site_url: str = "bandrcapital.sharepoint.com:/sites/BRCapital-Internal", 
                              drive_name: str = "Real Estate") -> bool:
        """Perform initial extraction of all UW model files"""
        try:
            logger.info("starting_initial_extraction", site_url=site_url, drive_name=drive_name)
            
            self.alerting.send_alert(
                "Initial Extraction Started",
                "Beginning initial extraction of all UW model files from SharePoint",
                "info"
            )
            
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
            
            logger.info("files_discovered_for_initial_extraction", count=len(discovered_files))
            
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
            
            # Extract data from each file
            successful = 0
            failed = 0
            
            for i, file_info in enumerate(discovered_files, 1):
                file_name = file_info['file_name']
                deal_name = file_info['deal_name']
                
                logger.info("processing_file_initial_extraction",
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
                        # Load to database
                        extraction_id = self.data_loader.load_complete_extraction_data(
                            extraction_data=extracted_data,
                            deal_stage=file_info['deal_stage'],
                            metadata={
                                'total_fields': len(self.mappings),
                                'successful': len([v for v in extracted_data.values() if v is not None]),
                                'duration_seconds': (datetime.now() - start_time).total_seconds(),
                                'extraction_method': 'graph_api_streaming'
                            }
                        )
                        
                        if extraction_id:
                            successful += 1
                            duration = (datetime.now() - start_time).total_seconds()
                            
                            logger.info("file_extraction_successful",
                                       file_name=file_name,
                                       extraction_id=extraction_id,
                                       duration_seconds=round(duration, 2))
                            
                            self.alerting.send_extraction_notification(
                                file_info, 'success', duration
                            )
                        else:
                            failed += 1
                            logger.error("database_load_failed", file_name=file_name)
                    else:
                        failed += 1
                        logger.error("extraction_failed", file_name=file_name)
                
                except Exception as e:
                    failed += 1
                    logger.error("file_processing_failed", 
                                file_name=file_name, error=str(e))
                    
                    self.alerting.send_extraction_notification(
                        file_info, str(e)
                    )
                
                # Progress update every 10 files
                if i % 10 == 0:
                    progress_msg = f"Progress: {i}/{len(discovered_files)} files processed"
                    logger.info("extraction_progress", 
                               processed=i, 
                               total=len(discovered_files),
                               successful=successful,
                               failed=failed)
                    
                    self.alerting.send_alert(
                        "Extraction Progress",
                        f"{progress_msg}\nSuccessful: {successful}, Failed: {failed}",
                        "info"
                    )
            
            # Final summary
            success_rate = (successful / len(discovered_files)) * 100 if discovered_files else 0
            
            logger.info("initial_extraction_completed",
                       total_files=len(discovered_files),
                       successful=successful,
                       failed=failed,
                       success_rate=round(success_rate, 1))
            
            self.alerting.send_alert(
                "Initial Extraction Completed",
                f"Processed {len(discovered_files)} files\n"
                f"✅ Successful: {successful}\n"
                f"❌ Failed: {failed}\n"
                f"Success Rate: {success_rate:.1f}%",
                "success" if success_rate > 90 else "warning"
            )
            
            self.initial_extraction_complete = True
            return True
            
        except Exception as e:
            logger.error("initial_extraction_failed", error=str(e))
            self.alerting.send_alert(
                "Initial Extraction Failed",
                f"Initial extraction failed: {e}",
                "error"
            )
            return False
    
    def start_realtime_monitoring(self, site_url: str = "bandrcapital.sharepoint.com/sites/BRCapital-Internal", 
                                 drive_name: str = "Real Estate") -> bool:
        """Start real-time delta monitoring"""
        try:
            logger.info("starting_realtime_monitoring")
            
            # Initialize delta monitor
            if not self.delta_monitor.initialize(site_url, drive_name):
                raise Exception("Failed to initialize delta monitor")
            
            # Set up change handlers
            self.delta_monitor.set_change_handlers(
                on_file_changed=self._handle_file_changed,
                on_file_added=self._handle_file_added,
                on_file_deleted=self._handle_file_deleted
            )
            
            # Start monitoring
            self.delta_monitor.start_monitoring()
            self.is_running = True
            
            logger.info("realtime_monitoring_started")
            return True
            
        except Exception as e:
            logger.error("realtime_monitoring_start_failed", error=str(e))
            self.alerting.send_alert(
                "Real-time Monitoring Failed to Start",
                f"Failed to start monitoring: {e}",
                "error"
            )
            return False
    
    def _handle_file_changed(self, new_file_info: Dict[str, Any], old_file_info: Dict[str, Any]):
        """Handle when an existing UW model file is modified"""
        file_name = new_file_info['file_name']
        deal_name = new_file_info['deal_name']
        
        logger.info("file_change_detected", 
                   file_name=file_name, 
                   deal_name=deal_name,
                   old_modified=old_file_info.get('last_modified'),
                   new_modified=new_file_info.get('last_modified'))
        
        self.alerting.send_alert(
            "File Change Detected",
            f"UW model file modified: {file_name} ({deal_name})\nTriggering full re-extraction...",
            "info"
        )
        
        # Trigger full re-extraction
        self._extract_single_file(new_file_info, "File Modified")
    
    def _handle_file_added(self, file_info: Dict[str, Any]):
        """Handle when a new UW model file is added"""
        file_name = file_info['file_name']
        deal_name = file_info['deal_name']
        
        logger.info("new_file_detected", 
                   file_name=file_name, 
                   deal_name=deal_name)
        
        self.alerting.send_alert(
            "New UW Model File Detected",
            f"New file added: {file_name} ({deal_name})\nTriggering extraction...",
            "info"
        )
        
        # Extract data from new file
        self._extract_single_file(file_info, "New File")
    
    def _handle_file_deleted(self, file_info: Dict[str, Any]):
        """Handle when a UW model file is deleted"""
        file_name = file_info['file_name']
        deal_name = file_info['deal_name']
        
        logger.info("file_deleted", 
                   file_name=file_name, 
                   deal_name=deal_name)
        
        self.alerting.send_alert(
            "UW Model File Deleted",
            f"File deleted: {file_name} ({deal_name})\nData remains in database for historical tracking.",
            "warning"
        )
    
    def _extract_single_file(self, file_info: Dict[str, Any], trigger_reason: str):
        """Extract data from a single file"""
        file_name = file_info['file_name']
        
        try:
            start_time = datetime.now()
            
            # Extract data
            extracted_data = self.graph_extractor.extract_from_file_info(
                file_info, self.excel_extractor
            )
            
            if extracted_data:
                # Load to database
                extraction_id = self.data_loader.load_complete_extraction_data(
                    extraction_data=extracted_data,
                    deal_stage=file_info['deal_stage'],
                    metadata={
                        'total_fields': len(self.mappings),
                        'successful': len([v for v in extracted_data.values() if v is not None]),
                        'duration_seconds': (datetime.now() - start_time).total_seconds(),
                        'extraction_method': 'graph_api_streaming_realtime',
                        'trigger_reason': trigger_reason
                    }
                )
                
                if extraction_id:
                    duration = (datetime.now() - start_time).total_seconds()
                    
                    logger.info("realtime_extraction_successful",
                               file_name=file_name,
                               extraction_id=extraction_id,
                               trigger_reason=trigger_reason,
                               duration_seconds=round(duration, 2))
                    
                    self.alerting.send_extraction_notification(
                        file_info, 'success', duration
                    )
                else:
                    logger.error("realtime_database_load_failed", file_name=file_name)
            else:
                logger.error("realtime_extraction_failed", file_name=file_name)
        
        except Exception as e:
            logger.error("realtime_extraction_error", 
                        file_name=file_name, 
                        trigger_reason=trigger_reason,
                        error=str(e))
            
            self.alerting.send_extraction_notification(
                file_info, str(e)
            )
    
    def stop(self):
        """Stop the workflow"""
        logger.info("stopping_realtime_workflow")
        
        self.is_running = False
        self.delta_monitor.stop_monitoring()
        
        self.alerting.send_alert(
            "Real-time Workflow Stopped",
            "Real-time extraction workflow has been stopped",
            "info"
        )
    
    def get_status(self) -> Dict[str, Any]:
        """Get current workflow status"""
        monitor_status = self.delta_monitor.get_monitoring_status()
        
        return {
            'workflow_running': self.is_running,
            'initial_extraction_complete': self.initial_extraction_complete,
            'monitoring': monitor_status,
            'mappings_loaded': len(self.mappings),
            'last_status_check': datetime.now().isoformat()
        }


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="B&R Real-time Extraction Workflow")
    parser.add_argument('--client-id', required=True, help='Azure AD client ID')
    parser.add_argument('--client-secret', required=True, help='Azure AD client secret') 
    parser.add_argument('--reference-file', required=True, help='Path to cell mapping reference file')
    parser.add_argument('--tenant-id', default='383e5745-a469-4712-aaa9-f7d79c981e10', help='Azure AD tenant ID')
    parser.add_argument('--site-url', default='bandrcapital.sharepoint.com:/sites/BRCapital-Internal', help='SharePoint site URL')
    parser.add_argument('--drive-name', default='Real Estate', help='SharePoint drive name')
    parser.add_argument('--skip-initial', action='store_true', help='Skip initial extraction (monitoring only)')
    
    args = parser.parse_args()
    
    # Create workflow
    workflow = RealtimeExtractionWorkflow(
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
        print("🚀 Starting B&R Real-time Extraction Workflow")
        print("=" * 70)
        
        # Phase 1: Initial extraction (if not skipped)
        if not args.skip_initial:
            print("\n📦 Phase 1: Initial Extraction of All Files")
            success = workflow.run_initial_extraction(args.site_url, args.drive_name)
            
            if not success:
                print("❌ Initial extraction failed. Check logs for details.")
                return 1
        
        # Phase 2: Real-time monitoring
        print("\n🔄 Phase 2: Starting Real-time Monitoring")
        success = workflow.start_realtime_monitoring(args.site_url, args.drive_name)
        
        if not success:
            print("❌ Real-time monitoring failed to start. Check logs for details.")
            return 1
        
        print("✅ Real-time monitoring active!")
        print("   • Monitoring SharePoint for file changes every 30 seconds")
        print("   • Changes will trigger automatic data extraction")
        print("   • Press Ctrl+C to stop")
        print("\n" + "=" * 70)
        
        # Keep running until interrupted
        while workflow.is_running:
            time.sleep(10)
            
            # Periodic status check
            status = workflow.get_status()
            if status['monitoring']['files_tracked'] > 0:
                print(f"📊 Monitoring {status['monitoring']['files_tracked']} files... "
                      f"(Last check: {status['monitoring']['last_check']})")
        
        return 0
        
    except Exception as e:
        logger.error("workflow_error", error=str(e))
        print(f"❌ Workflow error: {e}")
        return 1


if __name__ == "__main__":
    import time
    sys.exit(main())