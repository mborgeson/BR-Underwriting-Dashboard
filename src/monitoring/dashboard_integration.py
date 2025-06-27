#!/usr/bin/env python3
"""
Dashboard Integration Service

Real-time communication bridge between monitoring system and dashboard.
Handles live data updates, status broadcasting, and event notifications.
"""

import json
import asyncio
import websockets
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable, Set
import structlog
from pathlib import Path
import time

logger = structlog.get_logger().bind(component="DashboardIntegration")

class DashboardIntegrationService:
    """Manages real-time dashboard updates and notifications"""
    
    def __init__(self, port: int = 8765):
        self.port = port
        self.websocket_server = None
        self.connected_clients: Set[websockets.WebSocketServerProtocol] = set()
        
        # Event queues
        self.update_queue = []
        self.status_cache = {}
        
        # Background tasks
        self.is_running = False
        self.server_thread = None
        
        # Monitoring integration
        self.monitoring_service = None
        
        logger.info("dashboard_integration_service_initialized", port=port)
    
    def set_monitoring_service(self, monitoring_service):
        """Connect to monitoring service for real-time updates"""
        self.monitoring_service = monitoring_service
        
        # Set up monitoring callbacks
        if hasattr(monitoring_service, 'set_dashboard_integration'):
            monitoring_service.set_dashboard_integration(self.handle_monitoring_event)
            logger.info("monitoring_service_connected")
    
    def start_integration_server(self):
        """Start WebSocket server for dashboard communication"""
        if self.is_running:
            logger.warning("dashboard_integration_already_running")
            return
        
        self.is_running = True
        self.server_thread = threading.Thread(target=self._run_websocket_server, daemon=True)
        self.server_thread.start()
        
        logger.info("dashboard_integration_server_started", port=self.port)
    
    def _run_websocket_server(self):
        """Run WebSocket server in background thread"""
        asyncio.new_event_loop().run_until_complete(self._start_server())
    
    async def _start_server(self):
        """Start WebSocket server"""
        try:
            self.websocket_server = await websockets.serve(
                self._handle_websocket_connection,
                "localhost",
                self.port
            )
            
            logger.info("websocket_server_started", port=self.port)
            
            # Keep server running
            await self.websocket_server.wait_closed()
            
        except Exception as e:
            logger.error("websocket_server_failed", error=str(e))
    
    async def _handle_websocket_connection(self, websocket, path):
        """Handle new WebSocket connection from dashboard"""
        self.connected_clients.add(websocket)
        client_info = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        
        logger.info("dashboard_client_connected", client=client_info, total_clients=len(self.connected_clients))
        
        try:
            # Send initial status to new client
            await self._send_initial_status(websocket)
            
            # Handle incoming messages
            async for message in websocket:
                await self._handle_client_message(websocket, message)
                
        except websockets.exceptions.ConnectionClosed:
            logger.info("dashboard_client_disconnected", client=client_info)
        except Exception as e:
            logger.error("websocket_connection_error", client=client_info, error=str(e))
        finally:
            self.connected_clients.discard(websocket)
            logger.info("dashboard_client_removed", remaining_clients=len(self.connected_clients))
    
    async def _send_initial_status(self, websocket):
        """Send initial status and cached data to new dashboard client"""
        try:
            initial_data = {
                'event_type': 'initial_status',
                'timestamp': datetime.now().isoformat(),
                'status': self.status_cache,
                'recent_updates': self.update_queue[-10:] if self.update_queue else [],
                'server_info': {
                    'monitoring_active': self.monitoring_service is not None,
                    'connected_clients': len(self.connected_clients)
                }
            }
            
            await websocket.send(json.dumps(initial_data))
            logger.debug("initial_status_sent_to_client")
            
        except Exception as e:
            logger.error("send_initial_status_failed", error=str(e))
    
    async def _handle_client_message(self, websocket, message):
        """Handle messages from dashboard clients"""
        try:
            data = json.loads(message)
            message_type = data.get('type')
            
            if message_type == 'ping':
                await websocket.send(json.dumps({'type': 'pong', 'timestamp': datetime.now().isoformat()}))
            
            elif message_type == 'request_status':
                await self._send_current_status(websocket)
            
            elif message_type == 'request_file_list':
                await self._send_monitored_files(websocket)
            
            elif message_type == 'force_refresh':
                await self._handle_force_refresh_request(websocket)
            
            else:
                logger.warning("unknown_message_type", type=message_type)
                
        except json.JSONDecodeError:
            logger.error("invalid_json_from_client", message=message[:100])
        except Exception as e:
            logger.error("client_message_handling_failed", error=str(e))
    
    async def _send_current_status(self, websocket):
        """Send current monitoring status to client"""
        try:
            if self.monitoring_service:
                status = self.monitoring_service.get_enhanced_monitoring_status()
            else:
                status = {'error': 'No monitoring service connected'}
            
            response = {
                'event_type': 'status_update',
                'timestamp': datetime.now().isoformat(),
                'status': status
            }
            
            await websocket.send(json.dumps(response))
            
        except Exception as e:
            logger.error("send_current_status_failed", error=str(e))
    
    async def _send_monitored_files(self, websocket):
        """Send list of currently monitored files"""
        try:
            if self.monitoring_service and hasattr(self.monitoring_service, 'known_files'):
                files = list(self.monitoring_service.known_files.values())
            else:
                files = []
            
            response = {
                'event_type': 'monitored_files',
                'timestamp': datetime.now().isoformat(),
                'files': files,
                'count': len(files)
            }
            
            await websocket.send(json.dumps(response))
            
        except Exception as e:
            logger.error("send_monitored_files_failed", error=str(e))
    
    async def _handle_force_refresh_request(self, websocket):
        """Handle request to force refresh monitoring baseline"""
        try:
            if self.monitoring_service and hasattr(self.monitoring_service, 'force_refresh'):
                success = self.monitoring_service.force_refresh()
                
                response = {
                    'event_type': 'force_refresh_result',
                    'timestamp': datetime.now().isoformat(),
                    'success': success,
                    'message': 'Monitoring baseline refreshed' if success else 'Refresh failed'
                }
            else:
                response = {
                    'event_type': 'force_refresh_result',
                    'timestamp': datetime.now().isoformat(),
                    'success': False,
                    'message': 'No monitoring service available'
                }
            
            await websocket.send(json.dumps(response))
            
        except Exception as e:
            logger.error("handle_force_refresh_failed", error=str(e))
    
    def handle_monitoring_event(self, event_data: Dict[str, Any]):
        """Handle events from monitoring service"""
        try:
            # Add timestamp and source
            enhanced_event = {
                **event_data,
                'source': 'monitoring_service',
                'dashboard_timestamp': datetime.now().isoformat()
            }
            
            # Add to update queue
            self.update_queue.append(enhanced_event)
            
            # Keep only last 100 updates
            if len(self.update_queue) > 100:
                self.update_queue = self.update_queue[-100:]
            
            # Update status cache
            if event_data.get('event_type') == 'status_update':
                self.status_cache = event_data.get('status', {})
            
            # Broadcast to all connected dashboard clients
            if self.connected_clients:
                asyncio.create_task(self._broadcast_to_clients(enhanced_event))
            
            logger.info("monitoring_event_processed", 
                       event_type=event_data.get('event_type'),
                       connected_clients=len(self.connected_clients))
            
        except Exception as e:
            logger.error("monitoring_event_processing_failed", error=str(e))
    
    async def _broadcast_to_clients(self, event_data: Dict[str, Any]):
        """Broadcast event to all connected dashboard clients"""
        if not self.connected_clients:
            return
        
        message = json.dumps(event_data)
        disconnected_clients = set()
        
        for client in self.connected_clients:
            try:
                await client.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error("broadcast_to_client_failed", error=str(e))
                disconnected_clients.add(client)
        
        # Remove disconnected clients
        for client in disconnected_clients:
            self.connected_clients.discard(client)
        
        logger.debug("event_broadcasted_to_clients", 
                    successful_clients=len(self.connected_clients),
                    failed_clients=len(disconnected_clients))
    
    def send_extraction_update(self, file_info: Dict[str, Any], extraction_result: Dict[str, Any]):
        """Send extraction completion update to dashboard"""
        event = {
            'event_type': 'extraction_completed',
            'timestamp': datetime.now().isoformat(),
            'file_info': file_info,
            'extraction_result': extraction_result,
            'files_processed': extraction_result.get('successful_extractions', 0)
        }
        
        self.handle_monitoring_event(event)
    
    def send_error_notification(self, error_type: str, error_message: str, context: Dict[str, Any] = None):
        """Send error notification to dashboard"""
        event = {
            'event_type': 'error_notification',
            'timestamp': datetime.now().isoformat(),
            'error_type': error_type,
            'error_message': error_message,
            'context': context or {}
        }
        
        self.handle_monitoring_event(event)
    
    def send_status_update(self, status_data: Dict[str, Any]):
        """Send status update to dashboard"""
        event = {
            'event_type': 'status_update',
            'timestamp': datetime.now().isoformat(),
            'status': status_data
        }
        
        self.handle_monitoring_event(event)
    
    def get_integration_status(self) -> Dict[str, Any]:
        """Get dashboard integration status"""
        return {
            'server_running': self.is_running,
            'websocket_port': self.port,
            'connected_clients': len(self.connected_clients),
            'monitoring_connected': self.monitoring_service is not None,
            'updates_queued': len(self.update_queue),
            'last_update': self.update_queue[-1]['timestamp'] if self.update_queue else None
        }
    
    def stop_integration_server(self):
        """Stop dashboard integration server"""
        logger.info("stopping_dashboard_integration_server")
        
        self.is_running = False
        
        if self.websocket_server:
            self.websocket_server.close()
        
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)
        
        logger.info("dashboard_integration_server_stopped")

# Example dashboard client code for testing
class DashboardTestClient:
    """Test client for dashboard integration"""
    
    def __init__(self, server_url: str = "ws://localhost:8765"):
        self.server_url = server_url
        self.websocket = None
        
    async def connect_and_listen(self):
        """Connect to dashboard integration server and listen for updates"""
        try:
            async with websockets.connect(self.server_url) as websocket:
                self.websocket = websocket
                print(f"Connected to dashboard integration server at {self.server_url}")
                
                # Send ping to test connection
                await websocket.send(json.dumps({'type': 'ping'}))
                
                # Listen for messages
                async for message in websocket:
                    data = json.loads(message)
                    print(f"Received: {data['event_type']} - {data.get('timestamp')}")
                    
                    if data['event_type'] == 'file_changes_detected':
                        print(f"  Changes: {data['changes_count']} files")
                    elif data['event_type'] == 'extraction_completed':
                        print(f"  Extraction: {data['file_info']['file_name']}")
                    
        except Exception as e:
            print(f"Dashboard client error: {e}")

def main():
    """Test the dashboard integration service"""
    integration = DashboardIntegrationService()
    integration.start_integration_server()
    
    print("Dashboard integration server started on port 8765")
    print("Connect with: ws://localhost:8765")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        integration.stop_integration_server()
        print("Dashboard integration server stopped")

if __name__ == "__main__":
    main()