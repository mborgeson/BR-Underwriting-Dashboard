#!/usr/bin/env python3
"""
Real-time Alerting Service

Handles notifications for monitoring events, failures, and system status.
Supports multiple notification channels (console, file, email, webhook).
"""

import json
import smtplib
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import structlog
from pathlib import Path

logger = structlog.get_logger().bind(component="AlertingService")

class AlertingService:
    """Handles real-time notifications and alerts"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config = self._load_config(config_file)
        self.alert_history = []
        
        # Alert channels
        self.console_enabled = True
        self.file_enabled = True
        self.email_enabled = self.config.get('email', {}).get('enabled', False)
        self.webhook_enabled = self.config.get('webhook', {}).get('enabled', False)
        
        # Alert file
        self.alert_file = Path("logs/monitoring_alerts.jsonl")
        self.alert_file.parent.mkdir(exist_ok=True)
        
        logger.info("alerting_service_initialized", 
                   console=self.console_enabled,
                   file=self.file_enabled,
                   email=self.email_enabled,
                   webhook=self.webhook_enabled)
    
    def _load_config(self, config_file: Optional[str]) -> Dict[str, Any]:
        """Load alerting configuration"""
        default_config = {
            'email': {
                'enabled': False,
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'username': '',
                'password': '',
                'from_email': '',
                'to_emails': []
            },
            'webhook': {
                'enabled': False,
                'url': '',
                'headers': {}
            }
        }
        
        if config_file and Path(config_file).exists():
            try:
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
            except Exception as e:
                logger.warning("config_load_failed", config_file=config_file, error=str(e))
        
        return default_config
    
    def send_alert(self, title: str, message: str, level: str = "info", 
                   metadata: Optional[Dict[str, Any]] = None):
        """Send alert through all enabled channels"""
        alert_data = {
            'timestamp': datetime.now().isoformat(),
            'title': title,
            'message': message,
            'level': level,
            'metadata': metadata or {}
        }
        
        # Store in history
        self.alert_history.append(alert_data)
        
        # Keep only last 1000 alerts in memory
        if len(self.alert_history) > 1000:
            self.alert_history = self.alert_history[-1000:]
        
        # Send through enabled channels
        if self.console_enabled:
            self._send_console_alert(alert_data)
        
        if self.file_enabled:
            self._send_file_alert(alert_data)
        
        if self.email_enabled:
            self._send_email_alert(alert_data)
        
        if self.webhook_enabled:
            self._send_webhook_alert(alert_data)
    
    def _send_console_alert(self, alert_data: Dict[str, Any]):
        """Send alert to console with color coding"""
        level = alert_data['level']
        timestamp = alert_data['timestamp']
        title = alert_data['title']
        message = alert_data['message']
        
        # Color codes
        colors = {
            'info': '\033[94m',      # Blue
            'warning': '\033[93m',   # Yellow
            'error': '\033[91m',     # Red
            'success': '\033[92m',   # Green
            'reset': '\033[0m'       # Reset
        }
        
        color = colors.get(level, colors['reset'])
        level_icon = {
            'info': 'ℹ️',
            'warning': '⚠️',
            'error': '❌',
            'success': '✅'
        }.get(level, '📢')
        
        print(f"{color}{level_icon} [{timestamp}] {title}{colors['reset']}")
        if message != title:
            print(f"   {message}")
        print()
    
    def _send_file_alert(self, alert_data: Dict[str, Any]):
        """Send alert to log file"""
        try:
            with open(self.alert_file, 'a') as f:
                f.write(json.dumps(alert_data) + '\n')
        except Exception as e:
            logger.error("file_alert_failed", error=str(e))
    
    def _send_email_alert(self, alert_data: Dict[str, Any]):
        """Send alert via email"""
        try:
            email_config = self.config['email']
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = email_config['from_email']
            msg['To'] = ', '.join(email_config['to_emails'])
            msg['Subject'] = f"[B&R Dashboard] {alert_data['title']}"
            
            # Email body
            body = f"""
B&R Capital Dashboard Alert

Time: {alert_data['timestamp']}
Level: {alert_data['level'].upper()}
Title: {alert_data['title']}

Message:
{alert_data['message']}

Metadata:
{json.dumps(alert_data['metadata'], indent=2)}
"""
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
            server.starttls()
            server.login(email_config['username'], email_config['password'])
            
            for to_email in email_config['to_emails']:
                server.sendmail(email_config['from_email'], to_email, msg.as_string())
            
            server.quit()
            
        except Exception as e:
            logger.error("email_alert_failed", error=str(e))
    
    def _send_webhook_alert(self, alert_data: Dict[str, Any]):
        """Send alert to webhook endpoint"""
        try:
            webhook_config = self.config['webhook']
            
            # Prepare payload
            payload = {
                'source': 'b_and_r_dashboard',
                'alert': alert_data
            }
            
            # Send to webhook
            response = requests.post(
                webhook_config['url'],
                json=payload,
                headers=webhook_config.get('headers', {}),
                timeout=10
            )
            response.raise_for_status()
            
        except Exception as e:
            logger.error("webhook_alert_failed", error=str(e))
    
    def send_monitoring_summary(self, stats: Dict[str, Any]):
        """Send periodic monitoring summary"""
        message = f"""
Monitoring Summary:
- Files tracked: {stats.get('files_tracked', 0)}
- Changes detected today: {stats.get('changes_today', 0)}
- Extractions completed: {stats.get('extractions_completed', 0)}
- Last check: {stats.get('last_check', 'Never')}
- Status: {stats.get('status', 'Unknown')}
"""
        
        self.send_alert(
            "Daily Monitoring Summary",
            message,
            "info",
            stats
        )
    
    def send_extraction_notification(self, file_info: Dict[str, Any], 
                                   result: str, duration: Optional[float] = None):
        """Send notification about extraction completion"""
        file_name = file_info.get('file_name', 'Unknown')
        deal_name = file_info.get('deal_name', 'Unknown')
        
        if result == 'success':
            message = f"Successfully extracted data from {file_name} ({deal_name})"
            if duration:
                message += f" in {duration:.1f}s"
            level = 'success'
        else:
            message = f"Failed to extract data from {file_name} ({deal_name}): {result}"
            level = 'error'
        
        self.send_alert(
            f"Extraction {result.title()}",
            message,
            level,
            {
                'file_info': file_info,
                'result': result,
                'duration': duration
            }
        )
    
    def get_recent_alerts(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get alerts from the last N hours"""
        cutoff = datetime.now().timestamp() - (hours * 3600)
        
        recent_alerts = []
        for alert in reversed(self.alert_history):
            alert_time = datetime.fromisoformat(alert['timestamp']).timestamp()
            if alert_time >= cutoff:
                recent_alerts.append(alert)
        
        return recent_alerts
    
    def get_alert_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get summary of alerts in the last N hours"""
        recent_alerts = self.get_recent_alerts(hours)
        
        summary = {
            'total_alerts': len(recent_alerts),
            'by_level': {},
            'last_alert': None
        }
        
        for alert in recent_alerts:
            level = alert['level']
            summary['by_level'][level] = summary['by_level'].get(level, 0) + 1
        
        if recent_alerts:
            summary['last_alert'] = recent_alerts[0]
        
        return summary
    
    def clear_history(self):
        """Clear alert history"""
        self.alert_history.clear()
        logger.info("alert_history_cleared")