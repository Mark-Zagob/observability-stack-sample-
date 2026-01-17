# applications/consumer/logger.py

import logging
import sys
from datetime import datetime, timezone
from pythonjsonlogger import jsonlogger

from config import settings


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter with additional fields"""
    
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        
        # Timestamp in ISO format
        log_record['timestamp'] = datetime.now(timezone.utc).isoformat()
        
        # Service identification
        log_record['service'] = settings.app_name
        
        # Log level - lấy từ record thay vì log_record
        log_record['level'] = record.levelname
        
        # Source location
        log_record['logger'] = record.name
        log_record['module'] = record.module
        log_record['function'] = record.funcName
        log_record['line'] = record.lineno


def setup_logger(name: str = None) -> logging.Logger:
    """Setup structured JSON logger"""
    
    logger = logging.getLogger(name or settings.app_name)
    logger.setLevel(getattr(logging, settings.log_level))
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Console handler with JSON format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, settings.log_level))
    
    # JSON formatter - không dùng rename_fields
    formatter = CustomJsonFormatter(
        fmt='%(timestamp)s %(level)s %(service)s %(message)s'
    )
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


# Create default logger instance
logger = setup_logger()
