"""
B&R Capital Dashboard - Comprehensive Error Handling System

This module provides robust error handling for Excel data extraction,
ensuring missing values are properly handled with NaN and providing
detailed error categorization and reporting.

Key Features:
- NaN handling for all missing value scenarios
- Error categorization (sheet missing, cell invalid, formula errors, etc.)
- Comprehensive error reporting and logging
- Graceful degradation without stopping extraction
- Detailed error statistics and recovery suggestions
"""

import numpy as np
import pandas as pd
import logging
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
import structlog

class ErrorCategory(Enum):
    """Categories of extraction errors"""
    MISSING_SHEET = "missing_sheet"
    INVALID_CELL_ADDRESS = "invalid_cell_address"
    CELL_NOT_FOUND = "cell_not_found"
    FORMULA_ERROR = "formula_error"
    DATA_TYPE_ERROR = "data_type_error"
    EMPTY_VALUE = "empty_value"
    FILE_ACCESS_ERROR = "file_access_error"
    PARSING_ERROR = "parsing_error"
    UNKNOWN_ERROR = "unknown_error"

@dataclass
class ExtractionError:
    """Detailed error information for tracking and reporting"""
    category: ErrorCategory
    field_name: str
    sheet_name: str
    cell_address: str
    error_message: str
    original_value: Any = None
    suggested_fix: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

class ErrorHandler:
    """
    Comprehensive error handling system for Excel data extraction
    """
    
    def __init__(self):
        self.errors: List[ExtractionError] = []
        self.error_counts: Dict[ErrorCategory, int] = {cat: 0 for cat in ErrorCategory}
        self.logger = structlog.get_logger(__name__)
        
    def handle_missing_sheet(self, field_name: str, sheet_name: str, 
                           available_sheets: List[str]) -> Any:
        """Handle missing sheet scenarios"""
        
        # Try to find similar sheet names
        similar_sheets = self._find_similar_sheets(sheet_name, available_sheets)
        
        suggested_fix = None
        if similar_sheets:
            suggested_fix = f"Similar sheets found: {', '.join(similar_sheets[:3])}"
        
        error = ExtractionError(
            category=ErrorCategory.MISSING_SHEET,
            field_name=field_name,
            sheet_name=sheet_name,
            cell_address="N/A",
            error_message=f"Sheet '{sheet_name}' not found in workbook",
            suggested_fix=suggested_fix
        )
        
        self._log_error(error)
        return np.nan
        
    def handle_invalid_cell_address(self, field_name: str, sheet_name: str, 
                                  cell_address: str, error_msg: str) -> Any:
        """Handle invalid cell address formats"""
        
        error = ExtractionError(
            category=ErrorCategory.INVALID_CELL_ADDRESS,
            field_name=field_name,
            sheet_name=sheet_name,
            cell_address=cell_address,
            error_message=f"Invalid cell address format: {error_msg}",
            suggested_fix="Check cell address format (e.g., 'A1', 'B10', '$C$5')"
        )
        
        self._log_error(error)
        return np.nan
        
    def handle_cell_not_found(self, field_name: str, sheet_name: str, 
                            cell_address: str, sheet_size: Tuple[int, int] = None) -> Any:
        """Handle cases where cell address is outside sheet bounds"""
        
        suggested_fix = "Check if cell address is within sheet bounds"
        if sheet_size:
            max_row, max_col = sheet_size
            suggested_fix = f"Sheet has {max_row} rows and {max_col} columns"
        
        error = ExtractionError(
            category=ErrorCategory.CELL_NOT_FOUND,
            field_name=field_name,
            sheet_name=sheet_name,
            cell_address=cell_address,
            error_message=f"Cell {cell_address} not found or outside sheet bounds",
            suggested_fix=suggested_fix
        )
        
        self._log_error(error)
        return np.nan
        
    def handle_formula_error(self, field_name: str, sheet_name: str, 
                           cell_address: str, formula_error: str) -> Any:
        """Handle Excel formula errors"""
        
        error_meanings = {
            '#REF!': 'Invalid cell reference',
            '#VALUE!': 'Wrong data type for operation',
            '#DIV/0!': 'Division by zero',
            '#NAME?': 'Unrecognized function or name',
            '#N/A': 'Value not available',
            '#NULL!': 'Incorrect range operator',
            '#NUM!': 'Invalid numeric value'
        }
        
        meaning = error_meanings.get(formula_error, 'Unknown formula error')
        
        error = ExtractionError(
            category=ErrorCategory.FORMULA_ERROR,
            field_name=field_name,
            sheet_name=sheet_name,
            cell_address=cell_address,
            error_message=f"Formula error {formula_error}: {meaning}",
            original_value=formula_error,
            suggested_fix=f"Fix formula causing {formula_error} error"
        )
        
        self._log_error(error)
        return np.nan
        
    def handle_data_type_error(self, field_name: str, sheet_name: str, 
                             cell_address: str, value: Any, expected_type: str) -> Any:
        """Handle data type conversion errors"""
        
        error = ExtractionError(
            category=ErrorCategory.DATA_TYPE_ERROR,
            field_name=field_name,
            sheet_name=sheet_name,
            cell_address=cell_address,
            error_message=f"Cannot convert '{value}' to {expected_type}",
            original_value=value,
            suggested_fix=f"Ensure cell contains valid {expected_type} data"
        )
        
        self._log_error(error)
        return np.nan
        
    def handle_empty_value(self, field_name: str, sheet_name: str, 
                         cell_address: str, treat_as_error: bool = False) -> Any:
        """Handle empty/null values"""
        
        if treat_as_error:
            error = ExtractionError(
                category=ErrorCategory.EMPTY_VALUE,
                field_name=field_name,
                sheet_name=sheet_name,
                cell_address=cell_address,
                error_message="Cell is empty or contains null value",
                suggested_fix="Verify if this field should contain data"
            )
            self._log_error(error)
        
        return np.nan
        
    def handle_parsing_error(self, field_name: str, sheet_name: str, 
                           cell_address: str, error_msg: str) -> Any:
        """Handle general parsing errors"""
        
        error = ExtractionError(
            category=ErrorCategory.PARSING_ERROR,
            field_name=field_name,
            sheet_name=sheet_name,
            cell_address=cell_address,
            error_message=f"Parsing error: {error_msg}",
            suggested_fix="Check cell content format and data validity"
        )
        
        self._log_error(error)
        return np.nan
        
    def handle_file_access_error(self, field_name: str, error_msg: str) -> Any:
        """Handle file access and loading errors"""
        
        error = ExtractionError(
            category=ErrorCategory.FILE_ACCESS_ERROR,
            field_name=field_name,
            sheet_name="N/A",
            cell_address="N/A",
            error_message=f"File access error: {error_msg}",
            suggested_fix="Check file path, permissions, and file format"
        )
        
        self._log_error(error)
        return np.nan
        
    def handle_unknown_error(self, field_name: str, sheet_name: str, 
                           cell_address: str, error_msg: str) -> Any:
        """Handle unexpected errors"""
        
        error = ExtractionError(
            category=ErrorCategory.UNKNOWN_ERROR,
            field_name=field_name,
            sheet_name=sheet_name,
            cell_address=cell_address,
            error_message=f"Unexpected error: {error_msg}",
            suggested_fix="Contact support with error details"
        )
        
        self._log_error(error)
        return np.nan
        
    def process_cell_value(self, value: Any, field_name: str = "", 
                         sheet_name: str = "", cell_address: str = "") -> Any:
        """
        Process and validate cell values with comprehensive error handling
        
        Args:
            value: Raw cell value from Excel
            field_name: Name of the field being extracted
            sheet_name: Name of the sheet
            cell_address: Cell address (e.g., 'A1')
            
        Returns:
            Processed value or np.nan for errors/missing values
        """
        
        # Handle None/empty values
        if value is None or value == '':
            return self.handle_empty_value(field_name, sheet_name, cell_address)
            
        # Handle string values
        if isinstance(value, str):
            # Check for Excel formula errors
            excel_errors = ['#REF!', '#VALUE!', '#DIV/0!', '#NAME?', 
                          '#N/A', '#NULL!', '#NUM!']
            
            for error_code in excel_errors:
                if error_code in value:
                    return self.handle_formula_error(field_name, sheet_name, 
                                                   cell_address, error_code)
            
            # Handle string representations of common missing values
            missing_indicators = ['n/a', 'na', 'null', 'none', '', '-', 'tbd', 'tba']
            if value.lower().strip() in missing_indicators:
                return self.handle_empty_value(field_name, sheet_name, cell_address)
                
            # Clean and return string value
            return value.strip()
            
        # Handle numeric values
        if isinstance(value, (int, float)):
            # Check for NaN/infinite values
            if pd.isna(value) or np.isinf(value):
                return self.handle_empty_value(field_name, sheet_name, cell_address)
            return value
            
        # Handle datetime values
        if isinstance(value, datetime):
            return value
            
        # Handle boolean values
        if isinstance(value, bool):
            return value
            
        # Handle other types
        try:
            # Try to convert to string as fallback
            return str(value)
        except Exception as e:
            return self.handle_data_type_error(field_name, sheet_name, 
                                             cell_address, value, "string")
    
    def _find_similar_sheets(self, target_sheet: str, available_sheets: List[str], 
                           threshold: float = 0.6) -> List[str]:
        """Find sheets with similar names using simple string matching"""
        
        similar_sheets = []
        target_lower = target_sheet.lower()
        
        for sheet in available_sheets:
            sheet_lower = sheet.lower()
            
            # Exact match (case-insensitive)
            if target_lower == sheet_lower:
                return [sheet]
                
            # Contains match
            if target_lower in sheet_lower or sheet_lower in target_lower:
                similar_sheets.append(sheet)
                continue
                
            # Word-based similarity
            target_words = set(target_lower.split())
            sheet_words = set(sheet_lower.split())
            
            if target_words and sheet_words:
                intersection = target_words.intersection(sheet_words)
                union = target_words.union(sheet_words)
                similarity = len(intersection) / len(union)
                
                if similarity >= threshold:
                    similar_sheets.append(sheet)
        
        return similar_sheets
    
    def _log_error(self, error: ExtractionError):
        """Log error and update counters"""
        
        self.errors.append(error)
        self.error_counts[error.category] += 1
        
        # Log with structured logging
        self.logger.warning(
            "extraction_error",
            category=error.category.value,
            field_name=error.field_name,
            sheet_name=error.sheet_name,
            cell_address=error.cell_address,
            error_message=error.error_message,
            suggested_fix=error.suggested_fix
        )
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Generate comprehensive error summary"""
        
        total_errors = len(self.errors)
        
        if total_errors == 0:
            return {
                "total_errors": 0,
                "error_rate": 0.0,
                "categories": {},
                "summary": "No errors encountered during extraction"
            }
        
        # Error breakdown by category
        category_breakdown = {}
        for category, count in self.error_counts.items():
            if count > 0:
                category_breakdown[category.value] = {
                    "count": count,
                    "percentage": round((count / total_errors) * 100, 1)
                }
        
        # Most common errors
        common_errors = {}
        error_messages = {}
        for error in self.errors:
            key = f"{error.category.value}_{error.error_message}"
            if key not in error_messages:
                error_messages[key] = {
                    "category": error.category.value,
                    "message": error.error_message,
                    "count": 0,
                    "example_field": error.field_name,
                    "suggested_fix": error.suggested_fix
                }
            error_messages[key]["count"] += 1
        
        # Sort by frequency and take top 10
        sorted_errors = sorted(error_messages.values(), 
                             key=lambda x: x["count"], reverse=True)
        common_errors = sorted_errors[:10]
        
        # Generate actionable recommendations
        recommendations = self._generate_recommendations()
        
        return {
            "total_errors": total_errors,
            "error_breakdown_by_category": category_breakdown,
            "most_common_errors": common_errors,
            "recommendations": recommendations,
            "detailed_errors": [
                {
                    "field_name": error.field_name,
                    "category": error.category.value,
                    "sheet_name": error.sheet_name,
                    "cell_address": error.cell_address,
                    "error_message": error.error_message,
                    "suggested_fix": error.suggested_fix,
                    "timestamp": error.timestamp.isoformat()
                }
                for error in self.errors
            ]
        }
    
    def _generate_recommendations(self) -> List[str]:
        """Generate actionable recommendations based on error patterns"""
        
        recommendations = []
        
        # Missing sheet recommendations
        if self.error_counts[ErrorCategory.MISSING_SHEET] > 0:
            recommendations.append(
                "❌ Missing Sheets: Verify sheet names in the reference file match those in Excel files"
            )
        
        # Formula error recommendations  
        if self.error_counts[ErrorCategory.FORMULA_ERROR] > 0:
            recommendations.append(
                "🔢 Formula Errors: Review Excel formulas for errors like #REF!, #VALUE!, #DIV/0!"
            )
        
        # Cell address recommendations
        if self.error_counts[ErrorCategory.INVALID_CELL_ADDRESS] > 0:
            recommendations.append(
                "📍 Invalid Addresses: Check cell address format in reference file (e.g., 'A1', 'B10')"
            )
        
        # Empty value recommendations
        if self.error_counts[ErrorCategory.EMPTY_VALUE] > 5:  # Only if many empty values
            recommendations.append(
                "📝 Many Empty Values: Verify if missing data is expected or indicates data quality issues"
            )
        
        # Data type recommendations
        if self.error_counts[ErrorCategory.DATA_TYPE_ERROR] > 0:
            recommendations.append(
                "🔄 Data Type Issues: Ensure cells contain the expected data types (numbers, text, dates)"
            )
        
        # General recommendation
        if len(self.errors) > 0:
            recommendations.append(
                "📊 Review the detailed error list above for field-specific fixes"
            )
        
        return recommendations
    
    def export_error_report(self, file_path: str):
        """Export detailed error report to JSON file"""
        
        import json
        
        report = self.get_error_summary()
        
        with open(file_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
    
    def reset(self):
        """Reset error tracking for new extraction"""
        
        self.errors.clear()
        self.error_counts = {cat: 0 for cat in ErrorCategory}


# Enhanced cell value processor function for easy integration
def process_cell_value_with_error_handling(value: Any, field_name: str = "", 
                                         sheet_name: str = "", cell_address: str = "",
                                         error_handler: ErrorHandler = None) -> Any:
    """
    Standalone function for processing cell values with error handling
    
    Args:
        value: Raw cell value from Excel
        field_name: Name of the field being extracted
        sheet_name: Name of the sheet
        cell_address: Cell address (e.g., 'A1')
        error_handler: ErrorHandler instance (creates new one if None)
        
    Returns:
        Processed value or np.nan for errors/missing values
    """
    
    if error_handler is None:
        error_handler = ErrorHandler()
    
    return error_handler.process_cell_value(value, field_name, sheet_name, cell_address)