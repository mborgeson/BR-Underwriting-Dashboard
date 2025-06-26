"""
B&R Capital Data Extraction Package
Handles Excel file extraction from SharePoint
"""

# Make key classes available at package level
from .excel_extraction_system import (
    CellMappingParser,
    ExcelDataExtractor,
    BatchFileProcessor,
    ExtractionError,
    export_to_csv
)

from .sharepoint_excel_integration import SharePointExcelExtractor

__all__ = [
    'CellMappingParser',
    'ExcelDataExtractor', 
    'BatchFileProcessor',
    'SharePointExcelExtractor',
    'ExtractionError',
    'export_to_csv'
]

__version__ = '1.0.0'