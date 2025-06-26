#!/usr/bin/env python3
"""
Smart Cell Finder - Automatically locate data cells when mappings are incorrect
Handles template variations and evolving Excel structures
"""

import pyxlsb
import openpyxl
import re
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd

class SmartCellFinder:
    """Intelligently finds data cells even when mappings are incorrect"""
    
    def __init__(self):
        self.common_patterns = {
            'PROPERTY_NAME': ['project name', 'property name', 'name', 'project'],
            'PROPERTY_CITY': ['city'],
            'PROPERTY_STATE': ['state'],
            'YEAR_BUILT': ['year built', 'built', 'construction'],
            'UNITS': ['units', 'unit count', 'number of units', 'total units'],
            'AVG_SQUARE_FEET': ['avg nrsf', 'average square feet', 'avg sf', 'square feet'],
            'PURCHASE_PRICE': ['purchase price', 'acquisition price', 'price'],
            'NOI': ['noi', 'net operating income'],
        }
    
    def find_data_by_proximity(self, sheet_data: Dict[Tuple[int, int], Any], 
                              field_name: str, search_area: Tuple[int, int, int, int] = None) -> Optional[Any]:
        """
        Find data by looking for label patterns and checking adjacent cells
        
        Args:
            sheet_data: Dictionary of (row, col) -> value
            field_name: Field to search for
            search_area: Optional (min_row, max_row, min_col, max_col) to limit search
        """
        if field_name not in self.common_patterns:
            return None
        
        patterns = self.common_patterns[field_name]
        
        # If no search area specified, search first 20 rows and 15 columns
        if search_area is None:
            search_area = (1, 20, 1, 15)
        
        min_row, max_row, min_col, max_col = search_area
        
        # Find cells containing our label patterns
        label_cells = []
        for (row, col), value in sheet_data.items():
            if (min_row <= row <= max_row and min_col <= col <= max_col and 
                value is not None):
                value_str = str(value).lower().strip()
                
                for pattern in patterns:
                    if pattern.lower() in value_str:
                        label_cells.append((row, col, value, pattern))
        
        # For each label found, check adjacent cells for data
        candidates = []
        for label_row, label_col, label_value, pattern in label_cells:
            # Check cells to the right (common pattern)
            for offset in [1, 2, 3]:
                data_cell = sheet_data.get((label_row, label_col + offset))
                if data_cell is not None and str(data_cell).strip():
                    # Filter out other labels
                    if not self._looks_like_label(str(data_cell)):
                        candidates.append((data_cell, f"Right of '{label_value}' at +{offset}"))
            
            # Check cells below (another common pattern)
            for offset in [1, 2]:
                data_cell = sheet_data.get((label_row + offset, label_col))
                if data_cell is not None and str(data_cell).strip():
                    if not self._looks_like_label(str(data_cell)):
                        candidates.append((data_cell, f"Below '{label_value}' at +{offset}"))
        
        # Return the most likely candidate
        if candidates:
            # For now, return the first non-label value found
            # Could be enhanced with scoring logic
            return candidates[0][0]
        
        return None
    
    def _looks_like_label(self, value: str) -> bool:
        """Check if a value looks like a label rather than data"""
        value = value.strip().lower()
        
        # Obvious labels
        label_indicators = [
            'total', 'rate', 'ratio', 'percentage', 'amount', 'value', 'price',
            'date', 'number of', 'assessed', 'levy', 'taxes', 'parcel'
        ]
        
        for indicator in label_indicators:
            if indicator in value:
                return True
        
        # Very long strings are likely labels
        if len(value) > 30:
            return True
            
        return False
    
    def extract_sheet_data(self, file_path: str, sheet_name: str) -> Dict[Tuple[int, int], Any]:
        """Extract all data from a sheet into a dictionary"""
        sheet_data = {}
        
        try:
            if file_path.endswith('.xlsb'):
                with pyxlsb.open_workbook(file_path) as wb:
                    with wb.get_sheet(sheet_name) as sheet:
                        for row_data in sheet.rows():
                            if not row_data:
                                continue
                            for cell in row_data:
                                if hasattr(cell, 'r') and hasattr(cell, 'c') and cell.v is not None:
                                    sheet_data[(cell.r, cell.c)] = cell.v
            else:
                wb = openpyxl.load_workbook(file_path, data_only=True)
                sheet = wb[sheet_name]
                for row in sheet.iter_rows():
                    for cell in row:
                        if cell.value is not None:
                            sheet_data[(cell.row, cell.column)] = cell.value
                wb.close()
                            
        except Exception as e:
            print(f"Error reading sheet {sheet_name}: {e}")
        
        return sheet_data
    
    def find_field_with_fallback(self, file_path: str, sheet_name: str, 
                                original_cell: str, field_name: str) -> Any:
        """
        Try original mapping first, then fall back to smart discovery
        """
        # Try original mapping
        try:
            if file_path.endswith('.xlsb'):
                with pyxlsb.open_workbook(file_path) as wb:
                    with wb.get_sheet(sheet_name) as sheet:
                        # Parse original cell address
                        match = re.match(r'([A-Z]+)(\d+)', original_cell.replace('$', ''))
                        if match:
                            col_str, row_str = match.groups()
                            target_row = int(row_str)
                            target_col = sum((ord(char) - ord('A') + 1) * (26 ** i) 
                                           for i, char in enumerate(reversed(col_str)))
                            
                            # Look for the cell
                            for row_data in sheet.rows():
                                if not row_data:
                                    continue
                                for cell in row_data:
                                    if (hasattr(cell, 'r') and hasattr(cell, 'c') and
                                        cell.r == target_row and cell.c == target_col):
                                        value = cell.v
                                        # If we got a reasonable value, return it
                                        if (value is not None and 
                                            str(value).strip() and 
                                            str(value) != 'None' and
                                            not self._looks_like_label(str(value))):
                                            return value
                                        break
        except Exception as e:
            print(f"Original mapping failed for {field_name}: {e}")
        
        # Fall back to smart discovery
        print(f"Falling back to smart discovery for {field_name}")
        sheet_data = self.extract_sheet_data(file_path, sheet_name)
        return self.find_data_by_proximity(sheet_data, field_name)

# Test the smart finder
if __name__ == "__main__":
    finder = SmartCellFinder()
    
    file_path = "/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Emparrado UW Model vCurrent.xlsb"
    sheet_name = "Assumptions (Summary)"
    
    # Test problematic fields
    test_fields = [
        ('PROPERTY_NAME', 'D6'),
        ('PROPERTY_CITY', 'D8'), 
        ('UNITS', 'G6'),
        ('YEAR_BUILT', 'D10')
    ]
    
    print("SMART CELL FINDER TEST")
    print("="*40)
    
    for field_name, original_cell in test_fields:
        result = finder.find_field_with_fallback(file_path, sheet_name, original_cell, field_name)
        print(f"{field_name}: {result}")