#!/usr/bin/env python3
"""
Fix the pyxlsb indexing issue in the extraction system
"""

import pyxlsb
import pandas as pd
import re

def test_pyxlsb_indexing():
    """Test to understand pyxlsb's exact indexing scheme"""
    
    file_path = "/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Emparrado UW Model vCurrent.xlsb"
    ref_file = "/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx"
    
    # Read the validated mappings
    df = pd.read_excel(ref_file)
    
    print("PYXLSB INDEXING INVESTIGATION")
    print("="*50)
    
    # Test specific validated mappings
    test_cases = [
        ('PROPERTY_NAME', 'Emparrado'),
        ('PROPERTY_CITY', 'Mesa'), 
        ('UNITS', '154'),
        ('YEAR_BUILT', '1987')
    ]
    
    with pyxlsb.open_workbook(file_path) as wb:
        with wb.get_sheet("Assumptions (Summary)") as sheet:
            
            for field_name, expected_value in test_cases:
                # Get mapping from reference file
                matches = df[df['Cell Description'] == field_name]
                if matches.empty:
                    continue
                    
                row_data = matches.iloc[0]
                cell_address = row_data['Cell Address']
                validation_value = row_data['Value-Check Validation']
                
                print(f"\n{field_name} ({cell_address}):")
                print(f"  Validation shows: {validation_value}")
                print(f"  Expected: {expected_value}")
                
                # Parse Excel address
                match = re.match(r'([A-Z]+)(\d+)', cell_address.replace('$', ''))
                if match:
                    col_str, row_str = match.groups()
                    excel_row = int(row_str)
                    excel_col = ord(col_str) - ord('A') + 1
                    
                    print(f"  Excel address {cell_address} = Row {excel_row}, Col {excel_col}")
                    
                    # Try different indexing schemes
                    search_patterns = [
                        (excel_row, excel_col, "1-based as-is"),
                        (excel_row - 1, excel_col - 1, "0-based conversion"),
                        (excel_row + 1, excel_col + 1, "1-based +1 offset"),
                        (excel_row - 1, excel_col, "row 0-based, col 1-based"),
                        (excel_row, excel_col - 1, "row 1-based, col 0-based")
                    ]
                    
                    found_matches = []
                    
                    # Search all cells and find matches
                    for row_data_iter in sheet.rows():
                        if not row_data_iter:
                            continue
                        for cell in row_data_iter:
                            if hasattr(cell, 'r') and hasattr(cell, 'c') and cell.v is not None:
                                cell_value_str = str(cell.v).strip()
                                if cell_value_str == str(expected_value):
                                    found_matches.append((cell.r, cell.c, cell.v))
                    
                    print(f"  Found '{expected_value}' at pyxlsb coordinates:")
                    for r, c, v in found_matches:
                        print(f"    ({r}, {c})")
                        
                        # Check which scheme this matches
                        for test_r, test_c, scheme in search_patterns:
                            if r == test_r and c == test_c:
                                print(f"      ✅ Matches {scheme}")
                    
                    # Also try the exact coordinates we calculated
                    found_at_calculated = None
                    for row_data_iter in sheet.rows():
                        if not row_data_iter:
                            continue
                        for cell in row_data_iter:
                            if hasattr(cell, 'r') and hasattr(cell, 'c'):
                                if cell.r == excel_row and cell.c == excel_col:
                                    found_at_calculated = cell.v
                                    break
                        if found_at_calculated is not None:
                            break
                    
                    print(f"  At calculated coordinates ({excel_row}, {excel_col}): {found_at_calculated}")

if __name__ == "__main__":
    test_pyxlsb_indexing()