"""
Simple test to verify the excel_extraction_system module works
Run this from the src/data_extraction directory to test the module directly
"""

# Test if we can import the module when in the same directory
try:
    from excel_extraction_system import CellMappingParser, ExcelDataExtractor
    print("✓ Successfully imported from excel_extraction_system")
    
    # Try to create instances
    parser = CellMappingParser("dummy_path.xlsx")
    print("✓ CellMappingParser instantiated")
    
    extractor = ExcelDataExtractor({})
    print("✓ ExcelDataExtractor instantiated")
    
    print("\nThe excel_extraction_system module is working correctly!")
    
except ImportError as e:
    print(f"✗ Import error: {e}")
    print("\nMake sure you have installed required packages:")
    print("  pip install pandas openpyxl openpyxl-xlsb numpy structlog")
    
except Exception as e:
    print(f"✗ Other error: {e}")

# Also test if all required packages are available
print("\n" + "="*50)
print("Checking required packages:")
packages = [
    'pandas',
    'numpy', 
    'openpyxl',
    'openpyxl_xlsb',
    'structlog',
    'requests',
    'msal'
]

for package in packages:
    try:
        __import__(package)
        print(f"✓ {package} is installed")
    except ImportError:
        print(f"✗ {package} is NOT installed - run: pip install {package}")