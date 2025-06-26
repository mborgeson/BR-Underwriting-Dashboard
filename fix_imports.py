"""
Quick fix script to create all necessary __init__.py files
Run this from your project root to fix import issues
"""

import os
from pathlib import Path

def create_init_files():
    """Create all necessary __init__.py files"""
    
    # Get project root
    project_root = Path.cwd()
    print(f"Project root: {project_root}")
    
    # Define required __init__.py files and their content
    init_files = {
        'src/__init__.py': '"""B&R Capital Dashboard Source Package"""\n',
        
        'src/data_extraction/__init__.py': '''"""
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
''',
        
        'src/discovery/__init__.py': '"""SharePoint file discovery module"""\n',
        'src/config/__init__.py': '"""Configuration module"""\n',
        'src/auth/__init__.py': '"""Authentication module"""\n',
        'tests/__init__.py': '"""Test module"""\n',
    }
    
    # Create each file
    created = 0
    skipped = 0
    
    for file_path, content in init_files.items():
        full_path = project_root / file_path
        
        # Create directory if it doesn't exist
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        if full_path.exists():
            print(f"✓ Already exists: {file_path}")
            skipped += 1
        else:
            full_path.write_text(content, encoding='utf-8')
            print(f"✓ Created: {file_path}")
            created += 1
    
    print(f"\nSummary: Created {created} files, skipped {skipped} existing files")
    
    # Test imports
    print("\nTesting imports...")
    import sys
    sys.path.insert(0, str(project_root))
    
    try:
        import src
        print("✓ Can import 'src'")
    except ImportError as e:
        print(f"✗ Cannot import 'src': {e}")
    
    try:
        from src.data_extraction import CellMappingParser
        print("✓ Can import from src.data_extraction")
    except ImportError as e:
        print(f"✗ Cannot import from src.data_extraction: {e}")
    
    print("\nSetup complete! You can now run:")
    print("  python -m tests.excel_extraction_test")
    print("  python -m tests.test_sharepoint_connection")
    print("\nOr use the run_tests.bat file")


if __name__ == "__main__":
    print("B&R Capital - Import Fix Script")
    print("=" * 40)
    
    # Check if we're in the right directory
    if not Path("src").exists():
        print("ERROR: 'src' folder not found!")
        print("Please run this script from your project root:")
        print("  cd /home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard")
        print("  python fix_imports.py")
    else:
        create_init_files()