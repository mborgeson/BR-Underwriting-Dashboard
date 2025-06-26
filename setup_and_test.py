"""
Quick setup and test script for B&R Capital Dashboard
Ensures all directories exist and runs basic tests
"""

import os
import sys
from pathlib import Path

def setup_directories():
    """Create all required directories"""
    directories = [
        "data/extractions",
        "data/raw", 
        "data/processed",
        "logs",
        "output/reports",
        "output/exports",
        "temp"
    ]
    
    print("Setting up directories...")
    for dir_path in directories:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"✓ {dir_path}")
    
    print("\nAll directories created!")

def run_basic_test():
    """Run a basic import test"""
    print("\nTesting imports...")
    try:
        from src.data_extraction import CellMappingParser
        print("✓ Can import CellMappingParser")
        
        from src.data_extraction import ExcelDataExtractor
        print("✓ Can import ExcelDataExtractor")
        
        print("\n✅ System is ready!")
        return True
        
    except ImportError as e:
        print(f"\n❌ Import error: {e}")
        print("\nMake sure you've run:")
        print("  python fix_imports.py")
        print("  pip install pyxlsb pandas numpy openpyxl structlog")
        return False

def main():
    print("B&R Capital Dashboard - Setup & Test")
    print("=" * 50)
    
    # Check current directory
    if not Path("src").exists():
        print("ERROR: Not in project root directory!")
        print("Please run from:")
        print("  cd /home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard")
        return
    
    # Setup directories
    setup_directories()
    
    # Test imports
    if run_basic_test():
        print("\nYou can now run:")
        print("  python -m tests.excel_extraction_test")
        print("  python -m tests.test_sharepoint_connection")
    else:
        print("\nPlease fix import issues before running tests.")

if __name__ == "__main__":
    main()