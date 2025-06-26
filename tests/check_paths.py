"""
Diagnostic script to check Python paths and module availability
Run this to troubleshoot import issues
"""

import os
import sys
from pathlib import Path

print("=" * 60)
print("PYTHON PATH DIAGNOSTIC")
print("=" * 60)

# Current location
current_file = Path(__file__).resolve()
print(f"\n1. Current script location:")
print(f"   {current_file}")

# Project root
project_root = current_file.parent.parent
print(f"\n2. Project root:")
print(f"   {project_root}")

# Python path
print(f"\n3. Current Python path:")
for i, path in enumerate(sys.path[:5]):  # Show first 5
    print(f"   [{i}] {path}")

# Check if src is accessible
print(f"\n4. Checking 'src' module accessibility:")
src_path = project_root / 'src'
if src_path.exists():
    print(f"   ✓ src folder exists at: {src_path}")
    
    # Check for __init__.py
    init_file = src_path / '__init__.py'
    if init_file.exists():
        print(f"   ✓ src/__init__.py exists")
    else:
        print(f"   ✗ src/__init__.py NOT FOUND - creating it...")
        init_file.write_text("")
        print(f"   ✓ Created empty src/__init__.py")
else:
    print(f"   ✗ src folder NOT FOUND at: {src_path}")

# Check data_extraction module
print(f"\n5. Checking 'data_extraction' module:")
data_extraction_path = src_path / 'data_extraction'
if data_extraction_path.exists():
    print(f"   ✓ data_extraction folder exists")
    
    # Check for __init__.py
    init_file = data_extraction_path / '__init__.py'
    if init_file.exists():
        print(f"   ✓ data_extraction/__init__.py exists")
    else:
        print(f"   ✗ data_extraction/__init__.py NOT FOUND")
    
    # Check for main files
    for filename in ['excel_extraction_system.py', 'sharepoint_excel_integration.py']:
        file_path = data_extraction_path / filename
        if file_path.exists():
            print(f"   ✓ {filename} exists")
        else:
            print(f"   ✗ {filename} NOT FOUND")
else:
    print(f"   ✗ data_extraction folder NOT FOUND")

# Try imports
print(f"\n6. Testing imports:")

# Add project root to path
sys.path.insert(0, str(project_root))
print(f"   Added project root to Python path")

try:
    import src
    print(f"   ✓ Can import 'src'")
except ImportError as e:
    print(f"   ✗ Cannot import 'src': {e}")

try:
    import src.data_extraction
    print(f"   ✓ Can import 'src.data_extraction'")
except ImportError as e:
    print(f"   ✗ Cannot import 'src.data_extraction': {e}")

try:
    from src.data_extraction.excel_extraction_system import CellMappingParser
    print(f"   ✓ Can import CellMappingParser from excel_extraction_system")
except ImportError as e:
    print(f"   ✗ Cannot import from excel_extraction_system: {e}")

# Recommendations
print(f"\n7. Recommendations:")
print(f"   - Always run scripts from project root: {project_root}")
print(f"   - Or use: python -m tests.excel_extraction_test")
print(f"   - Ensure all __init__.py files exist in src and subdirectories")

print("\n" + "=" * 60)