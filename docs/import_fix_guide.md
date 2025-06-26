# 🛠️ Import Error Fix Guide

## Quick Fix (Do This First!)

1. **Navigate to your project root**:
   ```bash
   cd "/home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard"
   ```

2. **Run the fix script** (place `fix_imports.py` in project root):
   ```bash
   python fix_imports.py
   ```

3. **Run tests using -m flag**:
   ```bash
   python -m tests.excel_extraction_test
   python -m tests.test_sharepoint_connection
   ```

## Why This Error Happens

Python can't find the `src` module because:
- Python doesn't know where to look for it
- The `tests` folder is at the same level as `src`, not inside it
- Missing `__init__.py` files that mark directories as Python packages

## Complete File Placement

```
/home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard/
│
├── 📄 fix_imports.py              ← Run this first!
├── 📄 run_tests.bat               ← Or use this
├── 📄 complete_extraction_workflow.py
├── 📄 requirements.txt
│
├── 📁 src\
│   ├── 📄 __init__.py            ← Required!
│   │
│   ├── 📁 data_extraction\
│   │   ├── 📄 __init__.py        ← Required!
│   │   ├── 📄 excel_extraction_system.py
│   │   └── 📄 sharepoint_excel_integration.py
│   │
│   ├── 📁 discovery\             ← Your existing files
│   │   ├── 📄 __init__.py
│   │   ├── 📄 file_discovery.py
│   │   └── 📄 run_discovery.py
│   │
│   └── 📁 config\
│       ├── 📄 __init__.py
│       └── 📄 settings.py
│
├── 📁 tests\
│   ├── 📄 __init__.py
│   ├── 📄 check_paths.py         ← Diagnostic tool
│   ├── 📄 excel_extraction_test.py
│   └── 📄 test_sharepoint_connection.py
│
├── 📁 docs\
│   └── 📄 sharepoint_path_summary.md
│
└── 📁 data\
    └── 📁 extractions\           ← Created when you run extractions
```

## Step-by-Step Instructions

### 1. First Time Setup

```bash
# 1. Navigate to project root
cd "/home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard"

# 2. Place all the files in their correct locations (see structure above)

# 3. Run the fix script to create __init__.py files
python fix_imports.py

# 4. Install any missing packages
pip install -r requirements.txt
```

### 2. Running Tests

**Option A: Using -m flag (Recommended)**
```bash
# From project root
python -m tests.excel_extraction_test
python -m tests.test_sharepoint_connection
```

**Option B: Using the batch file**
```bash
# Double-click run_tests.bat or:
run_tests.bat
```

**Option C: After fixing paths**
```bash
# The original command should work after running fix_imports.py
python tests\excel_extraction_test.py
```

### 3. Running the Full Extraction

```bash
# From project root
python complete_extraction_workflow.py
```

## Troubleshooting

If you still get import errors:

1. **Check your location**:
   ```bash
   # Should show: /home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard
   echo %CD%
   ```

2. **Run the diagnostic**:
   ```bash
   python tests\check_paths.py
   ```

3. **Verify __init__.py files exist**:
   ```bash
   dir src\__init__.py
   dir src\data_extraction\__init__.py
   ```

4. **Check your conda environment**:
   ```bash
   conda activate underwriting_dashboard
   python --version  # Should be 3.11
   ```

## Common Issues

| Problem | Solution |
|---------|----------|
| "No module named 'src'" | Run `fix_imports.py` from project root |
| "No module named 'openpyxl_xlsb'" | Run `pip install openpyxl-xlsb` |
| "Cannot find reference file" | Update path in test file to your actual location |
| Tests run but can't find Excel file | Create test file or update path |

## Next Steps After Fixing Imports

1. ✅ Run `test_sharepoint_connection.py` to verify SharePoint access
2. ✅ Run `excel_extraction_test.py` with a sample file
3. ✅ Run `complete_extraction_workflow.py` for full pipeline

Remember: Always run from the project root directory!