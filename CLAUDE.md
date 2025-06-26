# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

B&R Capital Dashboard is a real estate underwriting data extraction and analysis system. The project extracts ~1,200 data points per Excel file from SharePoint-hosted underwriting models (.xlsb files), processes them through a comprehensive error handling system, and prepares the data for dashboard visualization.

**Current Status**: Phase 2 Complete (Excel Data Extraction System) - Ready for Phase 3 (Database Implementation)

## Core Architecture

### Data Extraction Pipeline
```
SharePoint Discovery → Excel Download → Cell Extraction → Error Handling → Batch Processing → Database Storage
```

**Key Components:**
- `src/data_extraction/excel_extraction_system.py` - Main extraction engine supporting .xlsb and .xlsx files
- `src/data_extraction/error_handling_system.py` - Comprehensive error categorization with NaN handling
- `src/workflows/batch_extraction_processor.py` - Parallel processing of multiple files
- `src/discovery/file_discovery.py` - SharePoint file discovery and metadata collection

### Critical Technical Detail: PyXLSB Indexing

**IMPORTANT**: Excel uses 1-based indexing while pyxlsb uses 0-based indexing. This was a major bug that was fixed:

```python
# CORRECT: Convert Excel coordinates to pyxlsb
target_row = int(excel_row) - 1  # Excel 6 → pyxlsb 5
target_col = excel_col - 1       # Excel 4 → pyxlsb 3
```

See `docs/PYXLSB_INDEXING_GUIDE.md` for complete details. This fix resolved 25/27 critical metrics extraction.

### SharePoint Integration

The system discovers and processes Excel files from SharePoint using Microsoft Graph API:
- **Authentication**: Azure AD app with client ID `5a620cea-31fe-40f6-8b48-d55bc5465dc9`
- **Discovery Pattern**: Scans "Real Estate" document library → "Deals" folder → stage subfolders
- **File Filter**: `*UW Model vCurrent.xlsb` files modified after July 15, 2024
- **Current Dataset**: 41 discovered files totaling 272.4 MB

Authentication tokens expire every 24 hours. Use MCP server for fresh authentication.

## Common Commands

### Testing and Validation
```bash
# Run comprehensive Excel extraction test (validates 1,140 fields)
python3 tests/excel_extraction_test.py

# Test error handling scenarios
python3 test_error_handling.py

# Test batch processing (3 files for testing)
python3 test_batch_extraction.py

# SharePoint discovery and connection
python3 src/discovery/run_discovery.py

# Check Python import paths (WSL environment)
python3 tests/check_paths.py
```

### Data Extraction Workflows
```bash
# Single file extraction (for testing)
python3 -c "
from src.data_extraction.excel_extraction_system import CellMappingParser, ExcelDataExtractor
parser = CellMappingParser('path/to/reference.xlsx')
extractor = ExcelDataExtractor(parser.load_mappings())
result = extractor.extract_from_file('path/to/model.xlsb')
"

# Batch process all discovered files
python3 -m src.workflows.batch_extraction_processor \
  --discovery-file "output/discovered_files_*.json" \
  --reference-file "path/to/reference.xlsx" \
  --max-workers 3

# Complete workflow (discovery + extraction)
python3 complete_extraction_workflow.py
```

### Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Activate conda environment (if using)
conda activate underwriting_dashboard

# Fix Python import issues (WSL-specific)
python3 fix_imports.py
```

## File Locations and Data Flow

### Reference Files
- **Cell Mappings**: `B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx`
- **Test Excel Files**: `B&R Capital Dashboard Project/Your Files/*.xlsb`

### Output Locations
- **Discovery Results**: `output/discovered_files_*.json` (SharePoint file metadata)
- **Extraction Results**: `data/extractions/` (individual file results)
- **Batch Results**: `data/batch_extractions/` (consolidated batch processing)
- **Error Reports**: Auto-generated with comprehensive categorization

### Key Configuration
- **SharePoint Site**: `bandrcapital.sharepoint.com/sites/BRCapital-Internal`
- **Document Library**: "Real Estate" (separate drive, not default Documents)
- **MCP Configuration**: `.mcp.json` for Microsoft 365 integration

## Development Patterns

### Error Handling Philosophy
All extraction errors return `np.nan` with detailed categorization:
- Missing sheets → Find similar sheet names
- Invalid cell addresses → Format validation
- Formula errors → Excel error code interpretation (#REF!, #DIV/0!, etc.)
- Empty values → Standardized null handling

### Cell Mapping Structure
Reference file contains ~1,200 mappings with columns:
- **Category**: Data grouping (e.g., "General Assumptions", "Annual Cashflows")
- **Description**: Field name for database
- **Sheet Name**: Excel worksheet name
- **Cell Address**: Excel coordinate (e.g., "D6", "$G$10")

### Batch Processing Design
- Parallel processing with configurable workers (default: 3)
- Progress tracking with detailed logging
- Graceful error recovery (continues on individual file failures)
- Consolidated output generation (JSON + CSV)

## WSL Environment Specifics

This project runs in Windows Subsystem for Linux (WSL). Key considerations:
- File paths use `/home/mattb/B&R Programming (WSL)/` format
- Python import resolution requires special handling (see `fix_imports.py`)
- MCP server needs WSL-compatible configuration

## Testing Strategy

### Validation Data
The reference file includes a "Value-Check Validation" column for cross-verification. Tests compare extracted values against expected results to ensure accuracy.

### Performance Benchmarks
- **Individual file**: 1,140 fields in ~22 seconds (100% success rate)
- **Batch processing**: 41 files × 1,140 fields = ~45,000 total extractions
- **Error rate**: <1% with comprehensive NaN handling

### Test Files
- `Emparrado UW Model vCurrent.xlsb` - Primary test file with known good data
- `Cimarron UW Model vCurrent.xlsb` - Secondary validation file
- `Broadstone 7th Street UW Model vCurrent.xlsb` - Additional test case

## Phase Development Status

### ✅ Completed Phases
- **Phase 1**: SharePoint Integration (authentication, file discovery)
- **Phase 2**: Excel Data Extraction System
  - ✅ Cell mapping parser for reference file
  - ✅ Excel extractor supporting .xlsb files  
  - ✅ Batch processor for multiple files
  - ✅ Error handling with NaN for missing values

### 🚧 Next Phase
- **Phase 3**: PostgreSQL Database Implementation
  - Database schema design for ~900 metrics per deal
  - Historical version tracking
  - Data loader with version control

## Integration Points

### Microsoft 365 MCP Server
- Location: `@softeria/ms-365-mcp-server`
- Configuration: `.mcp.json` with Azure AD credentials
- Capabilities: SharePoint search, file access, metadata retrieval
- WSL Wrapper: `start-ms365-mcp.sh` handles environment issues

### Future Dashboard Integration
Extraction system outputs are designed for:
- Streamlit dashboard visualization
- PostgreSQL historical storage
- Mobile PWA development
- Real-time monitoring and alerts

## Troubleshooting

### Common Issues
1. **Import Errors**: Run `python3 fix_imports.py` to create missing `__init__.py` files
2. **SharePoint 401 Errors**: Authentication tokens expired - refresh via MCP server
3. **Null Extraction Values**: Check pyxlsb indexing conversion (subtract 1 from Excel coordinates)
4. **Path Not Found**: Verify WSL file path format `/home/mattb/B&R Programming (WSL)/`

### Debug Commands
```bash
# Check extraction with detailed logging
python3 tests/excel_extraction_test.py  # Shows field-by-field results

# Validate cell mappings
python3 tests/diagnose_mappings.py

# Test SharePoint connectivity
python3 tests/test_sharepoint_connection.py

# Analyze error patterns
python3 test_error_handling.py  # Generates comprehensive error report
```

When debugging extraction issues, always check the error categorization in the output metadata - it provides specific guidance for each error type.