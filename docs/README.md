# B&R Capital Dashboard - Documentation Index

## Project Overview
This directory contains technical documentation for the B&R Capital Dashboard project, focusing on Excel data extraction, SharePoint integration, and system architecture.

## Documentation Files

### 📊 **Data Extraction**
- **[PYXLSB_INDEXING_GUIDE.md](./PYXLSB_INDEXING_GUIDE.md)** - Critical indexing differences between Excel and pyxlsb library
- **[CELL_MAPPING_CORRECTIONS.md](../CELL_MAPPING_CORRECTIONS.md)** - Template structure analysis and mapping fixes

### 🔧 **System Setup**
- **[MCP_SETUP.md](../MCP_SETUP.md)** - Microsoft 365 MCP Server configuration guide
- **[import_fix_guide.md](./import_fix_guide.md)** - Python import path resolution

### 📈 **Project Status**
- **[PROJECT_UPDATE_SUMMARY.md](../PROJECT_UPDATE_SUMMARY.md)** - Comprehensive project progress report

### 🔍 **SharePoint Integration**
- **[sharepoint_path_summary.md](./sharepoint_path_summary.md)** - SharePoint file structure documentation

## Key Technical Issues Resolved

### 1. **PyXLSB Indexing (CRITICAL)**
**Issue**: Cell mappings returning wrong values due to indexing mismatch  
**Solution**: Convert Excel 1-based coordinates to pyxlsb 0-based  
**Impact**: Fixed 25/27 critical metrics extraction  
**Reference**: [PYXLSB_INDEXING_GUIDE.md](./PYXLSB_INDEXING_GUIDE.md)

### 2. **Path Migration**
**Issue**: Windows paths incompatible with WSL environment  
**Solution**: Updated 16 files with correct WSL paths  
**Impact**: Eliminated file-not-found errors

### 3. **SharePoint Authentication**
**Issue**: MCP server library conflicts in WSL  
**Solution**: Custom configuration with npx wrapper  
**Impact**: Successful discovery of 41 Excel files

## Quick References

### Cell Address Conversion
```python
# Excel D6 → pyxlsb coordinates
excel_row = 6
excel_col = 4
pyxlsb_row = excel_row - 1  # = 5
pyxlsb_col = excel_col - 1  # = 3
```

### Key File Locations
```
├── src/data_extraction/excel_extraction_system.py  # Main extraction logic
├── tests/excel_extraction_test.py                  # Validation tests  
├── output/discovered_files_*.json                  # SharePoint discovery results
└── data/extractions/test_extraction_*.json         # Sample extraction output
```

### Performance Metrics
- **Files discovered**: 41 Excel files (272.4 MB)
- **Extraction rate**: 1,140 fields in 22 seconds
- **Success rate**: 100% field extraction
- **Critical metrics**: 25/27 successfully extracted

## Development Guidelines

### 1. **Always Use 0-Based Indexing for pyxlsb**
```python
# WRONG
if cell.r == excel_row and cell.c == excel_col:

# CORRECT  
if cell.r == (excel_row - 1) and cell.c == (excel_col - 1):
```

### 2. **Document Coordinate Systems**
- Excel uses 1-based indexing (A1 = row 1, col 1)
- pyxlsb uses 0-based indexing (A1 = row 0, col 0)
- Always comment coordinate conversions

### 3. **Test with Validation Data**
- Use the "Value-Check Validation" column in reference files
- Compare extracted values against expected results
- Log coordinate transformations for debugging

## Troubleshooting

### Common Issues
1. **Null values in extraction** → Check indexing conversion
2. **Wrong data extracted** → Verify cell address mapping
3. **File not found errors** → Update file paths for WSL
4. **SharePoint authentication** → Check MCP server configuration

### Debug Commands
```bash
# Test extraction
python3 tests/excel_extraction_test.py

# Check SharePoint discovery
python3 src/discovery/run_discovery.py

# Validate cell mappings
python3 tests/diagnose_mappings.py
```

## Future Enhancements

### Priority 1: Production Deployment
- [ ] Refresh SharePoint authentication tokens
- [ ] Process all 41 discovered files
- [ ] Build React dashboard interface

### Priority 2: System Improvements
- [ ] Implement incremental file processing
- [ ] Add real-time SharePoint monitoring
- [ ] Create automated validation checks

### Priority 3: Advanced Features
- [ ] Predictive analytics on deal data
- [ ] Automated report generation
- [ ] Integration with additional data sources

---

**Last Updated**: 2025-06-26  
**Project Status**: Production-ready extraction system  
**Next Sprint**: Dashboard UI development