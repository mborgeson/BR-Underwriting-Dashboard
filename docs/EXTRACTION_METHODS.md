# Excel Data Extraction Methods

## Current Method: Download & Extract

The current implementation downloads Excel files locally before extraction because:
- **pyxlsb library limitation**: Requires file-like objects or local file paths
- **Excel complexity**: .xlsb files are binary format requiring specialized parsing
- **Performance**: Local processing is faster for 1,140 fields per file

## Alternative Methods (Without Downloading)

### 1. **Microsoft Graph API Excel REST API** (Recommended)
```python
# Direct cell value reading via API
GET https://graph.microsoft.com/v1.0/sites/{site-id}/drive/items/{item-id}/workbook/worksheets/{name}/range(address='A1:Z100')
```

**Pros:**
- No file downloads required
- Direct cell access
- Supports authentication/permissions

**Cons:**
- Requires .xlsx format (not .xlsb)
- API rate limits
- More complex error handling

### 2. **SharePoint REST API with Excel Services**
```python
# Read Excel data via SharePoint Excel Services
GET https://bandrcapital.sharepoint.com/sites/BRCapital-Internal/_api/web/GetFileByServerRelativeUrl('/path/to/file.xlsx')/ExcelRest/Range('Sheet1!A1:Z100')
```

**Pros:**
- Native SharePoint integration
- No downloads needed

**Cons:**
- Limited to .xlsx files
- Requires Excel Services enabled
- Complex authentication

### 3. **Power Automate Flow Integration**
Create automated flows to:
1. Monitor Excel files for changes
2. Extract specific cell values
3. Push data to database via webhook

**Pros:**
- No code maintenance
- Automatic triggers
- Built-in error handling

**Cons:**
- Requires Power Automate license
- Less flexible for complex extractions

### 4. **Stream Processing with Memory Buffer**
```python
# Stream file content without saving to disk
response = requests.get(download_url, stream=True)
with io.BytesIO() as buffer:
    for chunk in response.iter_content(chunk_size=8192):
        buffer.write(chunk)
    buffer.seek(0)
    # Process with pyxlsb from memory
    with open_workbook(buffer) as wb:
        # Extract data
```

**Pros:**
- No disk storage
- Works with current code
- Supports .xlsb format

**Cons:**
- Still downloads to memory
- Memory usage for large files

## ✅ IMPLEMENTED: Graph API File Streaming (Recommended)

**Current Implementation** (Option 4 Enhanced):
- Uses Graph API for file access with fresh authentication
- Streams .xlsb files directly to memory buffers
- Processes with existing pyxlsb logic
- No permanent downloads required
- Real-time delta monitoring for changes

### Implementation Files:
- `src/extraction/graph_api_extractor.py` - Graph API streaming extractor
- `src/monitoring/delta_monitor.py` - Real-time change detection
- `src/monitoring/alerting.py` - Notification system
- `complete_realtime_workflow.py` - Main orchestrator

### Usage:
```bash
# Complete workflow: initial extraction + real-time monitoring
python complete_realtime_workflow.py \
    --client-id "5a620cea-31fe-40f6-8b48-d55bc5465dc9" \
    --client-secret "your-secret" \
    --reference-file "/path/to/reference.xlsx"

# Monitoring only (skip initial extraction)
python complete_realtime_workflow.py \
    --client-id "5a620cea-31fe-40f6-8b48-d55bc5465dc9" \
    --client-secret "your-secret" \
    --reference-file "/path/to/reference.xlsx" \
    --skip-initial
```

### Benefits Achieved:
✅ **No file downloads** - Everything streams through memory  
✅ **Real-time monitoring** - 30-second delta query checks  
✅ **Automatic token refresh** - No authentication expiry issues  
✅ **Change detection** - Only processes modified/new files  
✅ **Version tracking** - Full re-extraction on any file change  
✅ **Comprehensive alerting** - Real-time notifications for all events  

### Future Options:
1. **Graph API Excel REST** (when .xlsb support added)
   - Direct cell access without streaming
   - Requires Microsoft to add .xlsb support

2. **Power Automate Integration**
   - Automated flows for file monitoring
   - Push notifications to webhook endpoints