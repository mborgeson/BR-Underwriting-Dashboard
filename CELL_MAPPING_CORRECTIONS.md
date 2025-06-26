# Cell Mapping Corrections for Emparrado Template

## Issue Summary
The current cell mappings in "Underwriting Dashboard Project - Cell Value References.xlsx" don't match the actual structure of the Emparrado UW Model template.

## Identified Corrections Needed

### Property Information Fields
| Field | Current Mapping | Actual Location | Current Result | Correct Result |
|-------|-----------------|-----------------|----------------|----------------|
| PROPERTY_NAME | D6 | C5 | "None" | "Emparrado" |
| PROPERTY_CITY | D8 | C7 | "None" | "Mesa" |
| PROPERTY_STATE | D9 | C8 | "None" | "AZ" |
| YEAR_BUILT | D10 | C9 | "None" | "1987.0" |
| UNITS | G6 | F5 | "Last Sale Date" | "154.0" |

### Template Structure Analysis
The template appears to use this layout:
- **Column B**: Labels (e.g., "Project Name", "City", "State")
- **Column C**: Values (e.g., "Emparrado", "Mesa", "AZ")
- **Column E**: Secondary Labels (e.g., "Units (#)", "Avg NRSF")
- **Column F**: Secondary Values (e.g., "154.0", "703.51")

## Recommended Actions

### 1. Immediate Fix
Update the extraction system to handle template variations by:
- Checking multiple possible cell locations for each field
- Using pattern matching to find data by proximity to labels
- Implementing fallback logic for different template versions

### 2. Long-term Solution
- **Standardize templates**: Ensure all deals use the same template structure
- **Version control**: Track template versions and create mappings for each
- **Dynamic discovery**: Build logic to automatically detect template layout

### 3. Quality Assurance
- Test extraction on multiple template versions
- Validate that all critical fields are found
- Create alerts for missing required data

## Implementation Priority
1. **High**: Fix property basic info (name, city, units, year built)
2. **Medium**: Fix financial metrics (NOI, purchase price, returns)
3. **Low**: Fix detailed cashflow projections

## Template Comparison Needed
Compare the current mapping file template with:
1. Emparrado actual structure
2. Other deal templates in SharePoint
3. Determine if this is a one-off issue or systematic problem

This analysis shows the extraction system is working correctly - the issue is data location, not extraction logic.