"""
Generate cell_reference_mappings.json from Excel reference file
This is the missing piece that was causing all NaN values
"""

import os
import sys
import json
from pathlib import Path

# Add project root to path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np

def generate_cell_mappings():
    """Generate cell mappings JSON from Excel reference file"""
    
    # File paths
    reference_file = r"/home/mattb/B&R Programming (WSL)/B&R Capital Dashboard Project/Your Files/Underwriting Dashboard Project - Cell Value References.xlsx"
    output_file = os.path.join(project_root, "data", "cell_reference_mappings.json")
    
    print(f"Reading reference file: {reference_file}")
    
    try:
        # Read the Excel reference file
        # Based on your docs: Column B=Category, C=Description, D=Sheet, G=Cell Address
        df = pd.read_excel(reference_file, sheet_name="UW Model - Cell Reference Table")
        
        print(f"Loaded {len(df)} rows from reference file")
        print(f"Columns: {list(df.columns)}")
        
        # Clean up the data
        df = df.dropna(subset=[df.columns[1], df.columns[2], df.columns[3], df.columns[6]])  # B, C, D, G
        
        print(f"After cleaning: {len(df)} valid mappings")
        
        # Create mappings dictionary
        mappings = {}
        
        for idx, row in df.iterrows():
            try:
                category = str(row.iloc[1]).strip()  # Column B
                description = str(row.iloc[2]).strip()  # Column C  
                sheet_name = str(row.iloc[3]).strip()  # Column D
                cell_address = str(row.iloc[6]).strip()  # Column G
                
                # Clean cell address (remove $ signs if present)
                cell_address = cell_address.replace('$', '')
                
                # Skip if any value is empty or 'nan'
                if any(val in ['nan', '', 'None'] for val in [category, description, sheet_name, cell_address]):
                    continue
                
                mappings[description] = {
                    "category": category,
                    "sheet": sheet_name,
                    "cell": cell_address,
                    "value_type": "auto"  # Will be determined during extraction
                }
                
            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                continue
        
        print(f"Generated {len(mappings)} mappings")
        
        # Show some examples
        print("\nSample mappings:")
        for i, (key, value) in enumerate(list(mappings.items())[:5]):
            print(f"  {key}: {value}")
        
        # Create output directory
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save to JSON file
        with open(output_file, 'w') as f:
            json.dump(mappings, f, indent=2)
        
        print(f"\nMappings saved to: {output_file}")
        
        # Show category breakdown
        categories = {}
        for mapping in mappings.values():
            cat = mapping['category']
            categories[cat] = categories.get(cat, 0) + 1
        
        print("\nMappings by category:")
        for cat, count in sorted(categories.items()):
            print(f"  {cat}: {count} fields")
        
        return mappings
        
    except FileNotFoundError:
        print(f"ERROR: Reference file not found: {reference_file}")
        print("Please ensure the file exists and the path is correct.")
        return None
    except Exception as e:
        print(f"ERROR: Failed to process reference file: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    print("B&R Capital - Generating Cell Reference Mappings")
    print("=" * 50)
    
    mappings = generate_cell_mappings()
    
    if mappings:
        print("\n✓ Cell mappings generated successfully!")
        print("You can now run the extraction tests.")
    else:
        print("\n✗ Failed to generate cell mappings.")
        print("Please check the reference file and try again.")
