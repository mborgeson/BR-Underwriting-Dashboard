#!/usr/bin/env python3
"""
Database Schema Fix Script
Fixes identified issues with column sizes and SQL parameter mismatches
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import structlog
from src.database.connection import get_cursor, get_connection

logger = structlog.get_logger().bind(component="SchemaFix")

def fix_column_sizes():
    """Fix VARCHAR column size issues"""
    logger.info("fixing_column_sizes")
    
    with get_cursor() as cursor:
        # Drop views that depend on the columns we need to modify
        logger.info("dropping_dependent_views")
        cursor.execute("DROP VIEW IF EXISTS latest_underwriting_data CASCADE;")
        
        # Fix property_state column size (main issue)
        logger.info("expanding_property_state_column")
        cursor.execute("""
            ALTER TABLE properties 
            ALTER COLUMN property_state TYPE VARCHAR(50);
        """)
        
        # Expand other potentially problematic text columns
        logger.info("expanding_other_text_columns")
        cursor.execute("""
            ALTER TABLE properties 
            ALTER COLUMN property_city TYPE VARCHAR(100),
            ALTER COLUMN market TYPE VARCHAR(100),
            ALTER COLUMN submarket TYPE VARCHAR(100),
            ALTER COLUMN county TYPE VARCHAR(100);
        """)
        
        # Fix underwriting_data text columns
        cursor.execute("""
            ALTER TABLE underwriting_data 
            ALTER COLUMN location_quality TYPE VARCHAR(50),
            ALTER COLUMN building_quality TYPE VARCHAR(50),
            ALTER COLUMN building_type TYPE VARCHAR(100),
            ALTER COLUMN project_type TYPE VARCHAR(100),
            ALTER COLUMN individually_metered TYPE VARCHAR(50),
            ALTER COLUMN building_zoning TYPE VARCHAR(100),
            ALTER COLUMN property_latitude TYPE VARCHAR(50),
            ALTER COLUMN property_longitude TYPE VARCHAR(50),
            ALTER COLUMN property_zip TYPE VARCHAR(20);
        """)
        
        logger.info("column_sizes_fixed_successfully")

def count_insert_parameters():
    """Count parameters in current INSERT statement"""
    # This is the current INSERT from data_loader.py
    insert_columns = [
        'extraction_id', 'property_id', 'property_name', 'deal_stage',
        'file_path', 'extraction_timestamp', 'file_modified_date', 'file_size_mb',
        
        # General Assumptions
        'year_built', 'year_renovated', 'location_quality', 'building_quality',
        'units', 'avg_square_feet', 'parking_spaces_covered', 'parking_spaces_uncovered',
        'individually_metered', 'current_owner', 'last_sale_date', 'last_sale_price',
        'last_sale_price_per_unit', 'last_sale_cap_rate', 'building_height',
        'building_type', 'project_type', 'number_of_buildings', 'building_zoning',
        'land_area', 'parcel_number', 'property_latitude', 'property_longitude',
        'property_address_field', 'property_zip',
        
        # Exit Assumptions
        'exit_period_months', 'exit_cap_rate', 'sales_transaction_costs',
        
        # NOI Assumptions
        'empirical_rent', 'rent_psf', 'gross_potential_rental_income',
        'concessions', 'loss_to_lease', 'vacancy_loss', 'bad_debts', 'other_loss',
        'property_management_fee', 'net_rental_income', 'parking_income',
        'laundry_income', 'other_income', 'effective_gross_income',
        
        # Operating Expenses
        'advertising_marketing', 'management_fee', 'payroll', 'repairs_maintenance',
        'contract_services', 'turnover', 'utilities', 'insurance', 'real_estate_taxes',
        'other_expenses', 'total_operating_expenses', 'net_operating_income',
        
        # Debt and Equity
        'purchase_price', 'hard_costs_budget', 'soft_costs_budget',
        'total_hard_costs', 'total_soft_costs', 'total_acquisition_budget',
        'loan_amount', 'loan_to_cost', 'loan_to_value',
        'equity_lp_capital', 'equity_gp_capital',
        
        # Return Metrics
        't12_return_on_pp', 't12_return_on_cost', 'levered_returns_irr',
        'levered_returns_moic', 'basis_unit_at_close', 'basis_unit_at_exit'
    ]
    
    field_mapping = [
        # General Assumptions
        'YEAR_BUILT', 'YEAR_RENOVATED', 'LOCATION_QUALITY', 'BUILDING_QUALITY',
        'UNITS', 'AVG_SQUARE_FEET', 'NUMBER_OF_PARKING_SPACES_COVERED', 
        'NUMBER_OF_PARKING_SPACES_UNCOVERED', 'INDIVIDUALLY_METERED', 'CURRENT_OWNER',
        'LAST_SALE_DATE', 'LAST_SALE_PRICE', 'LAST_SALE_PRICE_PER_UNIT',
        'LAST_SALE_CAP_RATE', 'BUILDING_HEIGHT', 'BUILDING_TYPE', 'PROJECT_TYPE',
        'NUMBER_OF_BUILDINGS', 'BUILDING_ZONING', 'LAND_AREA', 'PARCEL_NUMBER',
        'PROPERTY_LATITUDE', 'PROPERTY_LONGITUDE', 'PROPERTY_ADDRESS', 'PROPERTY_ZIP',
        
        # Exit Assumptions
        'EXIT_PERIOD_MONTHS', 'EXIT_CAP_RATE', 'SALES_TRANSACTION_COSTS',
        
        # NOI Assumptions
        'EMPIRICAL_RENT', 'RENT_PSF', 'GROSS_POTENTIAL_RENTAL_INCOME',
        'CONCESSIONS', 'LOSS_TO_LEASE', 'VACANCY_LOSS', 'BAD_DEBTS', 'OTHER_LOSS',
        'PROPERTY_MANAGEMENT_FEE', 'NET_RENTAL_INCOME', 'PARKING_INCOME',
        'LAUNDRY_INCOME', 'OTHER_INCOME', 'EFFECTIVE_GROSS_INCOME',
        
        # Operating Expenses
        'ADVERTISING_MARKETING', 'MANAGEMENT_FEE', 'PAYROLL', 'REPAIRS_MAINTENANCE',
        'CONTRACT_SERVICES', 'TURNOVER', 'UTILITIES', 'INSURANCE', 'REAL_ESTATE_TAXES',
        'OTHER_EXPENSES', 'TOTAL_OPERATING_EXPENSES', 'NET_OPERATING_INCOME',
        
        # Debt and Equity
        'PURCHASE_PRICE', 'HARD_COSTS_BUDGET', 'SOFT_COSTS_BUDGET',
        'TOTAL_HARD_COSTS', 'TOTAL_SOFT_COSTS', 'TOTAL_ACQUISITION_BUDGET',
        'LOAN_AMOUNT', 'LOAN_TO_COST', 'LOAN_TO_VALUE',
        'EQUITY_LP_CAPITAL', 'EQUITY_GP_CAPITAL',
        
        # Return Metrics
        'T12_RETURN_ON_PP', 'T12_RETURN_ON_COST', 'LEVERED_RETURNS_IRR',
        'LEVERED_RETURNS_MOIC', 'BASIS_UNIT_AT_CLOSE', 'BASIS_UNIT_AT_EXIT'
    ]
    
    # Metadata fields (first 8 parameters)
    metadata_fields = 8
    
    print(f"INSERT columns: {len(insert_columns)} total")
    print(f"  - Metadata fields: {metadata_fields}")
    print(f"  - Data fields: {len(insert_columns) - metadata_fields}")
    print(f"Field mapping: {len(field_mapping)} fields")
    print(f"Mismatch: {len(insert_columns) - metadata_fields - len(field_mapping)} fields")
    
    # Find missing fields
    data_columns = insert_columns[metadata_fields:]  # Skip metadata
    print(f"\nMissing from field_mapping:")
    for i, col in enumerate(data_columns):
        if i >= len(field_mapping):
            print(f"  - {col}")
    
    return len(insert_columns), len(field_mapping) + metadata_fields

def create_parameter_fix():
    """Generate the corrected field mapping"""
    logger.info("creating_parameter_fix")
    
    # The complete field mapping that matches the INSERT statement
    corrected_field_mapping = [
        # General Assumptions (24 fields)
        'YEAR_BUILT', 'YEAR_RENOVATED', 'LOCATION_QUALITY', 'BUILDING_QUALITY',
        'UNITS', 'AVG_SQUARE_FEET', 'NUMBER_OF_PARKING_SPACES_COVERED', 
        'NUMBER_OF_PARKING_SPACES_UNCOVERED', 'INDIVIDUALLY_METERED', 'CURRENT_OWNER',
        'LAST_SALE_DATE', 'LAST_SALE_PRICE', 'LAST_SALE_PRICE_PER_UNIT',
        'LAST_SALE_CAP_RATE', 'BUILDING_HEIGHT', 'BUILDING_TYPE', 'PROJECT_TYPE',
        'NUMBER_OF_BUILDINGS', 'BUILDING_ZONING', 'LAND_AREA', 'PARCEL_NUMBER',
        'PROPERTY_LATITUDE', 'PROPERTY_LONGITUDE', 'PROPERTY_ADDRESS', 'PROPERTY_ZIP',
        
        # Exit Assumptions (3 fields)
        'EXIT_PERIOD_MONTHS', 'EXIT_CAP_RATE', 'SALES_TRANSACTION_COSTS',
        
        # NOI Assumptions (14 fields)
        'EMPIRICAL_RENT', 'RENT_PSF', 'GROSS_POTENTIAL_RENTAL_INCOME',
        'CONCESSIONS', 'LOSS_TO_LEASE', 'VACANCY_LOSS', 'BAD_DEBTS', 'OTHER_LOSS',
        'PROPERTY_MANAGEMENT_FEE', 'NET_RENTAL_INCOME', 'PARKING_INCOME',
        'LAUNDRY_INCOME', 'OTHER_INCOME', 'EFFECTIVE_GROSS_INCOME',
        
        # Operating Expenses (12 fields)
        'ADVERTISING_MARKETING', 'MANAGEMENT_FEE', 'PAYROLL', 'REPAIRS_MAINTENANCE',
        'CONTRACT_SERVICES', 'TURNOVER', 'UTILITIES', 'INSURANCE', 'REAL_ESTATE_TAXES',
        'OTHER_EXPENSES', 'TOTAL_OPERATING_EXPENSES', 'NET_OPERATING_INCOME',
        
        # Debt and Equity (11 fields)
        'PURCHASE_PRICE', 'HARD_COSTS_BUDGET', 'SOFT_COSTS_BUDGET',
        'TOTAL_HARD_COSTS', 'TOTAL_SOFT_COSTS', 'TOTAL_ACQUISITION_BUDGET',
        'LOAN_AMOUNT', 'LOAN_TO_COST', 'LOAN_TO_VALUE',
        'EQUITY_LP_CAPITAL', 'EQUITY_GP_CAPITAL',
        
        # Return Metrics (6 fields)
        'T12_RETURN_ON_PP', 'T12_RETURN_ON_COST', 'LEVERED_RETURNS_IRR',
        'LEVERED_RETURNS_MOIC', 'BASIS_UNIT_AT_CLOSE', 'BASIS_UNIT_AT_EXIT'
    ]
    
    print(f"Corrected field mapping: {len(corrected_field_mapping)} fields")
    
    # Write the corrected mapping to a file for easy copying
    with open("/tmp/corrected_field_mapping.py", "w") as f:
        f.write("# Corrected field_mapping for data_loader.py\n")
        f.write("field_mapping = [\n")
        
        current_section = ""
        for field in corrected_field_mapping:
            # Add section comments
            if field == 'YEAR_BUILT':
                f.write("    # General Assumptions\n")
            elif field == 'EXIT_PERIOD_MONTHS':
                f.write("    \n    # Exit Assumptions\n")
            elif field == 'EMPIRICAL_RENT':
                f.write("    \n    # NOI Assumptions\n")
            elif field == 'ADVERTISING_MARKETING':
                f.write("    \n    # Operating Expenses\n")
            elif field == 'PURCHASE_PRICE':
                f.write("    \n    # Debt and Equity\n")
            elif field == 'T12_RETURN_ON_PP':
                f.write("    \n    # Return Metrics\n")
            
            f.write(f"    '{field}',\n")
        
        f.write("]\n")
    
    print("Corrected field mapping written to /tmp/corrected_field_mapping.py")
    return corrected_field_mapping

def main():
    """Main function to run all fixes"""
    print("🔧 B&R Capital Database Schema Fix")
    print("=" * 50)
    
    try:
        # 1. Analyze parameter mismatch
        print("\n1. Analyzing SQL parameter mismatch...")
        insert_count, mapping_count = count_insert_parameters()
        
        # 2. Create corrected field mapping
        print("\n2. Creating corrected field mapping...")
        corrected_mapping = create_parameter_fix()
        
        # 3. Fix column sizes
        print("\n3. Fixing database column sizes...")
        fix_column_sizes()
        
        print("\n✅ Schema fixes completed successfully!")
        print("\nNext steps:")
        print("1. Update field_mapping in data_loader.py with corrected version")
        print("2. Test the database loading with sample data")
        print("3. Run the complete workflow again")
        
        return 0
        
    except Exception as e:
        logger.error("schema_fix_failed", error=str(e))
        print(f"❌ Schema fix failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())