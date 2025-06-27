"""
Expanded Data Loader for Complete 1,140 Field Storage
Handles all extracted fields with proper categorization and storage
"""

import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
import structlog
import pandas as pd
from .connection import get_cursor
from .expanded_schema import ExpandedSchemaManager

logger = structlog.get_logger().bind(component="ExpandedDataLoader")

class ExpandedDataLoader:
    """Loads all 1,140 extracted fields into categorized database tables"""
    
    def __init__(self):
        self.schema_manager = ExpandedSchemaManager()
        
        # Define field mappings for each category
        self._init_field_mappings()
    
    def _init_field_mappings(self):
        """Initialize field mappings for each table"""
        
        # Property Information Fields (47 fields)
        self.property_fields = {
            'PROPERTY_NAME': 'property_name',
            'PROPERTY_CITY': 'property_city', 
            'PROPERTY_STATE': 'property_state',
            'PROPERTY_ADDRESS': 'property_address',
            'PROPERTY_ZIP': 'property_zip',
            'PROPERTY_LATITUDE': 'property_latitude',
            'PROPERTY_LONGITUDE': 'property_longitude',
            'YEAR_BUILT': 'year_built',
            'YEAR_RENOVATED': 'year_renovated',
            'BUILDING_TYPE': 'building_type',
            'BUILDING_QUALITY': 'building_quality',
            'LOCATION_QUALITY': 'location_quality',
            'BUILDING_HEIGHT': 'building_height',
            'NUMBER_OF_BUILDINGS': 'number_of_buildings',
            'BUILDING_ZONING': 'building_zoning',
            'LAND_AREA': 'land_area',
            'PARCEL_NUMBER': 'parcel_number',
            'UNITS': 'units',
            'AVG_SQUARE_FEET': 'avg_square_feet',
            'NUMBER_OF_PARKING_SPACES_COVERED': 'parking_spaces_covered',
            'NUMBER_OF_PARKING_SPACES_UNCOVERED': 'parking_spaces_uncovered',
            'INDIVIDUALLY_METERED': 'individually_metered',
            'MARKET': 'market',
            'SUBMARKET': 'submarket',
            'COUNTY': 'county',
            'CURRENT_OWNER': 'current_owner',
            'LAST_SALE_DATE': 'last_sale_date',
            'LAST_SALE_PRICE': 'last_sale_price',
            'LAST_SALE_PRICE_PER_UNIT': 'last_sale_price_per_unit',
            'LAST_SALE_CAP_RATE': 'last_sale_cap_rate',
            'PROJECT_TYPE': 'project_type'
        }
        
        # Unit Mix Fields (52 fields) - Pattern-based extraction
        self.unit_mix_patterns = ['1_BED_', '2_BED_', '3_BED_', '4_BED_']
        
        # Income Fields (108 fields)
        self.income_fields = {
            'GROSS_POTENTIAL_RENTAL_INCOME': 'gross_potential_rental_income',
            'NET_RENTAL_INCOME': 'net_rental_income', 
            'EFFECTIVE_GROSS_INCOME': 'effective_gross_income',
            'CONCESSIONS': 'concessions',
            'LOSS_TO_LEASE': 'loss_to_lease',
            'VACANCY_LOSS': 'vacancy_loss',
            'BAD_DEBTS': 'bad_debts',
            'OTHER_LOSS': 'other_loss',
            'PARKING_INCOME': 'parking_income',
            'LAUNDRY_INCOME': 'laundry_income',
            'OTHER_INCOME': 'other_income',
            'EMPIRICAL_RENT': 'empirical_rent',
            'RENT_PSF': 'rent_per_sf'
        }
        
        # Operating Expenses Fields (83 fields)
        self.expense_fields = {
            'ADVERTISING_MARKETING': 'advertising_marketing',
            'MANAGEMENT_FEE': 'management_fee',
            'PAYROLL': 'payroll',
            'REPAIRS_MAINTENANCE': 'repairs_maintenance',
            'CONTRACT_SERVICES': 'contract_services',
            'TURNOVER': 'turnover',
            'UTILITIES': 'utilities',
            'INSURANCE': 'insurance',
            'REAL_ESTATE_TAXES': 'real_estate_taxes',
            'OTHER_EXPENSES': 'other_expenses',
            'TOTAL_OPERATING_EXPENSES': 'total_operating_expenses'
        }
        
        # Financing Fields (68 fields)
        self.financing_fields = {
            'LOAN_AMOUNT': 'loan_amount',
            'LOAN_TO_COST': 'loan_to_cost',
            'LOAN_TO_VALUE': 'loan_to_value',
            'EQUITY_LP_CAPITAL': 'equity_lp_capital',
            'EQUITY_GP_CAPITAL': 'equity_gp_capital'
        }
        
        # Returns Fields (19 fields)
        self.returns_fields = {
            'LEVERED_RETURNS_IRR': 'levered_returns_irr',
            'LEVERED_RETURNS_MOIC': 'levered_returns_moic',
            'T12_RETURN_ON_PP': 't12_return_on_pp',
            'T12_RETURN_ON_COST': 't12_return_on_cost',
            'BASIS_UNIT_AT_CLOSE': 'basis_unit_at_close',
            'BASIS_UNIT_AT_EXIT': 'basis_unit_at_exit',
            'EXIT_PERIOD_MONTHS': 'exit_period_months',
            'EXIT_CAP_RATE': 'exit_cap_rate',
            'SALES_TRANSACTION_COSTS': 'sales_transaction_costs'
        }
    
    def load_complete_extraction_data(self, extraction_data: Dict[str, Any], 
                                    deal_stage: str, metadata: Optional[Dict] = None) -> str:
        """
        Load all 1,140 fields into appropriate database tables
        
        Args:
            extraction_data: The complete extracted data dictionary (1,140 fields)
            deal_stage: The deal stage 
            metadata: Optional metadata about the extraction
            
        Returns:
            extraction_id: UUID of the created extraction record
        """
        try:
            extraction_id = str(uuid.uuid4())
            
            with get_cursor() as cursor:
                logger.info("loading_complete_extraction_data", 
                           extraction_id=extraction_id,
                           total_fields=len(extraction_data))
                
                # 1. Load property information
                property_id = self._load_property_data(cursor, extraction_id, extraction_data)
                
                # 2. Load unit mix data
                self._load_unit_mix_data(cursor, extraction_id, property_id, extraction_data)
                
                # 3. Load comparables data  
                self._load_comparables_data(cursor, extraction_id, property_id, extraction_data)
                
                # 4. Load projections data
                self._load_projections_data(cursor, extraction_id, property_id, extraction_data)
                
                # 5. Load financing data
                self._load_financing_data(cursor, extraction_id, property_id, extraction_data)
                
                # 6. Load returns data
                self._load_returns_data(cursor, extraction_id, property_id, extraction_data)
                
                # 7. Load operating expenses
                self._load_expenses_data(cursor, extraction_id, property_id, extraction_data)
                
                # 8. Load income data
                self._load_income_data(cursor, extraction_id, property_id, extraction_data)
                
                # 9. Load miscellaneous data (all remaining fields)
                self._load_miscellaneous_data(cursor, extraction_id, property_id, extraction_data)
                
                logger.info("complete_extraction_data_loaded_successfully",
                           extraction_id=extraction_id,
                           property_id=property_id)
                
                return extraction_id
                
        except Exception as e:
            logger.error("complete_extraction_data_load_failed",
                        error=str(e),
                        property_name=extraction_data.get('PROPERTY_NAME'))
            raise
    
    def _load_property_data(self, cursor, extraction_id: str, data: Dict) -> str:
        """Load property information into properties_expanded table"""
        property_id = str(uuid.uuid4())
        
        # Build property data with type validation
        property_values = {}
        for extract_field, db_field in self.property_fields.items():
            value = data.get(extract_field)
            if pd.isna(value) or value is None:
                property_values[db_field] = None
            else:
                # Type validation for numeric fields
                if db_field in ['property_latitude', 'property_longitude', 'year_built', 'year_renovated', 
                               'building_height', 'number_of_buildings', 'land_area', 'units', 'avg_square_feet',
                               'parking_spaces_covered', 'parking_spaces_uncovered', 'last_sale_date',
                               'last_sale_price', 'last_sale_price_per_unit', 'last_sale_cap_rate']:
                    # Try to convert to numeric, set to None if not possible
                    try:
                        if isinstance(value, str) and not value.replace('.', '').replace('-', '').isdigit():
                            property_values[db_field] = None
                        else:
                            property_values[db_field] = float(value) if '.' in str(value) else int(value)
                    except (ValueError, TypeError):
                        property_values[db_field] = None
                else:
                    # Text fields - convert to string
                    property_values[db_field] = str(value) if value is not None else None
        
        # Insert property data
        columns = ['property_id', 'extraction_id'] + list(property_values.keys())
        values = [property_id, extraction_id] + list(property_values.values())
        placeholders = ', '.join(['%s'] * len(values))
        
        cursor.execute(f"""
            INSERT INTO properties_expanded ({', '.join(columns)})
            VALUES ({placeholders})
        """, values)
        
        logger.info("property_data_loaded", property_id=property_id)
        return property_id
    
    def _load_unit_mix_data(self, cursor, extraction_id: str, property_id: str, data: Dict):
        """Load unit mix data as JSONB"""
        unit_mix_data = {}
        
        # Extract unit mix fields by pattern
        for field_name, value in data.items():
            for pattern in self.unit_mix_patterns:
                if field_name.startswith(pattern):
                    if pd.notna(value) and value is not None:
                        unit_mix_data[field_name] = value
        
        if unit_mix_data:
            cursor.execute("""
                INSERT INTO unit_mix_data (unit_mix_id, extraction_id, property_id, unit_mix_data)
                VALUES (%s, %s, %s, %s)
            """, [str(uuid.uuid4()), extraction_id, property_id, json.dumps(unit_mix_data)])
            
            logger.info("unit_mix_data_loaded", fields_count=len(unit_mix_data))
    
    def _load_comparables_data(self, cursor, extraction_id: str, property_id: str, data: Dict):
        """Load rent and sales comparables data"""
        rent_comps = {}
        sales_comps = {}
        
        # Categorize comparable fields
        for field_name, value in data.items():
            if 'RENT_COMP' in field_name and pd.notna(value) and value is not None:
                rent_comps[field_name] = value
            elif 'SALES_COMP' in field_name and pd.notna(value) and value is not None:
                sales_comps[field_name] = value
        
        # Store as JSONB for now due to the large number of comparable fields (543)
        # This can be normalized later if needed
        if rent_comps or sales_comps:
            misc_data = {
                'rent_comparables': rent_comps,
                'sales_comparables': sales_comps
            }
            
            cursor.execute("""
                INSERT INTO miscellaneous_data (misc_id, extraction_id, property_id, field_data)
                VALUES (%s, %s, %s, %s)
            """, [str(uuid.uuid4()), extraction_id, property_id, json.dumps(misc_data)])
            
            logger.info("comparables_data_loaded", 
                       rent_comps=len(rent_comps), 
                       sales_comps=len(sales_comps))
    
    def _load_projections_data(self, cursor, extraction_id: str, property_id: str, data: Dict):
        """Load annual projections data"""
        projections = {}
        
        for field_name, value in data.items():
            if 'ANNUAL_CF' in field_name or 'YEAR_' in field_name:
                if pd.notna(value) and value is not None:
                    projections[field_name] = value
        
        if projections:
            cursor.execute("""
                INSERT INTO miscellaneous_data (misc_id, extraction_id, property_id, field_data)
                VALUES (%s, %s, %s, %s)
            """, [str(uuid.uuid4()), extraction_id, property_id, json.dumps({'projections': projections})])
            
            logger.info("projections_data_loaded", fields_count=len(projections))
    
    def _load_financing_data(self, cursor, extraction_id: str, property_id: str, data: Dict):
        """Load financing data"""
        financing_values = {}
        
        for extract_field, db_field in self.financing_fields.items():
            value = data.get(extract_field)
            if pd.notna(value) and value is not None:
                financing_values[db_field] = value
        
        # Add equity cash-on-cash fields
        equity_fields = {}
        for field_name, value in data.items():
            if 'EQUITY_CASH_ON_CASH' in field_name and pd.notna(value) and value is not None:
                equity_fields[field_name] = value
        
        if financing_values or equity_fields:
            financing_values.update(equity_fields)
            columns = ['financing_id', 'extraction_id', 'property_id'] + list(financing_values.keys())
            values = [str(uuid.uuid4()), extraction_id, property_id] + list(financing_values.values())
            placeholders = ', '.join(['%s'] * len(values))
            
            # For now, store in miscellaneous due to field complexity
            cursor.execute("""
                INSERT INTO miscellaneous_data (misc_id, extraction_id, property_id, field_data)
                VALUES (%s, %s, %s, %s)
            """, [str(uuid.uuid4()), extraction_id, property_id, json.dumps({'financing': financing_values})])
            
            logger.info("financing_data_loaded", fields_count=len(financing_values))
    
    def _load_returns_data(self, cursor, extraction_id: str, property_id: str, data: Dict):
        """Load investment returns data"""
        returns_values = {}
        
        for extract_field, db_field in self.returns_fields.items():
            value = data.get(extract_field)
            if pd.notna(value) and value is not None:
                returns_values[db_field] = value
        
        if returns_values:
            columns = ['returns_id', 'extraction_id', 'property_id'] + list(returns_values.keys())
            values = [str(uuid.uuid4()), extraction_id, property_id] + list(returns_values.values())
            placeholders = ', '.join(['%s'] * len(values))
            
            cursor.execute(f"""
                INSERT INTO investment_returns ({', '.join(columns)})
                VALUES ({placeholders})
            """, values)
            
            logger.info("returns_data_loaded", fields_count=len(returns_values))
    
    def _load_expenses_data(self, cursor, extraction_id: str, property_id: str, data: Dict):
        """Load operating expenses data"""
        expense_values = {}
        
        for extract_field, db_field in self.expense_fields.items():
            value = data.get(extract_field)
            if pd.notna(value) and value is not None:
                expense_values[db_field] = value
        
        # Add CAPEX fields
        capex_fields = {}
        for field_name, value in data.items():
            if 'CAPEX' in field_name and pd.notna(value) and value is not None:
                capex_fields[field_name] = value
        
        if expense_values or capex_fields:
            all_expenses = {**expense_values, **capex_fields}
            columns = ['expenses_id', 'extraction_id', 'property_id'] + list(expense_values.keys())
            values = [str(uuid.uuid4()), extraction_id, property_id] + list(expense_values.values())
            placeholders = ', '.join(['%s'] * len(values))
            
            cursor.execute(f"""
                INSERT INTO operating_expenses ({', '.join(columns)})
                VALUES ({placeholders})
            """, values)
            
            # Store CAPEX in miscellaneous if any
            if capex_fields:
                cursor.execute("""
                    INSERT INTO miscellaneous_data (misc_id, extraction_id, property_id, field_data)
                    VALUES (%s, %s, %s, %s)
                """, [str(uuid.uuid4()), extraction_id, property_id, json.dumps({'capex': capex_fields})])
            
            logger.info("expenses_data_loaded", 
                       operating_fields=len(expense_values),
                       capex_fields=len(capex_fields))
    
    def _load_income_data(self, cursor, extraction_id: str, property_id: str, data: Dict):
        """Load income data"""
        income_values = {}
        
        for extract_field, db_field in self.income_fields.items():
            value = data.get(extract_field)
            if pd.notna(value) and value is not None:
                income_values[db_field] = value
        
        # Add rent-related fields
        rent_fields = {}
        for field_name, value in data.items():
            if ('RENT' in field_name and 'COMP' not in field_name) and pd.notna(value) and value is not None:
                rent_fields[field_name] = value
        
        if income_values or rent_fields:
            all_income = {**income_values, **rent_fields}
            columns = ['income_id', 'extraction_id', 'property_id'] + list(income_values.keys())
            values = [str(uuid.uuid4()), extraction_id, property_id] + list(income_values.values())
            placeholders = ', '.join(['%s'] * len(values))
            
            cursor.execute(f"""
                INSERT INTO income_data ({', '.join(columns)})
                VALUES ({placeholders})
            """, values)
            
            # Store additional rent fields in miscellaneous if any
            if rent_fields:
                cursor.execute("""
                    INSERT INTO miscellaneous_data (misc_id, extraction_id, property_id, field_data)
                    VALUES (%s, %s, %s, %s)
                """, [str(uuid.uuid4()), extraction_id, property_id, json.dumps({'additional_rent_data': rent_fields})])
            
            logger.info("income_data_loaded", 
                       core_fields=len(income_values),
                       additional_rent_fields=len(rent_fields))
    
    def _load_miscellaneous_data(self, cursor, extraction_id: str, property_id: str, data: Dict):
        """Load all remaining miscellaneous fields"""
        # Collect all fields not handled by other methods
        processed_fields = set()
        processed_fields.update(self.property_fields.keys())
        processed_fields.update(self.income_fields.keys())
        processed_fields.update(self.expense_fields.keys())
        processed_fields.update(self.financing_fields.keys())
        processed_fields.update(self.returns_fields.keys())
        
        # Add pattern-based fields
        for field_name in data.keys():
            for pattern in self.unit_mix_patterns:
                if field_name.startswith(pattern):
                    processed_fields.add(field_name)
            if any(x in field_name for x in ['RENT_COMP', 'SALES_COMP', 'ANNUAL_CF', 'YEAR_', 'CAPEX', 'EQUITY_CASH_ON_CASH']):
                processed_fields.add(field_name)
            if 'RENT' in field_name and 'COMP' not in field_name:
                processed_fields.add(field_name)
        
        # Get remaining fields
        remaining_fields = {}
        for field_name, value in data.items():
            if field_name not in processed_fields and pd.notna(value) and value is not None:
                remaining_fields[field_name] = value
        
        if remaining_fields:
            cursor.execute("""
                INSERT INTO miscellaneous_data (misc_id, extraction_id, property_id, field_data)
                VALUES (%s, %s, %s, %s)
            """, [str(uuid.uuid4()), extraction_id, property_id, json.dumps({'other_fields': remaining_fields})])
            
            logger.info("miscellaneous_data_loaded", fields_count=len(remaining_fields))

def main():
    """Test function"""
    loader = ExpandedDataLoader()
    print("✅ Expanded data loader initialized successfully!")
    print(f"📊 Property fields: {len(loader.property_fields)}")
    print(f"💰 Income fields: {len(loader.income_fields)}")
    print(f"💸 Expense fields: {len(loader.expense_fields)}")
    print(f"🏦 Financing fields: {len(loader.financing_fields)}")
    print(f"📈 Returns fields: {len(loader.returns_fields)}")

if __name__ == "__main__":
    main()