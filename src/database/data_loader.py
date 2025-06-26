"""
Data Loader for B&R Capital Dashboard

This module handles loading extracted underwriting data into the PostgreSQL
database with automatic version tracking and conflict resolution.

Features:
- Automatic property registration
- Version tracking with historical data
- Batch loading with transaction safety
- Data validation and type conversion
- Comprehensive error handling
"""

import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import structlog
import pandas as pd
from .connection import get_cursor, get_connection
from .schema import SchemaManager

logger = structlog.get_logger().bind(component="DataLoader")

class DataLoader:
    """Loads extracted underwriting data into the database"""
    
    def __init__(self):
        self.schema_manager = SchemaManager()
        
    def load_extraction_data(self, extraction_data: Dict[str, Any], 
                           deal_stage: str, metadata: Optional[Dict] = None) -> str:
        """
        Load a single extraction into the database
        
        Args:
            extraction_data: The extracted data dictionary
            deal_stage: The deal stage (e.g., 'active_uw_review')
            metadata: Optional metadata about the extraction
            
        Returns:
            extraction_id: UUID of the created extraction record
        """
        try:
            with get_cursor() as cursor:
                # 1. Register or get property
                property_id = self._register_property(cursor, extraction_data)
                
                # 2. Insert main underwriting data
                extraction_id = self._insert_underwriting_data(
                    cursor, property_id, extraction_data, deal_stage, metadata
                )
                
                # 3. Insert related data
                self._insert_annual_cashflows(cursor, extraction_id, property_id, extraction_data)
                self._insert_rent_comparables(cursor, extraction_id, property_id, extraction_data)
                self._insert_sales_comparables(cursor, extraction_id, property_id, extraction_data)
                
                # 4. Insert extraction metadata
                if metadata:
                    self._insert_extraction_metadata(cursor, extraction_id, metadata)
                
                logger.info(
                    "extraction_data_loaded",
                    extraction_id=extraction_id,
                    property_id=property_id,
                    property_name=extraction_data.get('PROPERTY_NAME'),
                    deal_stage=deal_stage
                )
                
                return extraction_id
                
        except Exception as e:
            logger.error(
                "extraction_data_load_failed",
                error=str(e),
                property_name=extraction_data.get('PROPERTY_NAME'),
                deal_stage=deal_stage
            )
            raise
    
    def _register_property(self, cursor, extraction_data: Dict[str, Any]) -> str:
        """Register a property or get existing property ID"""
        property_name = extraction_data.get('PROPERTY_NAME')
        if not property_name:
            raise ValueError("PROPERTY_NAME is required")
        
        # Check if property exists
        cursor.execute("""
            SELECT property_id FROM properties WHERE property_name = %s
        """, (property_name,))
        
        result = cursor.fetchone()
        if result:
            return result[0]
        
        # Create new property
        property_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO properties (
                property_id, property_name, property_city, property_state,
                property_address, market, submarket, county
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            property_id,
            property_name,
            extraction_data.get('PROPERTY_CITY'),
            extraction_data.get('PROPERTY_STATE'),
            extraction_data.get('PROPERTY_ADDRESS'),
            extraction_data.get('MARKET'),
            extraction_data.get('SUBMARKET'),
            extraction_data.get('COUNTY')
        ))
        
        logger.info("property_registered", property_id=property_id, property_name=property_name)
        return property_id
    
    def _insert_underwriting_data(self, cursor, property_id: str, 
                                 extraction_data: Dict[str, Any], 
                                 deal_stage: str, metadata: Optional[Dict]) -> str:
        """Insert main underwriting data"""
        extraction_id = str(uuid.uuid4())
        
        # Convert deal stage to enum format
        deal_stage_enum = self._convert_deal_stage(deal_stage)
        
        # Prepare data with type conversion
        data_values = self._prepare_underwriting_values(extraction_data, metadata)
        
        cursor.execute("""
            INSERT INTO underwriting_data (
                extraction_id, property_id, property_name, deal_stage,
                file_path, extraction_timestamp, file_modified_date, file_size_mb,
                
                -- General Assumptions
                year_built, year_renovated, location_quality, building_quality,
                units, avg_square_feet, parking_spaces_covered, parking_spaces_uncovered,
                individually_metered, current_owner, last_sale_date, last_sale_price,
                last_sale_price_per_unit, last_sale_cap_rate, building_height,
                building_type, project_type, number_of_buildings, building_zoning,
                land_area, parcel_number, property_latitude, property_longitude,
                property_address_field, property_zip,
                
                -- Exit Assumptions
                exit_period_months, exit_cap_rate, sales_transaction_costs,
                
                -- NOI Assumptions
                empirical_rent, rent_psf, gross_potential_rental_income,
                concessions, loss_to_lease, vacancy_loss, bad_debts, other_loss,
                property_management_fee, net_rental_income, parking_income,
                laundry_income, other_income, effective_gross_income,
                
                -- Operating Expenses
                advertising_marketing, management_fee, payroll, repairs_maintenance,
                contract_services, turnover, utilities, insurance, real_estate_taxes,
                other_expenses, total_operating_expenses, net_operating_income,
                
                -- Debt and Equity
                purchase_price, hard_costs_budget, soft_costs_budget,
                total_hard_costs, total_soft_costs, total_acquisition_budget,
                loan_amount, loan_to_cost, loan_to_value,
                equity_lp_capital, equity_gp_capital,
                
                -- Return Metrics
                t12_return_on_pp, t12_return_on_cost, levered_returns_irr,
                levered_returns_moic, basis_unit_at_close, basis_unit_at_exit
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
        """, [
            extraction_id, property_id, extraction_data.get('PROPERTY_NAME'), deal_stage_enum,
            data_values['file_path'], data_values['extraction_timestamp'], 
            data_values['file_modified_date'], data_values['file_size_mb']
        ] + data_values['field_values'])
        
        return extraction_id
    
    def _prepare_underwriting_values(self, extraction_data: Dict[str, Any], 
                                   metadata: Optional[Dict]) -> Dict[str, Any]:
        """Prepare and convert values for database insertion"""
        
        # Extract metadata
        file_path = extraction_data.get('_file_path', '')
        extraction_timestamp = extraction_data.get('_extraction_timestamp')
        if isinstance(extraction_timestamp, str):
            extraction_timestamp = datetime.fromisoformat(extraction_timestamp.replace('Z', '+00:00'))
        else:
            extraction_timestamp = datetime.now()
        
        file_modified_date = None
        file_size_mb = None
        
        if metadata:
            file_modified_date = metadata.get('_file_modified_date')
            if isinstance(file_modified_date, str):
                file_modified_date = datetime.fromisoformat(file_modified_date.replace('Z', '+00:00'))
            file_size_mb = metadata.get('_file_size_mb')
        
        # Define field mapping and convert values
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
        
        field_values = []
        for field in field_mapping:
            value = extraction_data.get(field)
            
            # Convert to appropriate type
            if pd.isna(value) or value is None or value == '':
                field_values.append(None)
            elif isinstance(value, str) and value.lower() in ['n/a', 'na', 'null']:
                field_values.append(None)
            else:
                field_values.append(value)
        
        return {
            'file_path': file_path,
            'extraction_timestamp': extraction_timestamp,
            'file_modified_date': file_modified_date,
            'file_size_mb': file_size_mb,
            'field_values': field_values
        }
    
    def _insert_annual_cashflows(self, cursor, extraction_id: str, property_id: str, 
                               extraction_data: Dict[str, Any]):
        """Insert annual cashflow data"""
        # Look for annual cashflow fields (Year 1-5)
        cashflow_fields = [
            'GROSS_POTENTIAL_INCOME', 'VACANCY_LOSS', 'EFFECTIVE_GROSS_INCOME',
            'OPERATING_EXPENSES', 'NET_OPERATING_INCOME', 'DEBT_SERVICE',
            'BEFORE_TAX_CASH_FLOW', 'CAPITAL_IMPROVEMENTS', 'TENANT_IMPROVEMENTS',
            'LEASING_COMMISSIONS'
        ]
        
        # Extract year-based data (if available)
        for year in range(1, 6):  # Years 1-5
            cashflow_data = {}
            has_data = False
            
            for field in cashflow_fields:
                year_field = f"{field}_YEAR_{year}"
                if year_field in extraction_data:
                    cashflow_data[field.lower()] = extraction_data[year_field]
                    has_data = True
            
            if has_data:
                cursor.execute("""
                    INSERT INTO annual_cashflows (
                        extraction_id, property_id, year_number,
                        gross_potential_income, vacancy_loss, effective_gross_income,
                        operating_expenses, net_operating_income, debt_service,
                        before_tax_cash_flow, capital_improvements, tenant_improvements,
                        leasing_commissions
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    extraction_id, property_id, year,
                    cashflow_data.get('gross_potential_income'),
                    cashflow_data.get('vacancy_loss'),
                    cashflow_data.get('effective_gross_income'),
                    cashflow_data.get('operating_expenses'),
                    cashflow_data.get('net_operating_income'),
                    cashflow_data.get('debt_service'),
                    cashflow_data.get('before_tax_cash_flow'),
                    cashflow_data.get('capital_improvements'),
                    cashflow_data.get('tenant_improvements'),
                    cashflow_data.get('leasing_commissions')
                ))
    
    def _insert_rent_comparables(self, cursor, extraction_id: str, property_id: str,
                               extraction_data: Dict[str, Any]):
        """Insert rent comparable data"""
        # Look for rent comp fields
        for i in range(1, 21):  # Up to 20 rent comps
            comp_data = {}
            has_data = False
            
            comp_fields = {
                f'RENT_COMP_{i}_NAME': 'comp_name',
                f'RENT_COMP_{i}_ADDRESS': 'comp_address',
                f'RENT_COMP_{i}_CITY': 'comp_city',
                f'RENT_COMP_{i}_DISTANCE': 'comp_distance',
                f'RENT_COMP_{i}_UNITS': 'comp_units',
                f'RENT_COMP_{i}_YEAR_BUILT': 'comp_year_built',
                f'RENT_COMP_{i}_RENT_PSF': 'comp_rent_psf',
                f'RENT_COMP_{i}_TOTAL_RENT': 'comp_total_rent'
            }
            
            for field, db_field in comp_fields.items():
                if field in extraction_data and extraction_data[field] is not None:
                    comp_data[db_field] = extraction_data[field]
                    has_data = True
            
            if has_data:
                cursor.execute("""
                    INSERT INTO rent_comparables (
                        extraction_id, property_id, comp_number,
                        comp_name, comp_address, comp_city, comp_distance,
                        comp_units, comp_year_built, comp_rent_psf, comp_total_rent
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    extraction_id, property_id, i,
                    comp_data.get('comp_name'),
                    comp_data.get('comp_address'),
                    comp_data.get('comp_city'),
                    comp_data.get('comp_distance'),
                    comp_data.get('comp_units'),
                    comp_data.get('comp_year_built'),
                    comp_data.get('comp_rent_psf'),
                    comp_data.get('comp_total_rent')
                ))
    
    def _insert_sales_comparables(self, cursor, extraction_id: str, property_id: str,
                                extraction_data: Dict[str, Any]):
        """Insert sales comparable data"""
        # Look for sales comp fields
        for i in range(1, 21):  # Up to 20 sales comps
            comp_data = {}
            has_data = False
            
            comp_fields = {
                f'SALES_COMP_{i}_NAME': 'comp_name',
                f'SALES_COMP_{i}_ADDRESS': 'comp_address',
                f'SALES_COMP_{i}_CITY': 'comp_city',
                f'SALES_COMP_{i}_UNITS': 'comp_units',
                f'SALES_COMP_{i}_YEAR_BUILT': 'comp_year_built',
                f'SALES_COMP_{i}_PRICE': 'comp_price',
                f'SALES_COMP_{i}_PRICE_PER_UNIT': 'comp_price_per_unit',
                f'SALES_COMP_{i}_CAP_RATE': 'comp_cap_rate',
                f'SALES_COMP_{i}_SALE_DATE': 'comp_sale_date'
            }
            
            for field, db_field in comp_fields.items():
                if field in extraction_data and extraction_data[field] is not None:
                    comp_data[db_field] = extraction_data[field]
                    has_data = True
            
            if has_data:
                cursor.execute("""
                    INSERT INTO sales_comparables (
                        extraction_id, property_id, comp_number,
                        comp_name, comp_address, comp_city, comp_units,
                        comp_year_built, comp_price, comp_price_per_unit,
                        comp_cap_rate, comp_sale_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    extraction_id, property_id, i,
                    comp_data.get('comp_name'),
                    comp_data.get('comp_address'),
                    comp_data.get('comp_city'),
                    comp_data.get('comp_units'),
                    comp_data.get('comp_year_built'),
                    comp_data.get('comp_price'),
                    comp_data.get('comp_price_per_unit'),
                    comp_data.get('comp_cap_rate'),
                    comp_data.get('comp_sale_date')
                ))
    
    def _insert_extraction_metadata(self, cursor, extraction_id: str, metadata: Dict[str, Any]):
        """Insert extraction metadata"""
        cursor.execute("""
            INSERT INTO extraction_metadata (
                extraction_id, total_fields_attempted, successful_extractions,
                failed_extractions, extraction_duration_seconds, error_count,
                warnings_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            extraction_id,
            metadata.get('total_fields', 0),
            metadata.get('successful', 0),
            metadata.get('total_fields', 0) - metadata.get('successful', 0),
            metadata.get('duration_seconds', 0),
            len(metadata.get('errors', [])),
            len(metadata.get('warnings', []))
        ))
    
    def _convert_deal_stage(self, deal_stage: str) -> str:
        """Convert deal stage to database enum format"""
        stage_mapping = {
            '0) Dead Deals': 'dead_deals',
            '1) Initial UW and Review': 'initial_uw_review',
            '2) Active UW and Review': 'active_uw_review',
            '3) Deals Under Contract': 'under_contract',
            '4) Closed Deals': 'closed_deals',
            '5) Realized Deals': 'realized_deals'
        }
        
        return stage_mapping.get(deal_stage, 'active_uw_review')
    
    def load_batch_extraction_results(self, batch_results_file: str) -> List[str]:
        """
        Load results from a batch extraction process
        
        Args:
            batch_results_file: Path to the batch results JSON file
            
        Returns:
            List of extraction IDs that were loaded
        """
        logger.info("loading_batch_extraction_results", file=batch_results_file)
        
        with open(batch_results_file, 'r') as f:
            batch_data = json.load(f)
        
        extraction_ids = []
        
        for result in batch_data.get('results', []):
            try:
                # Extract deal stage from file metadata
                deal_stage = result.get('_deal_stage', 'active_uw_review')
                metadata = result.get('_extraction_metadata', {})
                
                # Load the extraction
                extraction_id = self.load_extraction_data(result, deal_stage, metadata)
                extraction_ids.append(extraction_id)
                
            except Exception as e:
                logger.error(
                    "batch_extraction_load_failed",
                    error=str(e),
                    property_name=result.get('PROPERTY_NAME')
                )
                continue
        
        logger.info(
            "batch_extraction_results_loaded",
            total_loaded=len(extraction_ids),
            total_attempted=len(batch_data.get('results', []))
        )
        
        return extraction_ids
    
    def get_property_history(self, property_name: str) -> List[Dict[str, Any]]:
        """Get version history for a property"""
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    extraction_id, version_number, extraction_timestamp,
                    deal_stage, purchase_price, levered_returns_irr,
                    net_operating_income, file_path
                FROM underwriting_data
                WHERE property_name = %s
                ORDER BY extraction_timestamp DESC
            """, (property_name,))
            
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_latest_data_summary(self) -> Dict[str, Any]:
        """Get summary of latest data in the database"""
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_properties,
                    COUNT(DISTINCT deal_stage) as stages_count,
                    MAX(extraction_timestamp) as latest_extraction,
                    SUM(units) as total_units,
                    AVG(purchase_price) as avg_purchase_price
                FROM latest_underwriting_data
            """)
            
            result = cursor.fetchone()
            return {
                'total_properties': result[0],
                'stages_count': result[1],
                'latest_extraction': result[2],
                'total_units': result[3],
                'avg_purchase_price': result[4]
            }