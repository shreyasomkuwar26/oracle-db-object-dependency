"""
Oracle PL/SQL Package Generator for SCD Type 2 Implementation
Generates packages that handle staging to master table loads with history tracking
"""

from datetime import datetime
from typing import List, Dict
import os


class SCDPackageGenerator:
    """Generates Oracle PL/SQL packages for SCD Type 2 operations"""
    
    def __init__(self, output_dir: str = "generated_packages"):
        self.output_dir = output_dir
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def generate_package(self, 
                        staging_table: str,
                        master_table: str,
                        business_key_cols: List[str],
                        compare_cols: List[str],
                        additional_cols: List[str] = None) -> tuple:
        """
        Generate PL/SQL package specification and body
        
        Args:
            staging_table: Name of staging table
            master_table: Name of master table
            business_key_cols: Columns that identify unique business entity (e.g., ['CUSTOMER_ID'])
            compare_cols: Columns to compare for detecting changes (e.g., ['NAME', 'EMAIL', 'ADDRESS'])
            additional_cols: Other columns to include in insert (excluding key, compare, and SCD columns)
        
        Returns:
            tuple: (package_spec, package_body)
        """
        package_name = f"PKG_{master_table}_SCD2"
        
        if additional_cols is None:
            additional_cols = []
        
        # Generate package specification
        spec = self._generate_spec(package_name, staging_table, master_table)
        
        # Generate package body
        body = self._generate_body(
            package_name,
            staging_table,
            master_table,
            business_key_cols,
            compare_cols,
            additional_cols
        )
        
        return spec, body
    
    def _generate_spec(self, package_name: str, staging_table: str, master_table: str) -> str:
        """Generate package specification"""
        
        spec = f"""-- =====================================================
-- Package Specification: {package_name}
-- Purpose: Load data from {staging_table} to {master_table}
--          with SCD Type 2 history tracking
-- Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
-- =====================================================

CREATE OR REPLACE PACKAGE {package_name} AS
    
    -- Main procedure to process staging data
    PROCEDURE process_staging_data(
        p_run_date IN DATE DEFAULT TRUNC(SYSDATE)
    );
    
    -- Check if process already ran today
    FUNCTION is_already_processed(
        p_run_date IN DATE
    ) RETURN BOOLEAN;
    
    -- Log process execution
    PROCEDURE log_execution(
        p_status IN VARCHAR2,
        p_message IN VARCHAR2,
        p_records_processed IN NUMBER DEFAULT 0,
        p_records_inserted IN NUMBER DEFAULT 0,
        p_records_updated IN NUMBER DEFAULT 0
    );

END {package_name};
/
"""
        return spec
    
    def _generate_body(self,
                      package_name: str,
                      staging_table: str,
                      master_table: str,
                      business_key_cols: List[str],
                      compare_cols: List[str],
                      additional_cols: List[str]) -> str:
        """Generate package body"""
        
        # Build column lists
        all_cols = business_key_cols + compare_cols + additional_cols
        key_join = " AND ".join([f"m.{col} = s.{col}" for col in business_key_cols])
        key_cols_list = ", ".join(business_key_cols)
        
        # Build comparison logic for change detection
        compare_conditions = []
        for col in compare_cols:
            compare_conditions.append(
                f"NVL(m.{col}, 'NULL') != NVL(s.{col}, 'NULL')"
            )
        compare_logic = " OR ".join(compare_conditions)
        
        # Build insert column list
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join([f"s.{col}" for col in all_cols])
        
        body = f"""-- =====================================================
-- Package Body: {package_name}
-- =====================================================

CREATE OR REPLACE PACKAGE BODY {package_name} AS
    
    -- Constants
    c_end_of_time CONSTANT DATE := TO_DATE('31-DEC-9999', 'DD-MON-YYYY');
    c_package_name CONSTANT VARCHAR2(100) := '{package_name}';
    
    -- =====================================================
    -- Function: is_already_processed
    -- Purpose: Check if the process already ran for given date
    -- =====================================================
    FUNCTION is_already_processed(
        p_run_date IN DATE
    ) RETURN BOOLEAN IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_count
        FROM ETL_PROCESS_LOG
        WHERE package_name = c_package_name
          AND TRUNC(run_date) = TRUNC(p_run_date)
          AND status = 'SUCCESS';
        
        RETURN v_count > 0;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN FALSE;
        WHEN OTHERS THEN
            -- If log table doesn't exist, allow processing
            RETURN FALSE;
    END is_already_processed;
    
    -- =====================================================
    -- Procedure: log_execution
    -- Purpose: Log package execution details
    -- =====================================================
    PROCEDURE log_execution(
        p_status IN VARCHAR2,
        p_message IN VARCHAR2,
        p_records_processed IN NUMBER DEFAULT 0,
        p_records_inserted IN NUMBER DEFAULT 0,
        p_records_updated IN NUMBER DEFAULT 0
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO ETL_PROCESS_LOG (
            log_id,
            package_name,
            run_date,
            status,
            message,
            records_processed,
            records_inserted,
            records_updated,
            created_date
        ) VALUES (
            ETL_LOG_SEQ.NEXTVAL,  -- Assumes sequence exists
            c_package_name,
            SYSDATE,
            p_status,
            p_message,
            p_records_processed,
            p_records_inserted,
            p_records_updated,
            SYSDATE
        );
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            -- If logging fails, don't stop the main process
            DBMS_OUTPUT.PUT_LINE('Logging failed: ' || SQLERRM);
            ROLLBACK;
    END log_execution;
    
    -- =====================================================
    -- Procedure: process_staging_data
    -- Purpose: Main procedure to process SCD Type 2 logic
    -- =====================================================
    PROCEDURE process_staging_data(
        p_run_date IN DATE DEFAULT TRUNC(SYSDATE)
    ) IS
        v_records_processed NUMBER := 0;
        v_records_inserted NUMBER := 0;
        v_records_updated NUMBER := 0;
        v_start_time TIMESTAMP := SYSTIMESTAMP;
        v_error_message VARCHAR2(4000);
        
    BEGIN
        -- Check if already processed today
        IF is_already_processed(p_run_date) THEN
            v_error_message := 'Process already completed for ' || TO_CHAR(p_run_date, 'YYYY-MM-DD') || 
                             '. Skipping to prevent duplicate processing.';
            DBMS_OUTPUT.PUT_LINE(v_error_message);
            log_execution('SKIPPED', v_error_message, 0, 0, 0);
            RETURN;
        END IF;
        
        DBMS_OUTPUT.PUT_LINE('Starting SCD Type 2 process for {master_table}...');
        log_execution('STARTED', 'Process initiated', 0, 0, 0);
        
        -- Step 1: Expire existing records where data has changed
        UPDATE {master_table} m
        SET m.eff_end_dt = TRUNC(SYSDATE),
            m.current_version = 0,
            m.last_updated_date = SYSDATE
        WHERE m.current_version = 1
          AND EXISTS (
              SELECT 1
              FROM {staging_table} s
              WHERE {key_join}
                AND ({compare_logic})
          );
        
        v_records_updated := SQL%ROWCOUNT;
        DBMS_OUTPUT.PUT_LINE('Expired ' || v_records_updated || ' existing records');
        
        -- Step 2: Insert new records (both new IDs and changed records)
        INSERT INTO {master_table} (
            {insert_cols},
            eff_start_dt,
            eff_end_dt,
            current_version,
            created_date,
            last_updated_date
        )
        SELECT 
            {insert_vals},
            TRUNC(SYSDATE) as eff_start_dt,
            c_end_of_time as eff_end_dt,
            1 as current_version,
            SYSDATE as created_date,
            SYSDATE as last_updated_date
        FROM {staging_table} s
        WHERE NOT EXISTS (
            -- Exclude records that are identical to current version
            SELECT 1
            FROM {master_table} m
            WHERE {key_join}
              AND m.current_version = 1
              AND NOT ({compare_logic})
        );
        
        v_records_inserted := SQL%ROWCOUNT;
        DBMS_OUTPUT.PUT_LINE('Inserted ' || v_records_inserted || ' new/changed records');
        
        v_records_processed := v_records_inserted + v_records_updated;
        
        -- Commit the transaction
        COMMIT;
        
        -- Log success
        v_error_message := 'Process completed successfully. Duration: ' || 
                          TO_CHAR(EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)), '999.99') || ' seconds';
        DBMS_OUTPUT.PUT_LINE(v_error_message);
        
        log_execution(
            'SUCCESS',
            v_error_message,
            v_records_processed,
            v_records_inserted,
            v_records_updated
        );
        
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            v_error_message := 'Error in {package_name}: ' || SQLERRM || ' - ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
            DBMS_OUTPUT.PUT_LINE(v_error_message);
            
            log_execution(
                'ERROR',
                v_error_message,
                v_records_processed,
                v_records_inserted,
                v_records_updated
            );
            
            RAISE;
    END process_staging_data;

END {package_name};
/
"""
        return body
    
    def generate_log_table_ddl(self) -> str:
        """Generate DDL for ETL process log table"""
        
        ddl = """-- =====================================================
-- ETL Process Log Table
-- Purpose: Track package execution history
-- =====================================================

CREATE TABLE ETL_PROCESS_LOG (
    log_id NUMBER PRIMARY KEY,
    package_name VARCHAR2(100) NOT NULL,
    run_date DATE NOT NULL,
    status VARCHAR2(20) NOT NULL,
    message VARCHAR2(4000),
    records_processed NUMBER DEFAULT 0,
    records_inserted NUMBER DEFAULT 0,
    records_updated NUMBER DEFAULT 0,
    created_date DATE DEFAULT SYSDATE
);

CREATE INDEX idx_etl_log_pkg_date ON ETL_PROCESS_LOG(package_name, run_date);

CREATE SEQUENCE ETL_LOG_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;

COMMENT ON TABLE ETL_PROCESS_LOG IS 'Logs execution of ETL packages';
COMMENT ON COLUMN ETL_PROCESS_LOG.status IS 'Status: STARTED, SUCCESS, ERROR, SKIPPED';
"""
        return ddl
    
    def save_package(self, package_name: str, spec: str, body: str):
        """Save package specification and body to files"""
        
        spec_file = os.path.join(self.output_dir, f"{package_name}_spec.sql")
        body_file = os.path.join(self.output_dir, f"{package_name}_body.sql")
        
        with open(spec_file, 'w') as f:
            f.write(spec)
        
        with open(body_file, 'w') as f:
            f.write(body)
        
        print(f"Generated: {spec_file}")
        print(f"Generated: {body_file}")
    
    def generate_all_packages(self, table_mappings: List[Dict]):
        """
        Generate packages for multiple table mappings
        
        Args:
            table_mappings: List of dictionaries with keys:
                - staging_table: str
                - master_table: str
                - business_key_cols: List[str]
                - compare_cols: List[str]
                - additional_cols: List[str] (optional)
        """
        
        print(f"Starting package generation for {len(table_mappings)} table mappings...")
        print(f"Output directory: {self.output_dir}\n")
        
        # Generate log table DDL
        log_ddl = self.generate_log_table_ddl()
        log_file = os.path.join(self.output_dir, "00_create_log_table.sql")
        with open(log_file, 'w') as f:
            f.write(log_ddl)
        print(f"Generated: {log_file}\n")
        
        # Generate master deployment script
        master_script = self._generate_master_script(table_mappings)
        master_file = os.path.join(self.output_dir, "deploy_all_packages.sql")
        with open(master_file, 'w') as f:
            f.write(master_script)
        
        # Generate each package
        for idx, mapping in enumerate(table_mappings, 1):
            print(f"[{idx}/{len(table_mappings)}] Processing {mapping['master_table']}...")
            
            spec, body = self.generate_package(
                staging_table=mapping['staging_table'],
                master_table=mapping['master_table'],
                business_key_cols=mapping['business_key_cols'],
                compare_cols=mapping['compare_cols'],
                additional_cols=mapping.get('additional_cols', [])
            )
            
            package_name = f"PKG_{mapping['master_table']}_SCD2"
            self.save_package(package_name, spec, body)
            print()
        
        print(f"Generated master deployment script: {master_file}")
        print(f"\nAll packages generated successfully!")
        print(f"Total files created: {len(table_mappings) * 2 + 2}")
    
    def _generate_master_script(self, table_mappings: List[Dict]) -> str:
        """Generate master deployment script"""
        
        script = f"""-- =====================================================
-- Master Deployment Script
-- Purpose: Deploy all SCD Type 2 packages
-- Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
-- =====================================================

PROMPT Creating ETL Process Log Table...
@@00_create_log_table.sql

"""
        
        for mapping in table_mappings:
            package_name = f"PKG_{mapping['master_table']}_SCD2"
            script += f"""
PROMPT Deploying {package_name}...
@@{package_name}_spec.sql
@@{package_name}_body.sql
"""
        
        script += """
PROMPT
PROMPT All packages deployed successfully!
PROMPT
"""
        
        return script


# =====================================================
# EXAMPLE USAGE
# =====================================================

if __name__ == "__main__":
    
    # Define your table mappings
    table_mappings = [
        {
            'staging_table': 'STG_CUSTOMER',
            'master_table': 'CUSTOMER',
            'business_key_cols': ['CUSTOMER_ID'],
            'compare_cols': ['CUSTOMER_NAME', 'EMAIL', 'PHONE', 'ADDRESS', 'CITY', 'STATE', 'ZIP_CODE'],
            'additional_cols': ['CUSTOMER_TYPE', 'CREDIT_LIMIT']
        },
        {
            'staging_table': 'STG_PRODUCT',
            'master_table': 'PRODUCT',
            'business_key_cols': ['PRODUCT_ID'],
            'compare_cols': ['PRODUCT_NAME', 'DESCRIPTION', 'UNIT_PRICE', 'CATEGORY'],
            'additional_cols': ['SUPPLIER_ID', 'UNITS_IN_STOCK']
        },
        {
            'staging_table': 'STG_EMPLOYEE',
            'master_table': 'EMPLOYEE',
            'business_key_cols': ['EMPLOYEE_ID'],
            'compare_cols': ['FIRST_NAME', 'LAST_NAME', 'EMAIL', 'DEPARTMENT', 'TITLE', 'SALARY'],
            'additional_cols': ['MANAGER_ID', 'HIRE_DATE']
        },
        {
            'staging_table': 'STG_VENDOR',
            'master_table': 'VENDOR',
            'business_key_cols': ['VENDOR_ID'],
            'compare_cols': ['VENDOR_NAME', 'CONTACT_NAME', 'CONTACT_EMAIL', 'ADDRESS', 'RATING'],
            'additional_cols': ['PAYMENT_TERMS', 'TAX_ID']
        }
    ]
    
    # Initialize generator
    generator = SCDPackageGenerator(output_dir="generated_scd2_packages")
    
    # Generate all packages
    generator.generate_all_packages(table_mappings)
    
    print("\n" + "="*60)
    print("DEPLOYMENT INSTRUCTIONS:")
    print("="*60)
    print("1. Review generated files in 'generated_scd2_packages' folder")
    print("2. Connect to your Oracle database")
    print("3. Run: @deploy_all_packages.sql")
    print("4. Test each package:")
    print("   EXEC PKG_CUSTOMER_SCD2.process_staging_data;")
    print("="*60)