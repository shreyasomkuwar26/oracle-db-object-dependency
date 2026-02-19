/******************************************************************************
 * PACKAGE: PKG_DIM_PRODUCT_LOAD
 * PURPOSE: Loads and maintains SCD Type 2 for DIM_PRODUCT from Master layer
 *          tables covering Product, Category, Sub-Category, Supplier,
 *          Supplier Address, Brand, and UOM master tables.
 *
 * SCENARIOS HANDLED:
 *   - Normal daily delta load
 *   - Catch-up load for missed file dates (Scenario 1)
 *   - Correction/replay from a given file date (Scenario 2)
 *
 * ASSUMED MASTER TABLES:
 *   M_PRODUCT         - Core product attributes
 *   M_CATEGORY        - Product category
 *   M_SUB_CATEGORY    - Product sub-category
 *   M_SUPPLIER        - Supplier master
 *   M_SUPPLIER_ADDR   - Supplier address
 *   M_BRAND           - Brand master
 *   M_UOM             - Unit of measure
 *
 * ASSUMED DIM TABLE:
 *   DIM_PRODUCT       - Denormalized SCD Type 2 dimension
 *
 * ASSUMED CONTROL TABLE:
 *   DW_BATCH_CONTROL  - Tracks file load status per layer
 *
 * CHANGE DETECTION: MINUS operator across all DIM-contributing columns
 *
 * AUTHOR : <Your Name>
 * DATE   : 2024
 ******************************************************************************/

CREATE OR REPLACE PACKAGE PKG_DIM_PRODUCT_LOAD AS

    -- -------------------------------------------------------------------------
    -- Main entry point: load DIM for a single file date
    -- Called by orchestrator for both normal and catch-up runs
    -- -------------------------------------------------------------------------
    PROCEDURE LOAD_DIM_FOR_FILE_DATE (
        p_file_date     IN DATE,
        p_batch_id      IN NUMBER,
        p_called_by     IN VARCHAR2 DEFAULT 'SCHEDULER'  -- SCHEDULER / CATCHUP / REPLAY
    );

    -- -------------------------------------------------------------------------
    -- Scenario 1: Catch-up loader
    -- Finds all Master-complete but DIM-pending file dates and processes in order
    -- -------------------------------------------------------------------------
    PROCEDURE CATCHUP_DIM_LOAD (
        p_from_date     IN DATE DEFAULT NULL,   -- NULL = auto-detect from control table
        p_to_date       IN DATE DEFAULT TRUNC(SYSDATE)
    );

    -- -------------------------------------------------------------------------
    -- Scenario 2: Correction replay
    -- Rolls back DIM from p_from_date onward and replays each file date in order
    -- -------------------------------------------------------------------------
    PROCEDURE REPLAY_DIM_FROM_DATE (
        p_from_date     IN DATE,
        p_to_date       IN DATE DEFAULT TRUNC(SYSDATE)
    );

END PKG_DIM_PRODUCT_LOAD;
/


CREATE OR REPLACE PACKAGE BODY PKG_DIM_PRODUCT_LOAD AS

    -- =========================================================================
    -- PRIVATE CONSTANTS
    -- =========================================================================
    C_HIGH_DATE     CONSTANT DATE    := DATE '9999-12-31';
    C_DIM_LAYER     CONSTANT VARCHAR2(20) := 'DIM';
    C_MASTER_LAYER  CONSTANT VARCHAR2(20) := 'MASTER';
    C_STATUS_DONE   CONSTANT VARCHAR2(20) := 'COMPLETED';
    C_STATUS_FAIL   CONSTANT VARCHAR2(20) := 'FAILED';
    C_STATUS_PEND   CONSTANT VARCHAR2(20) := 'PENDING';

    -- =========================================================================
    -- PRIVATE: Logging helper
    -- Writes to DW_LOAD_LOG table. Adjust to your logging framework as needed.
    -- =========================================================================
    PROCEDURE LOG_MSG (
        p_batch_id  IN NUMBER,
        p_proc      IN VARCHAR2,
        p_msg       IN VARCHAR2,
        p_severity  IN VARCHAR2 DEFAULT 'INFO'
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO DW_LOAD_LOG (
            log_id, batch_id, layer, procedure_name,
            log_message, severity, log_timestamp
        ) VALUES (
            DW_LOAD_LOG_SEQ.NEXTVAL, p_batch_id, C_DIM_LAYER, p_proc,
            p_msg, p_severity, SYSTIMESTAMP
        );
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN NULL;  -- Never let logging kill the main process
    END LOG_MSG;

    -- =========================================================================
    -- PRIVATE: Update batch control status for DIM layer
    -- =========================================================================
    PROCEDURE UPDATE_BATCH_STATUS (
        p_batch_id      IN NUMBER,
        p_file_date     IN DATE,
        p_status        IN VARCHAR2,
        p_rows_affected IN NUMBER DEFAULT 0,
        p_error_msg     IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        MERGE INTO DW_BATCH_CONTROL tgt
        USING (SELECT p_batch_id    AS batch_id,
                      p_file_date   AS file_date,
                      C_DIM_LAYER   AS layer
               FROM DUAL) src
        ON (    tgt.batch_id  = src.batch_id
            AND tgt.file_date = src.file_date
            AND tgt.layer     = src.layer)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.status          = p_status,
                tgt.rows_processed  = p_rows_affected,
                tgt.error_message   = p_error_msg,
                tgt.updated_ts      = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (batch_id, file_date, layer, status,
                    rows_processed, error_message, created_ts, updated_ts)
            VALUES (p_batch_id, p_file_date, C_DIM_LAYER, p_status,
                    p_rows_affected, p_error_msg, SYSTIMESTAMP, SYSTIMESTAMP);
    END UPDATE_BATCH_STATUS;

    -- =========================================================================
    -- PRIVATE: Build current DIM snapshot by joining all 7 master tables
    --
    -- KEY DESIGN RULE: Always join Master tables using:
    --     WHERE m.effective_start_date <= p_file_date
    --       AND m.effective_end_date   >  p_file_date   (or = C_HIGH_DATE)
    --
    -- This ensures point-in-time correct denormalization, critical for
    -- catch-up (Scenario 1) and replay (Scenario 2) where we process
    -- past file dates after the fact.
    -- =========================================================================
    PROCEDURE BUILD_DIM_STAGING (
        p_file_date IN DATE
    ) IS
    BEGIN
        -- Truncate staging; it is session-scoped scratch space
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DIM_PRODUCT_STAGING';

        -- Insert denormalized snapshot as of p_file_date
        -- Only products active in M_PRODUCT as of this file date are included
        INSERT /*+ APPEND */ INTO DIM_PRODUCT_STAGING (
            -- Natural / business keys
            product_code,
            -- Product attributes
            product_name,
            product_description,
            product_status,
            launch_date,
            discontinue_date,
            -- Category attributes (from M_CATEGORY)
            category_code,
            category_name,
            category_description,
            -- Sub-category attributes (from M_SUB_CATEGORY)
            sub_category_code,
            sub_category_name,
            -- Supplier attributes (from M_SUPPLIER)
            supplier_code,
            supplier_name,
            supplier_contact_name,
            supplier_contact_email,
            supplier_phone,
            -- Supplier address attributes (from M_SUPPLIER_ADDR)
            supplier_address_line1,
            supplier_address_line2,
            supplier_city,
            supplier_state,
            supplier_country,
            supplier_postal_code,
            -- Brand attributes (from M_BRAND)
            brand_code,
            brand_name,
            brand_owner,
            -- UOM attributes (from M_UOM)
            uom_code,
            uom_description,
            uom_conversion_factor,
            -- Master surrogate keys stored in DIM for change detection
            -- (avoids full column MINUS on next load)
            m_product_sk,
            m_category_sk,
            m_sub_category_sk,
            m_supplier_sk,
            m_supplier_addr_sk,
            m_brand_sk,
            m_uom_sk
        )
        SELECT
            -- Natural key
            p.product_code,
            -- Product
            p.product_name,
            p.product_description,
            p.product_status,
            p.launch_date,
            p.discontinue_date,
            -- Category
            c.category_code,
            c.category_name,
            c.category_description,
            -- Sub-category
            sc.sub_category_code,
            sc.sub_category_name,
            -- Supplier
            s.supplier_code,
            s.supplier_name,
            s.contact_name,
            s.contact_email,
            s.phone,
            -- Supplier Address
            sa.address_line1,
            sa.address_line2,
            sa.city,
            sa.state,
            sa.country,
            sa.postal_code,
            -- Brand
            b.brand_code,
            b.brand_name,
            b.brand_owner,
            -- UOM
            u.uom_code,
            u.uom_description,
            u.conversion_factor,
            -- Master surrogate keys
            p.product_sk,
            c.category_sk,
            sc.sub_category_sk,
            s.supplier_sk,
            sa.supplier_addr_sk,
            b.brand_sk,
            u.uom_sk
        FROM M_PRODUCT p
        -- ----------------------------------------------------------------
        -- Point-in-time join: pull the version of each master table
        -- that was active ON p_file_date, NOT just is_current = 1.
        -- This is what makes Scenario 1 (catch-up) produce correct
        -- intermediate DIM history for past file dates.
        -- ----------------------------------------------------------------
        JOIN M_CATEGORY c
            ON  c.category_code          = p.category_code
            AND c.effective_start_date  <= p_file_date
            AND c.effective_end_date     > p_file_date
        JOIN M_SUB_CATEGORY sc
            ON  sc.sub_category_code     = p.sub_category_code
            AND sc.effective_start_date <= p_file_date
            AND sc.effective_end_date    > p_file_date
        JOIN M_SUPPLIER s
            ON  s.supplier_code          = p.supplier_code
            AND s.effective_start_date  <= p_file_date
            AND s.effective_end_date     > p_file_date
        JOIN M_SUPPLIER_ADDR sa
            ON  sa.supplier_code         = p.supplier_code
            AND sa.addr_type             = 'PRIMARY'
            AND sa.effective_start_date <= p_file_date
            AND sa.effective_end_date    > p_file_date
        JOIN M_BRAND b
            ON  b.brand_code             = p.brand_code
            AND b.effective_start_date  <= p_file_date
            AND b.effective_end_date     > p_file_date
        JOIN M_UOM u
            ON  u.uom_code               = p.base_uom_code
            AND u.effective_start_date  <= p_file_date
            AND u.effective_end_date     > p_file_date
        -- Only consider products active as of this file date
        WHERE p.effective_start_date  <= p_file_date
          AND p.effective_end_date     > p_file_date;

        COMMIT;
    END BUILD_DIM_STAGING;

    -- =========================================================================
    -- PRIVATE: Detect changes using MINUS between staging and current DIM
    --
    -- MINUS approach: compares ALL non-key columns in one set operation.
    -- Any row in staging that does not exist identically in DIM current
    -- records is treated as a change (new or updated).
    --
    -- Results written to DIM_PRODUCT_CHANGE_LOG (temp tracking table).
    -- =========================================================================
    PROCEDURE DETECT_DIM_CHANGES (
        p_file_date IN DATE,
        p_batch_id  IN NUMBER,
        p_changed_count OUT NUMBER
    ) IS
    BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DIM_PRODUCT_CHANGE_LOG';

        -- Identify product_codes that have changed or are new
        -- MINUS: rows in staging not present identically in current DIM
        INSERT INTO DIM_PRODUCT_CHANGE_LOG (product_code, change_type)
        SELECT product_code,
               CASE
                   WHEN NOT EXISTS (
                       SELECT 1 FROM DIM_PRODUCT d
                       WHERE d.product_code    = stg.product_code
                         AND d.effective_end_date = C_HIGH_DATE  -- current record
                   ) THEN 'INSERT'
                   ELSE 'UPDATE'
               END AS change_type
        FROM (
            -- All columns that define a unique DIM snapshot
            -- MINUS will detect any attribute change across all 7 master tables
            SELECT
                product_code,
                product_name, product_description, product_status,
                launch_date, discontinue_date,
                category_code, category_name, category_description,
                sub_category_code, sub_category_name,
                supplier_code, supplier_name, supplier_contact_name,
                supplier_contact_email, supplier_phone,
                supplier_address_line1, supplier_address_line2,
                supplier_city, supplier_state, supplier_country, supplier_postal_code,
                brand_code, brand_name, brand_owner,
                uom_code, uom_description, uom_conversion_factor
            FROM DIM_PRODUCT_STAGING
            MINUS
            -- Current (active) DIM records
            SELECT
                product_code,
                product_name, product_description, product_status,
                launch_date, discontinue_date,
                category_code, category_name, category_description,
                sub_category_code, sub_category_name,
                supplier_code, supplier_name, supplier_contact_name,
                supplier_contact_email, supplier_phone,
                supplier_address_line1, supplier_address_line2,
                supplier_city, supplier_state, supplier_country, supplier_postal_code,
                brand_code, brand_name, brand_owner,
                uom_code, uom_description, uom_conversion_factor
            FROM DIM_PRODUCT
            WHERE effective_end_date = C_HIGH_DATE
        ) stg;

        p_changed_count := SQL%ROWCOUNT;
        COMMIT;

        LOG_MSG(p_batch_id, 'DETECT_DIM_CHANGES',
                'File date: ' || TO_CHAR(p_file_date, 'YYYY-MM-DD') ||
                ' | Changes detected: ' || p_changed_count);
    END DETECT_DIM_CHANGES;

    -- =========================================================================
    -- PRIVATE: Apply SCD Type 2 to DIM_PRODUCT
    --
    -- Step 1: Expire current DIM records for changed product_codes
    -- Step 2: Insert new DIM versions for changed/new product_codes
    -- =========================================================================
    PROCEDURE APPLY_SCD2_TO_DIM (
        p_file_date     IN DATE,
        p_batch_id      IN NUMBER,
        p_rows_expired  OUT NUMBER,
        p_rows_inserted OUT NUMBER
    ) IS
    BEGIN
        -- ------------------------------------------------------------------
        -- STEP 1: Expire current DIM records that have changed
        --         Set effective_end_date = p_file_date (not today's load date)
        --         This preserves correct business-date history.
        -- ------------------------------------------------------------------
        UPDATE DIM_PRODUCT d
        SET
            d.effective_end_date = p_file_date,
            d.is_current         = 'N',
            d.dw_update_ts       = SYSTIMESTAMP,
            d.expired_by_batch   = p_batch_id
        WHERE d.effective_end_date = C_HIGH_DATE   -- Only touch current records
          AND d.product_code IN (
              SELECT cl.product_code
              FROM DIM_PRODUCT_CHANGE_LOG cl
              WHERE cl.change_type = 'UPDATE'
          );

        p_rows_expired := SQL%ROWCOUNT;

        -- ------------------------------------------------------------------
        -- STEP 2: Insert new DIM versions for all changed/new products
        --         effective_start_date = p_file_date (business date of change)
        --         effective_end_date   = C_HIGH_DATE (open-ended / current)
        -- ------------------------------------------------------------------
        INSERT INTO DIM_PRODUCT (
            dim_product_sk,
            -- Natural key
            product_code,
            -- Denormalized attributes
            product_name, product_description, product_status,
            launch_date, discontinue_date,
            category_code, category_name, category_description,
            sub_category_code, sub_category_name,
            supplier_code, supplier_name, supplier_contact_name,
            supplier_contact_email, supplier_phone,
            supplier_address_line1, supplier_address_line2,
            supplier_city, supplier_state, supplier_country, supplier_postal_code,
            brand_code, brand_name, brand_owner,
            uom_code, uom_description, uom_conversion_factor,
            -- Master surrogate keys (for future change detection optimization)
            m_product_sk, m_category_sk, m_sub_category_sk,
            m_supplier_sk, m_supplier_addr_sk, m_brand_sk, m_uom_sk,
            -- SCD2 control columns
            effective_start_date,
            effective_end_date,
            is_current,
            -- Audit columns
            source_batch_id,
            dw_insert_ts,
            dw_update_ts
        )
        SELECT
            DIM_PRODUCT_SK_SEQ.NEXTVAL,
            stg.product_code,
            stg.product_name, stg.product_description, stg.product_status,
            stg.launch_date, stg.discontinue_date,
            stg.category_code, stg.category_name, stg.category_description,
            stg.sub_category_code, stg.sub_category_name,
            stg.supplier_code, stg.supplier_name, stg.supplier_contact_name,
            stg.supplier_contact_email, stg.supplier_phone,
            stg.supplier_address_line1, stg.supplier_address_line2,
            stg.supplier_city, stg.supplier_state, stg.supplier_country, stg.supplier_postal_code,
            stg.brand_code, stg.brand_name, stg.brand_owner,
            stg.uom_code, stg.uom_description, stg.uom_conversion_factor,
            stg.m_product_sk, stg.m_category_sk, stg.m_sub_category_sk,
            stg.m_supplier_sk, stg.m_supplier_addr_sk, stg.m_brand_sk, stg.m_uom_sk,
            p_file_date,         -- effective_start_date = business date of file
            C_HIGH_DATE,         -- effective_end_date   = open / current
            'Y',
            p_batch_id,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        FROM DIM_PRODUCT_STAGING stg
        WHERE stg.product_code IN (
            SELECT cl.product_code
            FROM DIM_PRODUCT_CHANGE_LOG cl
        );

        p_rows_inserted := SQL%ROWCOUNT;
        COMMIT;

        LOG_MSG(p_batch_id, 'APPLY_SCD2_TO_DIM',
                'File date: ' || TO_CHAR(p_file_date, 'YYYY-MM-DD') ||
                ' | Expired: ' || p_rows_expired ||
                ' | Inserted: ' || p_rows_inserted);
    END APPLY_SCD2_TO_DIM;

    -- =========================================================================
    -- PUBLIC: LOAD_DIM_FOR_FILE_DATE
    -- Orchestrates staging → change detection → SCD2 apply for one file date.
    -- This is the atomic unit called by all three scenarios.
    -- =========================================================================
    PROCEDURE LOAD_DIM_FOR_FILE_DATE (
        p_file_date     IN DATE,
        p_batch_id      IN NUMBER,
        p_called_by     IN VARCHAR2 DEFAULT 'SCHEDULER'
    ) IS
        v_changed_count  NUMBER := 0;
        v_rows_expired   NUMBER := 0;
        v_rows_inserted  NUMBER := 0;
        v_proc           CONSTANT VARCHAR2(50) := 'LOAD_DIM_FOR_FILE_DATE';
    BEGIN
        LOG_MSG(p_batch_id, v_proc,
                'START | file_date=' || TO_CHAR(p_file_date,'YYYY-MM-DD') ||
                ' | called_by=' || p_called_by);

        UPDATE_BATCH_STATUS(p_batch_id, p_file_date, 'RUNNING');

        -- Step 1: Build point-in-time denormalized staging snapshot
        BUILD_DIM_STAGING(p_file_date);
        LOG_MSG(p_batch_id, v_proc, 'Staging built for ' || TO_CHAR(p_file_date,'YYYY-MM-DD'));

        -- Step 2: Detect changes via MINUS
        DETECT_DIM_CHANGES(p_file_date, p_batch_id, v_changed_count);

        -- Step 3: Apply SCD Type 2 only if changes exist (skip for no-op days)
        IF v_changed_count > 0 THEN
            APPLY_SCD2_TO_DIM(p_file_date, p_batch_id, v_rows_expired, v_rows_inserted);
        ELSE
            LOG_MSG(p_batch_id, v_proc, 'No DIM changes detected. Skipping SCD2 apply.');
        END IF;

        UPDATE_BATCH_STATUS(p_batch_id, p_file_date, C_STATUS_DONE,
                            v_rows_inserted + v_rows_expired);

        LOG_MSG(p_batch_id, v_proc,
                'END | changed=' || v_changed_count ||
                ' | expired=' || v_rows_expired ||
                ' | inserted=' || v_rows_inserted);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            UPDATE_BATCH_STATUS(p_batch_id, p_file_date, C_STATUS_FAIL,
                                0, SUBSTR(SQLERRM, 1, 4000));
            LOG_MSG(p_batch_id, v_proc,
                    'FAILED: ' || SQLERRM, 'ERROR');
            RAISE;  -- Re-raise so orchestrator knows this file date failed
    END LOAD_DIM_FOR_FILE_DATE;

    -- =========================================================================
    -- PUBLIC: CATCHUP_DIM_LOAD  (Scenario 1)
    --
    -- Finds all file dates where:
    --   - Master layer is COMPLETED
    --   - DIM layer is PENDING or missing
    --   - file_date is between p_from_date and p_to_date
    --
    -- Processes each in strict chronological order.
    -- Order matters: a product may change on day 10 and again on day 12.
    -- Processing out of order would produce wrong effective date ranges.
    -- =========================================================================
    PROCEDURE CATCHUP_DIM_LOAD (
        p_from_date IN DATE DEFAULT NULL,
        p_to_date   IN DATE DEFAULT TRUNC(SYSDATE)
    ) IS
        v_proc      CONSTANT VARCHAR2(50) := 'CATCHUP_DIM_LOAD';
        v_from_date DATE;

        -- Cursor: file dates with Master done but DIM not done, ordered strictly
        CURSOR c_pending_dates IS
            SELECT bc_master.file_date,
                   bc_master.batch_id
            FROM DW_BATCH_CONTROL bc_master
            WHERE bc_master.layer            = C_MASTER_LAYER
              AND bc_master.status           = C_STATUS_DONE
              AND bc_master.file_date       >= v_from_date
              AND bc_master.file_date       <= p_to_date
              AND NOT EXISTS (
                  SELECT 1
                  FROM DW_BATCH_CONTROL bc_dim
                  WHERE bc_dim.layer      = C_DIM_LAYER
                    AND bc_dim.batch_id   = bc_master.batch_id
                    AND bc_dim.status     = C_STATUS_DONE
              )
            ORDER BY bc_master.file_date ASC;  -- CRITICAL: strict chronological order

    BEGIN
        -- Auto-detect start date if not provided: earliest unprocessed DIM date
        IF p_from_date IS NULL THEN
            SELECT NVL(MIN(bc_master.file_date), p_to_date)
            INTO v_from_date
            FROM DW_BATCH_CONTROL bc_master
            WHERE bc_master.layer  = C_MASTER_LAYER
              AND bc_master.status = C_STATUS_DONE
              AND NOT EXISTS (
                  SELECT 1 FROM DW_BATCH_CONTROL bc_dim
                  WHERE bc_dim.layer    = C_DIM_LAYER
                    AND bc_dim.batch_id = bc_master.batch_id
                    AND bc_dim.status   = C_STATUS_DONE
              );
        ELSE
            v_from_date := p_from_date;
        END IF;

        LOG_MSG(-1, v_proc,
                'Catch-up START | from=' || TO_CHAR(v_from_date,'YYYY-MM-DD') ||
                ' | to=' || TO_CHAR(p_to_date,'YYYY-MM-DD'));

        -- Process each pending file date sequentially
        FOR r IN c_pending_dates LOOP
            BEGIN
                LOG_MSG(r.batch_id, v_proc,
                        'Processing catch-up for file_date=' ||
                        TO_CHAR(r.file_date,'YYYY-MM-DD'));

                LOAD_DIM_FOR_FILE_DATE(
                    p_file_date  => r.file_date,
                    p_batch_id   => r.batch_id,
                    p_called_by  => 'CATCHUP'
                );

            EXCEPTION
                WHEN OTHERS THEN
                    -- Log and stop: do not skip a date, as subsequent dates
                    -- depend on correct DIM state from this date onward.
                    LOG_MSG(r.batch_id, v_proc,
                            'Catch-up ABORTED at file_date=' ||
                            TO_CHAR(r.file_date,'YYYY-MM-DD') ||
                            ' | Error: ' || SQLERRM, 'ERROR');
                    RAISE;  -- Stop the loop; fix the issue and re-run catch-up
            END;
        END LOOP;

        LOG_MSG(-1, v_proc, 'Catch-up COMPLETE');
    END CATCHUP_DIM_LOAD;

    -- =========================================================================
    -- PUBLIC: REPLAY_DIM_FROM_DATE  (Scenario 2)
    --
    -- Used when corrupt data was loaded starting from p_from_date.
    -- Assumes Master layer has already been corrected and replayed.
    --
    -- Steps:
    --   1. ROLLBACK DIM: Remove all DIM records inserted/modified from
    --      p_from_date onward. Re-open records that were expired on that date.
    --   2. REPLAY: Re-run LOAD_DIM_FOR_FILE_DATE for each file date in order.
    --
    -- WARNING: This procedure is destructive for the date range specified.
    --          Ensure Master layer correction is complete before calling.
    -- =========================================================================
    PROCEDURE REPLAY_DIM_FROM_DATE (
        p_from_date IN DATE,
        p_to_date   IN DATE DEFAULT TRUNC(SYSDATE)
    ) IS
        v_proc          CONSTANT VARCHAR2(50) := 'REPLAY_DIM_FROM_DATE';
        v_rows_deleted  NUMBER := 0;
        v_rows_reopened NUMBER := 0;

        CURSOR c_replay_dates IS
            SELECT bc.file_date, bc.batch_id
            FROM DW_BATCH_CONTROL bc
            WHERE bc.layer     = C_MASTER_LAYER
              AND bc.status    = C_STATUS_DONE
              AND bc.file_date >= p_from_date
              AND bc.file_date <= p_to_date
            ORDER BY bc.file_date ASC;

    BEGIN
        LOG_MSG(-1, v_proc,
                'Replay START | from=' || TO_CHAR(p_from_date,'YYYY-MM-DD') ||
                ' | to='   || TO_CHAR(p_to_date,'YYYY-MM-DD'));

        -- ------------------------------------------------------------------
        -- STEP 1: ROLLBACK DIM to state just before p_from_date
        -- ------------------------------------------------------------------

        -- 1a. Delete all DIM records whose life started on or after p_from_date
        --     These are records that were created by the corrupt load or
        --     subsequent loads that built on top of corrupt state.
        DELETE FROM DIM_PRODUCT
        WHERE effective_start_date >= p_from_date;

        v_rows_deleted := SQL%ROWCOUNT;
        LOG_MSG(-1, v_proc, 'Deleted DIM records with effective_start_date >= ' ||
                TO_CHAR(p_from_date,'YYYY-MM-DD') || ' | Count: ' || v_rows_deleted);

        -- 1b. Re-open records that were expired BY the corrupt load or any
        --     subsequent load in the replay range.
        --     These were correctly valid before p_from_date and should be
        --     current again before we replay.
        UPDATE DIM_PRODUCT
        SET
            effective_end_date = C_HIGH_DATE,
            is_current         = 'Y',
            dw_update_ts       = SYSTIMESTAMP
        WHERE effective_end_date >= p_from_date   -- Was expired in the corrupt window
          AND effective_end_date <  C_HIGH_DATE   -- Was not already open-ended
          AND effective_start_date < p_from_date; -- Record genuinely predates corruption

        v_rows_reopened := SQL%ROWCOUNT;
        LOG_MSG(-1, v_proc, 'Re-opened DIM records expired in corrupt window | Count: ' || v_rows_reopened);

        -- 1c. Reset DIM batch control status for all dates in replay window
        --     so LOAD_DIM_FOR_FILE_DATE will not see them as already done
        UPDATE DW_BATCH_CONTROL
        SET
            status     = C_STATUS_PEND,
            updated_ts = SYSTIMESTAMP
        WHERE layer     = C_DIM_LAYER
          AND file_date >= p_from_date
          AND file_date <= p_to_date;

        COMMIT;

        LOG_MSG(-1, v_proc, 'DIM rollback complete. Beginning replay.');

        -- ------------------------------------------------------------------
        -- STEP 2: REPLAY - process each corrected file date end-to-end
        --         Same logic as catch-up; each date is self-contained.
        -- ------------------------------------------------------------------
        FOR r IN c_replay_dates LOOP
            BEGIN
                LOG_MSG(r.batch_id, v_proc,
                        'Replaying file_date=' || TO_CHAR(r.file_date,'YYYY-MM-DD'));

                LOAD_DIM_FOR_FILE_DATE(
                    p_file_date  => r.file_date,
                    p_batch_id   => r.batch_id,
                    p_called_by  => 'REPLAY'
                );

            EXCEPTION
                WHEN OTHERS THEN
                    LOG_MSG(r.batch_id, v_proc,
                            'Replay ABORTED at file_date=' ||
                            TO_CHAR(r.file_date,'YYYY-MM-DD') ||
                            ' | Error: ' || SQLERRM, 'ERROR');
                    RAISE;
            END;
        END LOOP;

        LOG_MSG(-1, v_proc, 'Replay COMPLETE | from=' ||
                TO_CHAR(p_from_date,'YYYY-MM-DD') || ' to=' ||
                TO_CHAR(p_to_date,'YYYY-MM-DD'));

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            LOG_MSG(-1, v_proc, 'Replay FAILED during rollback phase: ' || SQLERRM, 'ERROR');
            RAISE;
    END REPLAY_DIM_FROM_DATE;

END PKG_DIM_PRODUCT_LOAD;
/


/******************************************************************************
 * SUPPORTING DDL
 * Run these once before deploying the package.
 ******************************************************************************/

-- Staging table: point-in-time denormalized snapshot for current file date
CREATE GLOBAL TEMPORARY TABLE DIM_PRODUCT_STAGING (
    product_code              VARCHAR2(50),
    product_name              VARCHAR2(200),
    product_description       VARCHAR2(1000),
    product_status            VARCHAR2(20),
    launch_date               DATE,
    discontinue_date          DATE,
    category_code             VARCHAR2(50),
    category_name             VARCHAR2(200),
    category_description      VARCHAR2(500),
    sub_category_code         VARCHAR2(50),
    sub_category_name         VARCHAR2(200),
    supplier_code             VARCHAR2(50),
    supplier_name             VARCHAR2(200),
    supplier_contact_name     VARCHAR2(200),
    supplier_contact_email    VARCHAR2(200),
    supplier_phone            VARCHAR2(50),
    supplier_address_line1    VARCHAR2(200),
    supplier_address_line2    VARCHAR2(200),
    supplier_city             VARCHAR2(100),
    supplier_state            VARCHAR2(100),
    supplier_country          VARCHAR2(100),
    supplier_postal_code      VARCHAR2(20),
    brand_code                VARCHAR2(50),
    brand_name                VARCHAR2(200),
    brand_owner               VARCHAR2(200),
    uom_code                  VARCHAR2(20),
    uom_description           VARCHAR2(200),
    uom_conversion_factor     NUMBER(18,6),
    -- Master surrogate keys
    m_product_sk              NUMBER,
    m_category_sk             NUMBER,
    m_sub_category_sk         NUMBER,
    m_supplier_sk             NUMBER,
    m_supplier_addr_sk        NUMBER,
    m_brand_sk                NUMBER,
    m_uom_sk                  NUMBER
) ON COMMIT DELETE ROWS;


-- Change detection log: product_codes with detected changes for current run
CREATE GLOBAL TEMPORARY TABLE DIM_PRODUCT_CHANGE_LOG (
    product_code   VARCHAR2(50),
    change_type    VARCHAR2(10)   -- 'INSERT' or 'UPDATE'
) ON COMMIT DELETE ROWS;


-- Batch control table (shared across Master and DIM layers)
CREATE TABLE DW_BATCH_CONTROL (
    batch_id          NUMBER         NOT NULL,
    file_date         DATE           NOT NULL,
    layer             VARCHAR2(20)   NOT NULL,   -- 'MASTER' or 'DIM'
    status            VARCHAR2(20)   NOT NULL,   -- PENDING / RUNNING / COMPLETED / FAILED
    rows_processed    NUMBER         DEFAULT 0,
    error_message     VARCHAR2(4000),
    created_ts        TIMESTAMP      DEFAULT SYSTIMESTAMP,
    updated_ts        TIMESTAMP,
    CONSTRAINT pk_dw_batch_control PRIMARY KEY (batch_id, file_date, layer)
);

-- Load log table
CREATE TABLE DW_LOAD_LOG (
    log_id            NUMBER         NOT NULL,
    batch_id          NUMBER,
    layer             VARCHAR2(20),
    procedure_name    VARCHAR2(100),
    log_message       VARCHAR2(4000),
    severity          VARCHAR2(10)   DEFAULT 'INFO',
    log_timestamp     TIMESTAMP      DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_dw_load_log PRIMARY KEY (log_id)
);

CREATE SEQUENCE DW_LOAD_LOG_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE DIM_PRODUCT_SK_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;


-- DIM_PRODUCT table (SCD Type 2)
CREATE TABLE DIM_PRODUCT (
    dim_product_sk            NUMBER         NOT NULL,
    product_code              VARCHAR2(50)   NOT NULL,
    product_name              VARCHAR2(200),
    product_description       VARCHAR2(1000),
    product_status            VARCHAR2(20),
    launch_date               DATE,
    discontinue_date          DATE,
    category_code             VARCHAR2(50),
    category_name             VARCHAR2(200),
    category_description      VARCHAR2(500),
    sub_category_code         VARCHAR2(50),
    sub_category_name         VARCHAR2(200),
    supplier_code             VARCHAR2(50),
    supplier_name             VARCHAR2(200),
    supplier_contact_name     VARCHAR2(200),
    supplier_contact_email    VARCHAR2(200),
    supplier_phone            VARCHAR2(50),
    supplier_address_line1    VARCHAR2(200),
    supplier_address_line2    VARCHAR2(200),
    supplier_city             VARCHAR2(100),
    supplier_state            VARCHAR2(100),
    supplier_country          VARCHAR2(100),
    supplier_postal_code      VARCHAR2(20),
    brand_code                VARCHAR2(50),
    brand_name                VARCHAR2(200),
    brand_owner               VARCHAR2(200),
    uom_code                  VARCHAR2(20),
    uom_description           VARCHAR2(200),
    uom_conversion_factor     NUMBER(18,6),
    -- Master surrogate key references (for change lineage)
    m_product_sk              NUMBER,
    m_category_sk             NUMBER,
    m_sub_category_sk         NUMBER,
    m_supplier_sk             NUMBER,
    m_supplier_addr_sk        NUMBER,
    m_brand_sk                NUMBER,
    m_uom_sk                  NUMBER,
    -- SCD2 columns
    effective_start_date      DATE           NOT NULL,
    effective_end_date        DATE           NOT NULL,
    is_current                CHAR(1)        DEFAULT 'Y',
    -- Audit columns
    source_batch_id           NUMBER,
    expired_by_batch          NUMBER,
    dw_insert_ts              TIMESTAMP      DEFAULT SYSTIMESTAMP,
    dw_update_ts              TIMESTAMP,
    CONSTRAINT pk_dim_product PRIMARY KEY (dim_product_sk)
);

-- Performance indexes
CREATE INDEX idx_dim_product_code    ON DIM_PRODUCT (product_code, effective_end_date);
CREATE INDEX idx_dim_product_current ON DIM_PRODUCT (product_code) WHERE is_current = 'Y';
CREATE INDEX idx_dim_product_dates   ON DIM_PRODUCT (effective_start_date, effective_end_date);
