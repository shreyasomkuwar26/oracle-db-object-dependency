/******************************************************************************
 * PACKAGE : PKG_DIM_LOAD
 * PURPOSE : Loads and maintains SCD Type 2 for DIM_PRODUCT by denormalizing
 *           all 7 Master tables. Depends on PKG_MASTER_LOAD having completed
 *           successfully for the same file_date before this runs.
 *
 * CHANGE DETECTION STRATEGY:
 *   Step 1 - Fan-out resolver: reads DW_MASTER_CHANGE_LOG (populated by
 *            PKG_MASTER_LOAD) to identify which product_codes are affected
 *            by changes in any of the 7 master tables. Only affected
 *            product_codes are processed further.
 *
 *   Step 2 - Row hash comparison: builds a denormalized snapshot for affected
 *            product_codes only, computes dim_row_hash, compares against the
 *            stored hash in current DIM_PRODUCT records. Only true attribute
 *            changes trigger SCD2 versioning.
 *
 *   This two-stage approach means on a typical day only a small fraction of
 *   the product catalog is compared or touched, regardless of DIM size.
 *
 * PUBLIC PROCEDURES:
 *   LOAD_DIM_FOR_FILE_DATE   - Normal daily entry point (called after Master)
 *   CATCHUP_DIM_LOAD         - Scenario 1: process missed file dates
 *   REPLAY_DIM_FROM_DATE     - Scenario 2: rollback + replay from a date
 *
 * AUTHOR : <Your Name>
 * DATE   : 2024
 ******************************************************************************/

CREATE OR REPLACE PACKAGE PKG_DIM_LOAD AS

    PROCEDURE LOAD_DIM_FOR_FILE_DATE (
        p_file_date  IN DATE,
        p_batch_id   IN NUMBER,
        p_called_by  IN VARCHAR2 DEFAULT 'SCHEDULER'
    );

    PROCEDURE CATCHUP_DIM_LOAD (
        p_from_date  IN DATE DEFAULT NULL,
        p_to_date    IN DATE DEFAULT TRUNC(SYSDATE)
    );

    PROCEDURE REPLAY_DIM_FROM_DATE (
        p_from_date  IN DATE,
        p_to_date    IN DATE DEFAULT TRUNC(SYSDATE)
    );

END PKG_DIM_LOAD;
/


CREATE OR REPLACE PACKAGE BODY PKG_DIM_LOAD AS

    -- =========================================================================
    -- PRIVATE CONSTANTS
    -- =========================================================================
    C_HIGH_DATE     CONSTANT DATE        := DATE '9999-12-31';
    C_DIM_LAYER     CONSTANT VARCHAR2(20) := 'DIM';
    C_MASTER_LAYER  CONSTANT VARCHAR2(20) := 'MASTER';
    C_STATUS_DONE   CONSTANT VARCHAR2(20) := 'COMPLETED';
    C_STATUS_FAIL   CONSTANT VARCHAR2(20) := 'FAILED';
    C_STATUS_PEND   CONSTANT VARCHAR2(20) := 'PENDING';
    C_STATUS_RUN    CONSTANT VARCHAR2(20) := 'RUNNING';
    C_DELIM         CONSTANT VARCHAR2(1)  := CHR(1);
    C_NULL_SEN      CONSTANT VARCHAR2(10) := '~NULL~';

    -- =========================================================================
    -- PRIVATE: Autonomous logger
    -- =========================================================================
    PROCEDURE LOG_MSG (
        p_batch_id IN NUMBER,
        p_proc     IN VARCHAR2,
        p_msg      IN VARCHAR2,
        p_severity IN VARCHAR2 DEFAULT 'INFO'
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO DW_LOAD_LOG (
            log_id, batch_id, layer, procedure_name,
            log_message, severity, log_timestamp
        ) VALUES (
            DW_LOAD_LOG_SEQ.NEXTVAL, p_batch_id, C_DIM_LAYER,
            p_proc, SUBSTR(p_msg,1,4000), p_severity, SYSTIMESTAMP
        );
        COMMIT;
    EXCEPTION WHEN OTHERS THEN NULL;
    END LOG_MSG;

    -- =========================================================================
    -- PRIVATE: Upsert batch control for DIM layer
    -- =========================================================================
    PROCEDURE UPDATE_BATCH_STATUS (
        p_batch_id  IN NUMBER,
        p_file_date IN DATE,
        p_status    IN VARCHAR2,
        p_rows      IN NUMBER   DEFAULT 0,
        p_error     IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        MERGE INTO DW_BATCH_CONTROL tgt
        USING (SELECT p_batch_id AS batch_id, p_file_date AS file_date,
                      C_DIM_LAYER AS layer FROM DUAL) src
        ON (tgt.batch_id = src.batch_id AND tgt.file_date = src.file_date
            AND tgt.layer = src.layer)
        WHEN MATCHED THEN UPDATE SET
            tgt.status         = p_status,
            tgt.rows_processed = p_rows,
            tgt.error_message  = SUBSTR(p_error,1,4000),
            tgt.updated_ts     = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            batch_id, file_date, layer, status,
            rows_processed, error_message, created_ts, updated_ts
        ) VALUES (
            p_batch_id, p_file_date, C_DIM_LAYER, p_status,
            p_rows, SUBSTR(p_error,1,4000), SYSTIMESTAMP, SYSTIMESTAMP
        );
    END UPDATE_BATCH_STATUS;

    -- =========================================================================
    -- PRIVATE: NULL-safe hash helpers
    -- =========================================================================
    FUNCTION H (p_val IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN RETURN NVL(p_val, C_NULL_SEN); END H;

    FUNCTION H (p_val IN DATE) RETURN VARCHAR2 IS
    BEGIN RETURN NVL(TO_CHAR(p_val,'YYYY-MM-DD'), C_NULL_SEN); END H;

    FUNCTION H (p_val IN NUMBER) RETURN VARCHAR2 IS
    BEGIN RETURN NVL(TO_CHAR(p_val), C_NULL_SEN); END H;

    -- =========================================================================
    -- PRIVATE: RESOLVE_AFFECTED_PRODUCT_CODES
    --
    -- Fan-out resolver: maps changes in any of the 7 master tables back to
    -- the set of product_codes whose DIM record may need a new version.
    --
    -- Uses DW_MASTER_CHANGE_LOG populated by PKG_MASTER_LOAD.
    -- Only DIM-contributing changes are in that log (dim_contributing_hash
    -- filter applied at Master load time), so no spurious fan-outs here.
    --
    -- Results stored in DIM_AFFECTED_KEYS (GTT, session-scoped).
    -- =========================================================================
    PROCEDURE RESOLVE_AFFECTED_PRODUCT_CODES (
        p_file_date    IN DATE,
        p_batch_id     IN NUMBER,
        p_affected_cnt OUT NUMBER
    ) IS
        v_proc CONSTANT VARCHAR2(50) := 'RESOLVE_AFFECTED_PRODUCT_CODES';
    BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DIM_AFFECTED_KEYS';

        INSERT INTO DIM_AFFECTED_KEYS (product_code)

        -- 1. Direct product changes (product itself changed in Master)
        SELECT mcl.natural_key AS product_code
        FROM DW_MASTER_CHANGE_LOG mcl
        WHERE mcl.batch_id     = p_batch_id
          AND mcl.master_table = 'M_PRODUCT'

        UNION

        -- 2. Category changed -> fan out to all current products in that category
        SELECT mp.product_code
        FROM DW_MASTER_CHANGE_LOG mcl
        JOIN M_PRODUCT mp
            ON  mp.category_code      = mcl.natural_key
            AND mp.effective_end_date = C_HIGH_DATE
            AND mp.is_current         = 'Y'
        WHERE mcl.batch_id     = p_batch_id
          AND mcl.master_table = 'M_CATEGORY'

        UNION

        -- 3. Sub-category changed -> fan out via sub_category_code
        SELECT mp.product_code
        FROM DW_MASTER_CHANGE_LOG mcl
        JOIN M_PRODUCT mp
            ON  mp.sub_category_code  = mcl.natural_key
            AND mp.effective_end_date = C_HIGH_DATE
            AND mp.is_current         = 'Y'
        WHERE mcl.batch_id     = p_batch_id
          AND mcl.master_table = 'M_SUB_CATEGORY'

        UNION

        -- 4. Supplier changed -> fan out via supplier_code
        SELECT mp.product_code
        FROM DW_MASTER_CHANGE_LOG mcl
        JOIN M_PRODUCT mp
            ON  mp.supplier_code      = mcl.natural_key
            AND mp.effective_end_date = C_HIGH_DATE
            AND mp.is_current         = 'Y'
        WHERE mcl.batch_id     = p_batch_id
          AND mcl.master_table = 'M_SUPPLIER'

        UNION

        -- 5. Supplier address changed -> natural_key is 'supplier_code|addr_type'
        --    Extract supplier_code by splitting on pipe
        SELECT mp.product_code
        FROM DW_MASTER_CHANGE_LOG mcl
        JOIN M_PRODUCT mp
            ON  mp.supplier_code      = SUBSTR(mcl.natural_key, 1,
                                            INSTR(mcl.natural_key,'|') - 1)
            AND mp.effective_end_date = C_HIGH_DATE
            AND mp.is_current         = 'Y'
        WHERE mcl.batch_id     = p_batch_id
          AND mcl.master_table = 'M_SUPPLIER_ADDR'

        UNION

        -- 6. Brand changed -> fan out via brand_code
        SELECT mp.product_code
        FROM DW_MASTER_CHANGE_LOG mcl
        JOIN M_PRODUCT mp
            ON  mp.brand_code         = mcl.natural_key
            AND mp.effective_end_date = C_HIGH_DATE
            AND mp.is_current         = 'Y'
        WHERE mcl.batch_id     = p_batch_id
          AND mcl.master_table = 'M_BRAND'

        UNION

        -- 7. UOM changed -> fan out via base_uom_code
        SELECT mp.product_code
        FROM DW_MASTER_CHANGE_LOG mcl
        JOIN M_PRODUCT mp
            ON  mp.base_uom_code      = mcl.natural_key
            AND mp.effective_end_date = C_HIGH_DATE
            AND mp.is_current         = 'Y'
        WHERE mcl.batch_id     = p_batch_id
          AND mcl.master_table = 'M_UOM';

        p_affected_cnt := SQL%ROWCOUNT;
        COMMIT;

        LOG_MSG(p_batch_id, v_proc,
            'file_date='||TO_CHAR(p_file_date,'YYYY-MM-DD')||
            ' affected_product_codes='||p_affected_cnt);
    END RESOLVE_AFFECTED_PRODUCT_CODES;

    -- =========================================================================
    -- PRIVATE: BUILD_DIM_STAGING
    --
    -- Builds a point-in-time denormalized snapshot for affected product_codes
    -- only (pre-filtered by DIM_AFFECTED_KEYS).
    --
    -- CRITICAL: All 7 master table joins use point-in-time predicates:
    --     effective_start_date <= p_file_date AND effective_end_date > p_file_date
    -- This ensures correct intermediate history during catch-up (Scenario 1)
    -- and replay (Scenario 2) when past file dates are processed retroactively.
    --
    -- Also computes dim_row_hash covering all DIM-visible attributes across
    -- all 7 tables in one shot. This single hash is what we compare to the
    -- stored DIM hash in the change detection step.
    -- =========================================================================
    PROCEDURE BUILD_DIM_STAGING (
        p_file_date IN DATE,
        p_batch_id  IN NUMBER,
        p_built_cnt OUT NUMBER
    ) IS
        v_proc CONSTANT VARCHAR2(50) := 'BUILD_DIM_STAGING';
    BEGIN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE DIM_PRODUCT_STAGING';

        INSERT /*+ APPEND */ INTO DIM_PRODUCT_STAGING (
            product_code,
            product_name, product_description, product_status,
            launch_date, discontinue_date,
            category_code, category_name, category_description, category_status,
            sub_category_code, sub_category_name, sub_category_description, sub_category_status,
            supplier_code, supplier_name, supplier_contact_name,
            supplier_contact_email, supplier_phone, supplier_status,
            supplier_address_line1, supplier_address_line2,
            supplier_city, supplier_state, supplier_country, supplier_postal_code,
            brand_code, brand_name, brand_owner, brand_status,
            uom_code, uom_description, uom_conversion_factor, uom_base_uom_code,
            -- Master surrogate keys stored for lineage/audit
            m_product_sk, m_category_sk, m_sub_category_sk,
            m_supplier_sk, m_supplier_addr_sk, m_brand_sk, m_uom_sk,
            -- Single hash of ALL DIM-visible attributes across all 7 tables
            dim_row_hash
        )
        SELECT
            p.product_code,
            p.product_name, p.product_description, p.product_status,
            p.launch_date, p.discontinue_date,
            c.category_code, c.category_name, c.category_description, c.category_status,
            sc.sub_category_code, sc.sub_category_name, sc.sub_category_description, sc.sub_category_status,
            s.supplier_code, s.supplier_name, s.contact_name,
            s.contact_email, s.phone, s.supplier_status,
            sa.address_line1, sa.address_line2,
            sa.city, sa.state, sa.country, sa.postal_code,
            b.brand_code, b.brand_name, b.brand_owner, b.brand_status,
            u.uom_code, u.uom_description, u.conversion_factor, u.base_uom_code,
            p.product_sk, c.category_sk, sc.sub_category_sk,
            s.supplier_sk, sa.supplier_addr_sk, b.brand_sk, u.uom_sk,
            -- Composite DIM row hash: covers every attribute visible in DIM_PRODUCT
            -- If ANY of these change across ANY of the 7 master tables, hash changes
            STANDARD_HASH(
                H(p.product_name)        || C_DELIM || H(p.product_description) || C_DELIM ||
                H(p.product_status)      || C_DELIM || H(p.launch_date)          || C_DELIM ||
                H(p.discontinue_date)    || C_DELIM ||
                H(c.category_code)       || C_DELIM || H(c.category_name)        || C_DELIM ||
                H(c.category_description)|| C_DELIM || H(c.category_status)      || C_DELIM ||
                H(sc.sub_category_code)  || C_DELIM || H(sc.sub_category_name)   || C_DELIM ||
                H(sc.sub_category_description) || C_DELIM || H(sc.sub_category_status) || C_DELIM ||
                H(s.supplier_name)       || C_DELIM || H(s.contact_name)         || C_DELIM ||
                H(s.contact_email)       || C_DELIM || H(s.phone)                || C_DELIM ||
                H(s.supplier_status)     || C_DELIM ||
                H(sa.address_line1)      || C_DELIM || H(sa.address_line2)       || C_DELIM ||
                H(sa.city)               || C_DELIM || H(sa.state)               || C_DELIM ||
                H(sa.country)            || C_DELIM || H(sa.postal_code)         || C_DELIM ||
                H(b.brand_name)          || C_DELIM || H(b.brand_owner)          || C_DELIM ||
                H(b.brand_status)        || C_DELIM ||
                H(u.uom_description)     || C_DELIM || H(u.conversion_factor)    || C_DELIM ||
                H(u.base_uom_code),
                'SHA256'
            )
        FROM M_PRODUCT p
        -- ---------------------------------------------------------------
        -- Point-in-time joins: pull the version of each master table
        -- active ON p_file_date. Critical for catch-up and replay.
        -- Using strict < on effective_end_date handles records whose
        -- end date equals exactly p_file_date (they expired that day).
        -- ---------------------------------------------------------------
        JOIN M_CATEGORY c
            ON  c.category_code        = p.category_code
            AND c.effective_start_date <= p_file_date
            AND c.effective_end_date   >  p_file_date
        JOIN M_SUB_CATEGORY sc
            ON  sc.sub_category_code   = p.sub_category_code
            AND sc.effective_start_date <= p_file_date
            AND sc.effective_end_date  >  p_file_date
        JOIN M_SUPPLIER s
            ON  s.supplier_code        = p.supplier_code
            AND s.effective_start_date <= p_file_date
            AND s.effective_end_date   >  p_file_date
        JOIN M_SUPPLIER_ADDR sa
            ON  sa.supplier_code       = p.supplier_code
            AND sa.addr_type           = 'PRIMARY'
            AND sa.effective_start_date <= p_file_date
            AND sa.effective_end_date  >  p_file_date
        JOIN M_BRAND b
            ON  b.brand_code           = p.brand_code
            AND b.effective_start_date <= p_file_date
            AND b.effective_end_date   >  p_file_date
        JOIN M_UOM u
            ON  u.uom_code             = p.base_uom_code
            AND u.effective_start_date <= p_file_date
            AND u.effective_end_date   >  p_file_date
        -- Products active on this file date
        WHERE p.effective_start_date <= p_file_date
          AND p.effective_end_date   >  p_file_date
          -- KEY FILTER: only process affected product_codes identified by resolver
          AND p.product_code IN (SELECT ak.product_code FROM DIM_AFFECTED_KEYS ak);

        p_built_cnt := SQL%ROWCOUNT;
        COMMIT;

        LOG_MSG(p_batch_id, 'BUILD_DIM_STAGING',
            'file_date='||TO_CHAR(p_file_date,'YYYY-MM-DD')||
            ' staging_rows='||p_built_cnt);
    END BUILD_DIM_STAGING;

    -- =========================================================================
    -- PRIVATE: DETECT_AND_APPLY_SCD2
    --
    -- Single procedure: detects changes via dim_row_hash comparison and
    -- immediately applies SCD2 in the same pass.
    --
    -- Step 1: Expire current DIM records where dim_row_hash differs from staging.
    -- Step 2: Insert new DIM versions for changed + new product_codes.
    --
    -- Combined into one procedure to avoid a separate change log table and
    -- reduce round trips. Staging already holds only affected product_codes.
    -- =========================================================================
    PROCEDURE DETECT_AND_APPLY_SCD2 (
        p_file_date     IN DATE,
        p_batch_id      IN NUMBER,
        p_rows_expired  OUT NUMBER,
        p_rows_inserted OUT NUMBER
    ) IS
        v_proc CONSTANT VARCHAR2(50) := 'DETECT_AND_APPLY_SCD2';
    BEGIN
        -- ------------------------------------------------------------------
        -- STEP 1: Expire current DIM records whose hash differs from staging.
        -- Hash comparison is a single indexed column lookup - very fast.
        -- New product_codes (INSERT) have no current DIM record to expire.
        -- ------------------------------------------------------------------
        UPDATE DIM_PRODUCT dp
        SET
            dp.effective_end_date = p_file_date,
            dp.is_current         = 'N',
            dp.dw_update_ts       = SYSTIMESTAMP,
            dp.expired_by_batch   = p_batch_id
        WHERE dp.effective_end_date = C_HIGH_DATE
          AND dp.is_current         = 'Y'
          AND dp.product_code IN (SELECT stg.product_code FROM DIM_PRODUCT_STAGING stg)
          AND EXISTS (
              -- Hash mismatch: DIM attributes differ from new denormalized snapshot
              SELECT 1 FROM DIM_PRODUCT_STAGING stg
              WHERE stg.product_code  = dp.product_code
                AND stg.dim_row_hash != dp.dim_row_hash
          );

        p_rows_expired := SQL%ROWCOUNT;
        LOG_MSG(p_batch_id, v_proc, 'Expired='||p_rows_expired);

        -- ------------------------------------------------------------------
        -- STEP 2: Insert new DIM versions for:
        --   a) Updated products (current record just expired above)
        --   b) Brand new products (no DIM record existed at all)
        -- Exclude product_codes where hash is unchanged (no DIM change needed).
        -- ------------------------------------------------------------------
        INSERT INTO DIM_PRODUCT (
            dim_product_sk,
            product_code,
            product_name, product_description, product_status,
            launch_date, discontinue_date,
            category_code, category_name, category_description, category_status,
            sub_category_code, sub_category_name, sub_category_description, sub_category_status,
            supplier_code, supplier_name, supplier_contact_name,
            supplier_contact_email, supplier_phone, supplier_status,
            supplier_address_line1, supplier_address_line2,
            supplier_city, supplier_state, supplier_country, supplier_postal_code,
            brand_code, brand_name, brand_owner, brand_status,
            uom_code, uom_description, uom_conversion_factor, uom_base_uom_code,
            -- Master surrogate keys for lineage
            m_product_sk, m_category_sk, m_sub_category_sk,
            m_supplier_sk, m_supplier_addr_sk, m_brand_sk, m_uom_sk,
            -- Stored hash for next run's change detection
            dim_row_hash,
            -- SCD2 control
            effective_start_date, effective_end_date, is_current,
            -- Audit
            source_batch_id, dw_insert_ts, dw_update_ts
        )
        SELECT
            DIM_PRODUCT_SK_SEQ.NEXTVAL,
            stg.product_code,
            stg.product_name, stg.product_description, stg.product_status,
            stg.launch_date, stg.discontinue_date,
            stg.category_code, stg.category_name, stg.category_description, stg.category_status,
            stg.sub_category_code, stg.sub_category_name, stg.sub_category_description, stg.sub_category_status,
            stg.supplier_code, stg.supplier_name, stg.supplier_contact_name,
            stg.supplier_contact_email, stg.supplier_phone, stg.supplier_status,
            stg.supplier_address_line1, stg.supplier_address_line2,
            stg.supplier_city, stg.supplier_state, stg.supplier_country, stg.supplier_postal_code,
            stg.brand_code, stg.brand_name, stg.brand_owner, stg.brand_status,
            stg.uom_code, stg.uom_description, stg.uom_conversion_factor, stg.uom_base_uom_code,
            stg.m_product_sk, stg.m_category_sk, stg.m_sub_category_sk,
            stg.m_supplier_sk, stg.m_supplier_addr_sk, stg.m_brand_sk, stg.m_uom_sk,
            stg.dim_row_hash,
            p_file_date,   -- effective_start_date = business date of change
            C_HIGH_DATE,   -- open-ended current record
            'Y',
            p_batch_id,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        FROM DIM_PRODUCT_STAGING stg
        WHERE
            -- Case A: product is new (never in DIM before)
            NOT EXISTS (
                SELECT 1 FROM DIM_PRODUCT dp
                WHERE dp.product_code = stg.product_code
            )
            OR
            -- Case B: product existed but hash changed (record just expired)
            EXISTS (
                SELECT 1 FROM DIM_PRODUCT dp
                WHERE dp.product_code    = stg.product_code
                  AND dp.expired_by_batch = p_batch_id  -- just expired in Step 1
            );
        -- Note: product_codes with matching hash are not in either case above,
        -- so they are correctly skipped with no new DIM version created.

        p_rows_inserted := SQL%ROWCOUNT;
        COMMIT;

        LOG_MSG(p_batch_id, v_proc, 'Inserted='||p_rows_inserted);
    END DETECT_AND_APPLY_SCD2;

    -- =========================================================================
    -- PUBLIC: LOAD_DIM_FOR_FILE_DATE
    -- Atomic unit: fan-out resolve -> stage -> detect+apply SCD2.
    -- Called by scheduler, catch-up loop, and replay loop.
    -- =========================================================================
    PROCEDURE LOAD_DIM_FOR_FILE_DATE (
        p_file_date IN DATE,
        p_batch_id  IN NUMBER,
        p_called_by IN VARCHAR2 DEFAULT 'SCHEDULER'
    ) IS
        v_proc         CONSTANT VARCHAR2(50) := 'LOAD_DIM_FOR_FILE_DATE';
        v_affected_cnt NUMBER := 0;
        v_built_cnt    NUMBER := 0;
        v_expired      NUMBER := 0;
        v_inserted     NUMBER := 0;
    BEGIN
        LOG_MSG(p_batch_id, v_proc,
            'START file_date='||TO_CHAR(p_file_date,'YYYY-MM-DD')||
            ' called_by='||p_called_by);
        UPDATE_BATCH_STATUS(p_batch_id, p_file_date, C_STATUS_RUN);

        -- Step 1: Identify affected product_codes from Master change log
        RESOLVE_AFFECTED_PRODUCT_CODES(p_file_date, p_batch_id, v_affected_cnt);

        IF v_affected_cnt = 0 THEN
            -- Nothing changed in any master table that affects DIM; skip entirely
            LOG_MSG(p_batch_id, v_proc,
                'No DIM-relevant master changes. Skipping. file_date='||
                TO_CHAR(p_file_date,'YYYY-MM-DD'));
            UPDATE_BATCH_STATUS(p_batch_id, p_file_date, C_STATUS_DONE, 0);
            RETURN;
        END IF;

        -- Step 2: Build point-in-time denormalized staging for affected codes only
        BUILD_DIM_STAGING(p_file_date, p_batch_id, v_built_cnt);

        -- Step 3: Detect via hash comparison and apply SCD2
        DETECT_AND_APPLY_SCD2(p_file_date, p_batch_id, v_expired, v_inserted);

        UPDATE_BATCH_STATUS(p_batch_id, p_file_date, C_STATUS_DONE,
                            v_expired + v_inserted);
        LOG_MSG(p_batch_id, v_proc,
            'END affected='||v_affected_cnt||
            ' staged='||v_built_cnt||
            ' expired='||v_expired||
            ' inserted='||v_inserted);

    EXCEPTION WHEN OTHERS THEN
        ROLLBACK;
        UPDATE_BATCH_STATUS(p_batch_id, p_file_date, C_STATUS_FAIL, 0, SQLERRM);
        LOG_MSG(p_batch_id, v_proc, 'FAILED: '||SQLERRM, 'ERROR'); RAISE;
    END LOAD_DIM_FOR_FILE_DATE;

    -- =========================================================================
    -- PUBLIC: CATCHUP_DIM_LOAD  (Scenario 1)
    --
    -- Finds file dates where Master=COMPLETED but DIM!=COMPLETED.
    -- Processes each in strict chronological order.
    --
    -- WHY STRICT ORDER MATTERS:
    --   If product_code P changed on day 10 and again on day 12,
    --   the DIM must have three versions: pre-10, post-10, post-12.
    --   Processing day 12 before day 10 would produce wrong effective dates.
    --   Processing in order means each run sees is_current=Y records as its
    --   baseline, which is always the correct prior state.
    -- =========================================================================
    PROCEDURE CATCHUP_DIM_LOAD (
        p_from_date IN DATE DEFAULT NULL,
        p_to_date   IN DATE DEFAULT TRUNC(SYSDATE)
    ) IS
        v_proc      CONSTANT VARCHAR2(50) := 'CATCHUP_DIM_LOAD';
        v_from_date DATE;

        CURSOR c_pending IS
            SELECT bc_m.file_date, bc_m.batch_id
            FROM DW_BATCH_CONTROL bc_m
            WHERE bc_m.layer    = C_MASTER_LAYER
              AND bc_m.status   = C_STATUS_DONE
              AND bc_m.file_date >= v_from_date
              AND bc_m.file_date <= p_to_date
              AND NOT EXISTS (
                  SELECT 1 FROM DW_BATCH_CONTROL bc_d
                  WHERE bc_d.batch_id = bc_m.batch_id
                    AND bc_d.layer    = C_DIM_LAYER
                    AND bc_d.status   = C_STATUS_DONE)
            ORDER BY bc_m.file_date ASC;  -- CRITICAL: strict chronological order
    BEGIN
        IF p_from_date IS NULL THEN
            SELECT NVL(MIN(bc_m.file_date), p_to_date) INTO v_from_date
            FROM DW_BATCH_CONTROL bc_m
            WHERE bc_m.layer  = C_MASTER_LAYER AND bc_m.status = C_STATUS_DONE
              AND NOT EXISTS (SELECT 1 FROM DW_BATCH_CONTROL bc_d
                              WHERE bc_d.batch_id = bc_m.batch_id
                                AND bc_d.layer    = C_DIM_LAYER
                                AND bc_d.status   = C_STATUS_DONE);
        ELSE
            v_from_date := p_from_date;
        END IF;

        LOG_MSG(-1, v_proc, 'START from='||TO_CHAR(v_from_date,'YYYY-MM-DD')
                          ||' to='||TO_CHAR(p_to_date,'YYYY-MM-DD'));

        FOR r IN c_pending LOOP
            BEGIN
                LOAD_DIM_FOR_FILE_DATE(r.file_date, r.batch_id, 'CATCHUP');
            EXCEPTION WHEN OTHERS THEN
                -- Abort: do not skip a date. Each date's DIM state is the
                -- foundation for subsequent dates' change detection.
                LOG_MSG(r.batch_id, v_proc,
                    'ABORTED at '||TO_CHAR(r.file_date,'YYYY-MM-DD')||': '||SQLERRM,'ERROR');
                RAISE;
            END;
        END LOOP;

        LOG_MSG(-1, v_proc, 'COMPLETE');
    END CATCHUP_DIM_LOAD;

    -- =========================================================================
    -- PUBLIC: REPLAY_DIM_FROM_DATE  (Scenario 2)
    --
    -- Rolls back DIM to state just before p_from_date, then replays each
    -- file date forward using the already-corrected Master layer.
    --
    -- PRECONDITION: PKG_MASTER_LOAD.REPLAY_MASTER_FROM_DATE must have
    -- completed successfully before calling this procedure.
    --
    -- Rollback steps:
    --   1. Delete DIM records born on or after p_from_date
    --   2. Re-open DIM records that were expired during the corrupt window
    --   3. Reset DIM batch control statuses
    --   4. Replay forward file-by-file
    -- =========================================================================
    PROCEDURE REPLAY_DIM_FROM_DATE (
        p_from_date IN DATE,
        p_to_date   IN DATE DEFAULT TRUNC(SYSDATE)
    ) IS
        v_proc CONSTANT VARCHAR2(50) := 'REPLAY_DIM_FROM_DATE';

        CURSOR c_replay IS
            SELECT bc.file_date, bc.batch_id
            FROM DW_BATCH_CONTROL bc
            WHERE bc.layer    = C_MASTER_LAYER
              AND bc.status   = C_STATUS_DONE
              AND bc.file_date >= p_from_date
              AND bc.file_date <= p_to_date
            ORDER BY bc.file_date ASC;
    BEGIN
        LOG_MSG(-1, v_proc, 'START from='||TO_CHAR(p_from_date,'YYYY-MM-DD')
                          ||' to='||TO_CHAR(p_to_date,'YYYY-MM-DD'));

        -- ------------------------------------------------------------------
        -- STEP 1: Delete DIM records created during or after the corrupt window.
        --         These were built from wrong Master data.
        -- ------------------------------------------------------------------
        DELETE FROM DIM_PRODUCT
        WHERE effective_start_date >= p_from_date;

        LOG_MSG(-1, v_proc, 'Deleted DIM rows with start >= '||
            TO_CHAR(p_from_date,'YYYY-MM-DD')||' count='||SQL%ROWCOUNT);

        -- ------------------------------------------------------------------
        -- STEP 2: Re-open DIM records that were expired by the corrupt load
        --         or any subsequent load in the replay window.
        --         These records were valid before the corruption and should
        --         be current again so replay has a clean baseline.
        -- ------------------------------------------------------------------
        UPDATE DIM_PRODUCT
        SET
            effective_end_date = C_HIGH_DATE,
            is_current         = 'Y',
            dw_update_ts       = SYSTIMESTAMP,
            expired_by_batch   = NULL
        WHERE effective_end_date  >= p_from_date      -- expired in corrupt window
          AND effective_end_date  <  C_HIGH_DATE      -- was not already open
          AND effective_start_date < p_from_date;     -- genuinely predates corruption

        LOG_MSG(-1, v_proc, 'Re-opened DIM rows='||SQL%ROWCOUNT);

        -- ------------------------------------------------------------------
        -- STEP 3: Reset DIM batch control for the replay window so
        --         LOAD_DIM_FOR_FILE_DATE sees each date as unprocessed.
        -- ------------------------------------------------------------------
        UPDATE DW_BATCH_CONTROL
        SET status = C_STATUS_PEND, updated_ts = SYSTIMESTAMP
        WHERE layer    = C_DIM_LAYER
          AND file_date >= p_from_date
          AND file_date <= p_to_date;

        COMMIT;
        LOG_MSG(-1, v_proc, 'DIM rollback complete. Starting replay.');

        -- ------------------------------------------------------------------
        -- STEP 4: Replay each file date using corrected Master data.
        --         Fan-out resolver reads DW_MASTER_CHANGE_LOG which was
        --         repopulated by PKG_MASTER_LOAD.REPLAY_MASTER_FROM_DATE.
        -- ------------------------------------------------------------------
        FOR r IN c_replay LOOP
            BEGIN
                LOAD_DIM_FOR_FILE_DATE(r.file_date, r.batch_id, 'REPLAY');
            EXCEPTION WHEN OTHERS THEN
                LOG_MSG(r.batch_id, v_proc,
                    'ABORTED at '||TO_CHAR(r.file_date,'YYYY-MM-DD')||': '||SQLERRM,'ERROR');
                RAISE;
            END;
        END LOOP;

        LOG_MSG(-1, v_proc, 'Replay COMPLETE');

    EXCEPTION WHEN OTHERS THEN
        ROLLBACK;
        LOG_MSG(-1, v_proc, 'FAILED in rollback phase: '||SQLERRM,'ERROR'); RAISE;
    END REPLAY_DIM_FROM_DATE;

END PKG_DIM_LOAD;
/
