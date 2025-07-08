-- Oracle Database Dependency Flow Analyzer
-- This script identifies and traces dependency flows in Oracle databases

-- 1. Create a temporary table to store dependency relationships
CREATE GLOBAL TEMPORARY TABLE temp_dependencies (
    source_owner VARCHAR2(128),
    source_name VARCHAR2(128),
    source_type VARCHAR2(50),
    target_owner VARCHAR2(128),
    target_name VARCHAR2(128),
    target_type VARCHAR2(50),
    dependency_type VARCHAR2(50),
    level_num NUMBER
) ON COMMIT PRESERVE ROWS;

-- 2. Main procedure to analyze dependency flows with flexible dependency view
CREATE OR REPLACE PROCEDURE analyze_dependency_flow(
    p_object_name VARCHAR2,
    p_object_owner VARCHAR2 DEFAULT USER,
    p_object_type VARCHAR2 DEFAULT NULL,
    p_direction VARCHAR2 DEFAULT 'FORWARD', -- FORWARD or BACKWARD
    p_dependency_view VARCHAR2 DEFAULT 'ALL_DEPENDENCIES' -- USER_DEPENDENCIES, ALL_DEPENDENCIES, or DBA_DEPENDENCIES
) AS
    v_sql VARCHAR2(4000);
    v_count NUMBER;
    v_dep_view VARCHAR2(50) := UPPER(p_dependency_view);
    v_owner_filter VARCHAR2(200);
BEGIN
    -- Clear temporary table
    DELETE FROM temp_dependencies;
    
    -- Set owner filter based on dependency view
    IF v_dep_view = 'USER_DEPENDENCIES' THEN
        v_owner_filter := 'AND d.owner = ''' || USER || '''';
    ELSIF v_dep_view = 'ALL_DEPENDENCIES' THEN
        v_owner_filter := ''; -- No additional filter for ALL_DEPENDENCIES
    ELSE
        v_dep_view := 'DBA_DEPENDENCIES';
        v_owner_filter := ''; -- No additional filter for DBA_DEPENDENCIES
    END IF;
    
    -- Insert direct dependencies based on direction
    IF p_direction = 'FORWARD' THEN
        -- Forward dependencies (what this object depends on)
        v_sql := 'INSERT INTO temp_dependencies
        SELECT DISTINCT
            d.owner as source_owner,
            d.name as source_name,
            d.type as source_type,
            d.referenced_owner as target_owner,
            d.referenced_name as target_name,
            d.referenced_type as target_type,
            ''DEPENDS_ON'' as dependency_type,
            1 as level_num
        FROM ' || v_dep_view || ' d
        WHERE d.owner = ''' || p_object_owner || '''
        AND d.name = ''' || p_object_name || '''
        AND (''' || p_object_type || ''' IS NULL OR d.type = ''' || p_object_type || ''')
        AND d.referenced_owner NOT IN (''SYS'', ''SYSTEM'', ''PUBLIC'')
        AND d.referenced_type IN (''TABLE'', ''VIEW'', ''PACKAGE'', ''PROCEDURE'', ''FUNCTION'', ''TRIGGER'')';
        
        EXECUTE IMMEDIATE v_sql;
    ELSE
        -- Backward dependencies (what depends on this object)
        v_sql := 'INSERT INTO temp_dependencies
        SELECT DISTINCT
            d.referenced_owner as source_owner,
            d.referenced_name as source_name,
            d.referenced_type as source_type,
            d.owner as target_owner,
            d.name as target_name,
            d.type as target_type,
            ''DEPENDED_BY'' as dependency_type,
            1 as level_num
        FROM ' || v_dep_view || ' d
        WHERE d.referenced_owner = ''' || p_object_owner || '''
        AND d.referenced_name = ''' || p_object_name || '''
        AND (''' || p_object_type || ''' IS NULL OR d.referenced_type = ''' || p_object_type || ''')
        AND d.owner NOT IN (''SYS'', ''SYSTEM'', ''PUBLIC'')
        AND d.type IN (''TABLE'', ''VIEW'', ''PACKAGE'', ''PROCEDURE'', ''FUNCTION'', ''TRIGGER'')';
        
        EXECUTE IMMEDIATE v_sql;
    END IF;
    
    -- Recursively find deeper dependencies (up to 10 levels)
    FOR i IN 2..10 LOOP
        v_count := 0;
        
        IF p_direction = 'FORWARD' THEN
            v_sql := 'INSERT INTO temp_dependencies
            SELECT DISTINCT
                d.owner,
                d.name,
                d.type,
                d.referenced_owner,
                d.referenced_name,
                d.referenced_type,
                ''DEPENDS_ON'',
                ' || i || '
            FROM ' || v_dep_view || ' d
            WHERE (d.owner, d.name, d.type) IN (
                SELECT target_owner, target_name, target_type 
                FROM temp_dependencies 
                WHERE level_num = ' || (i-1) || '
            )
            AND d.referenced_owner NOT IN (''SYS'', ''SYSTEM'', ''PUBLIC'')
            AND d.referenced_type IN (''TABLE'', ''VIEW'', ''PACKAGE'', ''PROCEDURE'', ''FUNCTION'', ''TRIGGER'')
            AND (d.owner, d.name, d.type, d.referenced_owner, d.referenced_name, d.referenced_type) 
                NOT IN (SELECT source_owner, source_name, source_type, target_owner, target_name, target_type FROM temp_dependencies)';
            
            EXECUTE IMMEDIATE v_sql;
        ELSE
            v_sql := 'INSERT INTO temp_dependencies
            SELECT DISTINCT
                d.referenced_owner,
                d.referenced_name,
                d.referenced_type,
                d.owner,
                d.name,
                d.type,
                ''DEPENDED_BY'',
                ' || i || '
            FROM ' || v_dep_view || ' d
            WHERE (d.referenced_owner, d.referenced_name, d.referenced_type) IN (
                SELECT target_owner, target_name, target_type 
                FROM temp_dependencies 
                WHERE level_num = ' || (i-1) || '
            )
            AND d.owner NOT IN (''SYS'', ''SYSTEM'', ''PUBLIC'')
            AND d.type IN (''TABLE'', ''VIEW'', ''PACKAGE'', ''PROCEDURE'', ''FUNCTION'', ''TRIGGER'')
            AND (d.referenced_owner, d.referenced_name, d.referenced_type, d.owner, d.name, d.type) 
                NOT IN (SELECT source_owner, source_name, source_type, target_owner, target_name, target_type FROM temp_dependencies)';
            
            EXECUTE IMMEDIATE v_sql;
        END IF;
        
        v_count := SQL%ROWCOUNT;
        EXIT WHEN v_count = 0;
    END LOOP;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Dependency analysis complete using ' || v_dep_view || '. Found ' || 
                        (SELECT COUNT(*) FROM temp_dependencies) || ' dependencies.');
END;
/

-- 3. Function to generate dependency flow paths
CREATE OR REPLACE FUNCTION get_dependency_flow_path(
    p_object_name VARCHAR2,
    p_object_owner VARCHAR2 DEFAULT USER,
    p_object_type VARCHAR2 DEFAULT NULL,
    p_direction VARCHAR2 DEFAULT 'FORWARD',
    p_dependency_view VARCHAR2 DEFAULT 'ALL_DEPENDENCIES'
) RETURN CLOB AS
    v_result CLOB;
    v_path VARCHAR2(4000);
    
    CURSOR c_paths IS
        WITH dependency_paths AS (
            SELECT 
                source_owner || '.' || source_name || ' (' || source_type || ')' as source_obj,
                target_owner || '.' || target_name || ' (' || target_type || ')' as target_obj,
                level_num,
                dependency_type
            FROM temp_dependencies
            ORDER BY level_num
        )
        SELECT DISTINCT
            CONNECT_BY_ROOT source_obj as root_object,
            SYS_CONNECT_BY_PATH(target_obj, ' -> ') as path,
            LEVEL as path_level
        FROM dependency_paths
        WHERE dependency_type = CASE WHEN p_direction = 'FORWARD' THEN 'DEPENDS_ON' ELSE 'DEPENDED_BY' END
        CONNECT BY NOCYCLE PRIOR target_obj = source_obj
        START WITH source_obj = p_object_owner || '.' || p_object_name || 
                   CASE WHEN p_object_type IS NULL THEN '' ELSE ' (' || p_object_type || ')' END
        ORDER BY path_level, path;
BEGIN
    v_result := '';
    
    FOR rec IN c_paths LOOP
        v_path := rec.root_object || rec.path;
        v_result := v_result || v_path || CHR(10);
    END LOOP;
    
    RETURN v_result;
END;
/

-- 4. Simplified query to show dependency flow
CREATE OR REPLACE VIEW dependency_flow_view AS
SELECT 
    source_owner,
    source_name,
    source_type,
    target_owner,
    target_name,
    target_type,
    level_num,
    dependency_type,
    source_owner || '.' || source_name || ' (' || source_type || ')' as source_object,
    target_owner || '.' || target_name || ' (' || target_type || ')' as target_object
FROM temp_dependencies
ORDER BY level_num, source_owner, source_name, target_owner, target_name;

-- 5. Enhanced procedure with formatted output
CREATE OR REPLACE PROCEDURE show_dependency_flow(
    p_object_name VARCHAR2,
    p_object_owner VARCHAR2 DEFAULT USER,
    p_object_type VARCHAR2 DEFAULT NULL,
    p_direction VARCHAR2 DEFAULT 'FORWARD',
    p_dependency_view VARCHAR2 DEFAULT 'ALL_DEPENDENCIES'
) AS
    v_arrow VARCHAR2(10);
    v_level NUMBER;
    v_indent VARCHAR2(100);
    v_prev_level NUMBER := 0;
    
    CURSOR c_flow IS
        SELECT DISTINCT
            source_owner || '.' || source_name || ' (' || source_type || ')' as source_obj,
            target_owner || '.' || target_name || ' (' || target_type || ')' as target_obj,
            level_num
        FROM temp_dependencies
        WHERE dependency_type = CASE WHEN p_direction = 'FORWARD' THEN 'DEPENDS_ON' ELSE 'DEPENDED_BY' END
        ORDER BY level_num;
BEGIN
    -- First run the analysis
    analyze_dependency_flow(p_object_name, p_object_owner, p_object_type, p_direction, p_dependency_view);
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== DEPENDENCY FLOW ANALYSIS ===');
    DBMS_OUTPUT.PUT_LINE('Object: ' || p_object_owner || '.' || p_object_name || 
                        CASE WHEN p_object_type IS NOT NULL THEN ' (' || p_object_type || ')' END);
    DBMS_OUTPUT.PUT_LINE('Direction: ' || p_direction);
    DBMS_OUTPUT.PUT_LINE('Using View: ' || p_dependency_view);
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Show the starting object
    DBMS_OUTPUT.PUT_LINE('Starting Object: ' || p_object_owner || '.' || p_object_name || 
                        CASE WHEN p_object_type IS NOT NULL THEN ' (' || p_object_type || ')' END);
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Show dependency flow
    FOR rec IN c_flow LOOP
        v_indent := RPAD(' ', (rec.level_num - 1) * 2, ' ');
        v_arrow := CASE WHEN p_direction = 'FORWARD' THEN ' -> ' ELSE ' <- ' END;
        
        IF rec.level_num > v_prev_level THEN
            DBMS_OUTPUT.PUT_LINE(v_indent || 'Level ' || rec.level_num || ':');
        END IF;
        
        DBMS_OUTPUT.PUT_LINE(v_indent || rec.source_obj || v_arrow || rec.target_obj);
        v_prev_level := rec.level_num;
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== END OF ANALYSIS ===');
END;
/

-- 6. Function to get complete dependency chains
CREATE OR REPLACE FUNCTION get_complete_dependency_chains(
    p_object_name VARCHAR2,
    p_object_owner VARCHAR2 DEFAULT USER,
    p_max_depth NUMBER DEFAULT 5
) RETURN CLOB AS
    v_result CLOB;
    v_chain VARCHAR2(4000);
    
    -- Recursive CTE to build complete chains
    CURSOR c_chains IS
        WITH RECURSIVE dependency_chain AS (
            -- Base case: direct dependencies
            SELECT 
                d.owner as current_owner,
                d.name as current_name,
                d.type as current_type,
                d.referenced_owner as dep_owner,
                d.referenced_name as dep_name,
                d.referenced_type as dep_type,
                1 as depth,
                d.owner || '.' || d.name || ' (' || d.type || ')' as chain_path
            FROM dba_dependencies d
            WHERE d.owner = p_object_owner
            AND d.name = p_object_name
            AND d.referenced_owner NOT IN ('SYS', 'SYSTEM', 'PUBLIC')
            
            UNION ALL
            
            -- Recursive case: follow dependencies
            SELECT 
                d.owner,
                d.name,
                d.type,
                d.referenced_owner,
                d.referenced_name,
                d.referenced_type,
                dc.depth + 1,
                dc.chain_path || ' -> ' || d.owner || '.' || d.name || ' (' || d.type || ')'
            FROM dba_dependencies d
            JOIN dependency_chain dc ON (d.owner = dc.dep_owner AND d.name = dc.dep_name)
            WHERE dc.depth < p_max_depth
            AND d.referenced_owner NOT IN ('SYS', 'SYSTEM', 'PUBLIC')
        )
        SELECT DISTINCT chain_path, depth
        FROM dependency_chain
        ORDER BY depth, chain_path;
BEGIN
    v_result := 'Complete Dependency Chains for ' || p_object_owner || '.' || p_object_name || ':' || CHR(10) || CHR(10);
    
    FOR rec IN c_chains LOOP
        v_result := v_result || 'Depth ' || rec.depth || ': ' || rec.chain_path || CHR(10);
    END LOOP;
    
    RETURN v_result;
END;
/

-- 7. Usage Examples and Test Queries

-- Example 1: Using USER_DEPENDENCIES (only current user's objects)
/*
-- To analyze dependencies of objects in your schema:
EXEC show_dependency_flow('VIEW1', USER, 'VIEW', 'FORWARD', 'USER_DEPENDENCIES');

-- To see what depends on a specific object in your schema:
EXEC show_dependency_flow('TABLE1', USER, 'TABLE', 'BACKWARD', 'USER_DEPENDENCIES');
*/

-- Example 2: Using ALL_DEPENDENCIES (objects you have access to)
/*
-- To analyze dependencies across multiple schemas you have access to:
EXEC show_dependency_flow('VIEW1', 'MYSCHEMA', 'VIEW', 'FORWARD', 'ALL_DEPENDENCIES');

-- To see what depends on a specific object across accessible schemas:
EXEC show_dependency_flow('TABLE1', 'MYSCHEMA', 'TABLE', 'BACKWARD', 'ALL_DEPENDENCIES');
*/

-- Example 3: Using DBA_DEPENDENCIES (if you have DBA privileges)
/*
-- To analyze dependencies across all schemas in the database:
EXEC show_dependency_flow('VIEW1', 'MYSCHEMA', 'VIEW', 'FORWARD', 'DBA_DEPENDENCIES');
*/

-- Example 4: Default behavior (uses ALL_DEPENDENCIES)
/*
-- Default usage without specifying dependency view:
EXEC show_dependency_flow('VIEW1', 'MYSCHEMA', 'VIEW', 'FORWARD');
*/

-- Example 5: Query to find all tables that a package depends on
/*
-- First run the analysis
EXEC analyze_dependency_flow('PACKAGE1', 'MYSCHEMA', 'PACKAGE', 'FORWARD', 'ALL_DEPENDENCIES');

-- Then query the results
SELECT DISTINCT
    target_owner || '.' || target_name as dependent_table
FROM temp_dependencies
WHERE source_name = 'PACKAGE1'
AND source_type = 'PACKAGE'
AND target_type = 'TABLE'
ORDER BY dependent_table;
*/

-- Example 6: Query to show dependency levels
/*
SELECT 
    level_num,
    COUNT(*) as dependency_count,
    LISTAGG(target_owner || '.' || target_name, ', ') WITHIN GROUP (ORDER BY target_name) as objects
FROM temp_dependencies
GROUP BY level_num
ORDER BY level_num;
*/

-- Comparison of dependency views:
/*
USER_DEPENDENCIES:
- Shows only objects owned by current user
- No special privileges required
- Limited scope but fast and secure

ALL_DEPENDENCIES:
- Shows objects you have access to (own + granted)
- Most commonly used option
- Good balance of scope and accessibility

DBA_DEPENDENCIES:
- Shows all objects in the database
- Requires DBA privileges or SELECT_CATALOG_ROLE
- Complete database-wide view
*/

-- 8. Cleanup procedure
CREATE OR REPLACE PROCEDURE cleanup_dependency_analysis AS
BEGIN
    DELETE FROM temp_dependencies;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Dependency analysis data cleared.');
END;
/

-- Grant necessary permissions (run as privileged user if using DBA_DEPENDENCIES)
/*
-- For DBA_DEPENDENCIES access:
GRANT SELECT ON dba_dependencies TO your_analysis_user;

-- For basic functionality (USER_DEPENDENCIES and ALL_DEPENDENCIES work without additional grants):
GRANT CREATE PROCEDURE TO your_analysis_user;
GRANT CREATE VIEW TO your_analysis_user;
*/

-- Usage Instructions:
/*
1. Run the entire script to create all objects
2. Choose the appropriate dependency view based on your needs and privileges:
   - USER_DEPENDENCIES: For your own objects only
   - ALL_DEPENDENCIES: For objects you have access to (recommended default)
   - DBA_DEPENDENCIES: For system-wide analysis (requires DBA privileges)

3. Use show_dependency_flow procedure for interactive analysis:
   EXEC show_dependency_flow('YOUR_OBJECT_NAME', 'YOUR_SCHEMA', 'OBJECT_TYPE', 'FORWARD', 'ALL_DEPENDENCIES');
   
4. Query temp_dependencies table directly for custom analysis
5. Use dependency_flow_view for easier querying
6. Clean up with cleanup_dependency_analysis when done

Example flows you'll see:
- PACKAGE2 -> TABLE2 -> PACKAGE1 -> TABLE1 -> VIEW1
- TABLE1 -> TRIGGER1 -> PACKAGE1 -> TABLE2

Recommended approach:
- Start with ALL_DEPENDENCIES for most use cases
- Use USER_DEPENDENCIES if you only need your own schema
- Use DBA_DEPENDENCIES only if you have DBA privileges and need system-wide analysis
*/
