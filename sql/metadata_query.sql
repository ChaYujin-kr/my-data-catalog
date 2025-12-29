-- metadata_query.sql
SELECT 
    t.TABLE_SCHEMA AS db_name,
    t.TABLE_NAME AS table_name,
    IFNULL(t.TABLE_COMMENT, '') AS table_comment,
    t.CREATE_TIME AS created_at,
    c.COLUMN_NAME AS col_name,
    c.COLUMN_TYPE AS col_type,
    IFNULL(c.COLUMN_COMMENT, '') AS col_comment,
    c.IS_NULLABLE AS is_nullable,
    c.ORDINAL_POSITION AS col_order
FROM information_schema.TABLES t
JOIN information_schema.COLUMNS c 
    ON t.TABLE_SCHEMA = c.TABLE_SCHEMA 
    AND t.TABLE_NAME = c.TABLE_NAME
WHERE t.TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION;