SELECT
    relname AS "table_name",
    pg_size_pretty(pg_total_relation_size(relid)) AS "total_size"
FROM
    pg_catalog.pg_statio_user_tables
ORDER BY
    pg_total_relation_size(relid) DESC;


SELECT
    hypertable_name,pg_size_pretty(SUM(pg_total_relation_size(relid))) AS "total_size"
FROM
    timescaledb_information.chunks a inner join 
     pg_catalog.pg_statio_user_tables b on a.chunk_name = b.relname
group by hypertable_name ;
