commonqueries = {

    'tablesize':
    """
        SELECT
            database,
            table,
            formatReadableSize(sum(data_compressed_bytes) AS size) AS compressed,
            formatReadableSize(sum(data_uncompressed_bytes) AS usize) AS uncompressed,
            round(usize / size, 2) AS compr_rate,
            sum(rows) AS rows,
            count() AS part_count,
            round((sum(data_uncompressed_bytes) AS usize)/rows) AS uncompressed_row_size
        FROM system.parts
        WHERE (active = 1) AND (database LIKE '{database}') AND (table LIKE '{table}')
        GROUP BY
            database,
            table
        ORDER BY size DESC;
    """,

    'columnsize':
    """
    SELECT
        database,
        table,
        column,
        formatReadableSize(sum(column_data_compressed_bytes) AS size) AS compressed,
        formatReadableSize(sum(column_data_uncompressed_bytes) AS usize) AS uncompressed,
        round(usize / size, 2) AS compr_ratio,
        sum(rows) rows_cnt,
        round(usize / rows_cnt, 2) avg_row_size
    FROM system.parts_columns
    WHERE (active = 1) AND (database LIKE '{database}') AND (table LIKE '{table}') AND (column LIKE '{column}')
    GROUP BY
        database,
        table,
        column
    ORDER BY size DESC;
    """
}
