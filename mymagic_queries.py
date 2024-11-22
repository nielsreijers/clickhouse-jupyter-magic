common_queries = {

    'tablesize': {
        'default_limit': None,
        'query':
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
        ORDER BY size DESC
        """,
    },

    'columnsize': {
        'default_limit': None,
        'query':
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
        ORDER BY size DESC
        """,
    },

    'myrecentqueries': {
        'default_limit': 10,
        'query':
        """
        SELECT
            event_time,
            type,
            query_duration_ms,
            query
        FROM system.query_log
        WHERE user='{user}'
        ORDER BY event_time_microseconds DESC
        """,
    },

    'recentslowqueries': {
        'default_limit': 10,
        'query':
        """
        SELECT
            event_time,
            user,
            type,
            query_duration_ms,
            query
        FROM system.query_log
        WHERE event_time >= toDateTime('{since}')
        ORDER BY query_duration_ms DESC
        """,
    }
}

trace_log_queries = {

    'a':
    """
        SELECT
            arrayStringConcat(arrayReverse(arrayMap(x -> demangle(addressToSymbol(x)), trace)), ';') AS stack,
            count() AS samples
        FROM system.trace_log
        WHERE query_id = '{query_id}'
        GROUP BY trace
        SETTINGS allow_introspection_functions=1
    """,

    'b':
    """
        SELECT
            arrayStringConcat(arrayReverse(arrayMap(x -> demangle(addressToSymbol(x)), trace)), ';') AS stack,
            count() AS samples
        FROM (
            SELECT
                event_time_microseconds,
                trace,
                -- group id is the number of groups that have ended before the current record (end_of_group=1) 
                SUM(end_of_group) OVER (ORDER BY event_time_microseconds ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS grp
            FROM (
              SELECT event_time_microseconds, trace, case when trace!=leadInFrame(trace) OVER () then 1 else 0 end as end_of_group
              FROM (
                    select event_time_microseconds, trace from system.trace_log where query_id='{query_id}'
                )
            )
        )
        GROUP BY grp, trace
        ORDER BY grp
        SETTINGS allow_introspection_functions=1
    """
}
