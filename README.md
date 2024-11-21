# ClickHouse extension for Jupyter

This extension adds several % magics to Jupyter to make it easier to analyse queries.
Work in progress, this is just a first draft.

## Usage
I don't have a way to install it properly yet.

It depends on jupysql, so do a `pip install jupysql` first, then just put these files next to your notebook and add something like this as the first cell.

```
import sqlalchemy
import getpass
import os

%load_ext sql
%config SqlMagic.autocommit=False
%config SqlMagic.style="SINGLE_BORDER"
%config SqlMagic.autolimit = 1000
%config SqlMagic.displaylimit = 1000
%config SqlMagic.feedback = 2

%load_ext mymagic

SERVER='<fill in server name>'
DATABASE='<fill in database name>'
USER='<fill in user name>'
engine = sqlalchemy.create_engine(f"clickhouse+native://{USER}:{getpass.getpass()}@{SERVER}/{DATABASE}?secure=true&ssl_verify_cert=false")
%sql engine
```

## Magics
- `%qsql`: Runs a query and returns the query_id.
- `%tsql`: Runs a query and outputs the result as fixed-width text. Useful for getting readable results from EXPLAIN PLAN or EXPLAIN PIPELINE queries.
- `%csql`: Runs one of a number of predefined common queries. (currently just 'tablesize' and 'columnsize')
- `%ch_pipeline`: Show the query pipeline as a graph
- `%ch_flame`: Show the trace_log profiling data as a flamegraph

Adding a `?` after the magic displays help about its parameters. (ex: `%qsql?`)

## Example
```
query = """
    SELECT * FROM numbers() LIMIT 1000000
"""
query_id = %qsql -p { query }
%ch_flame -q { query_id }
%ch_pipeline -c -q { query_id }
```

Note that when connecting through haproxy each query may end up on a different server, which means it can't find the query. It will retry 10 times, but of course it would be better if we can make sure we end up on the same server.


