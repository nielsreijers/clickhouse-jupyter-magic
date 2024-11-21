# This code can be put in any Python module, it does not require IPython
# itself to be running already.  It only creates the magics subclass but
# doesn't instantiate it yet.
from __future__ import print_function
from pathlib import Path
import os
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
import urllib
from IPython.core.display import display, HTML
from IPython.display import FileLink
import graphviz
import uuid
import time
from datetime import datetime, timedelta, timezone
from IPython.core.magic_arguments import (argument, magic_arguments, parse_argstring)
import subprocess
from mymagic_commonqueries import commonqueries

PROFILER_SETTINGS = 'query_profiler_cpu_time_period_ns=10000000, query_profiler_real_time_period_ns=10000000'
POLL_QUERY_LOG_TIMEOUT_SECONDS = 10
POLL_TRACE_LOG_TIMEOUT_SECONDS = 10

def add_setting_to_query(query, setting):
    # TODO: better parsing. this will fail for queries like "SELECT 'there are no settings here'"
    if ' settings ' in query.replace("\r"," ").replace("\n"," ").lower():
        return query + ', ' + setting
    else:
        return query + ' SETTINGS ' + setting

def guard_query_id(query_id):
    try:
        uuid.UUID(query_id)
    except ValueError:
        raise ValueError(f"'{query_id} is not a valid query id.")

# The class MUST call this class decorator at creation time
@magics_class
class JupysqlTextOutputMagics(Magics):

    
    
    ################################################################################
    #                  Helper methods                                              #
    ################################################################################
    def run_query(self, query):
        """
        Runs a query.
        Currently this uses jupysql but maybe we should replace this with a direct sqlalchemy query to remove the dependency
        """
        if 'jupysql' not in self.shell.magics_manager.magics['cell']:
            raise ModuleNotFoundError("Can't find %jupysql magic. Is the extension not loaded?")

        return self.shell.run_cell_magic('sql', '', query)

    def run_query_until_result(self, query, timeout_s, wait_message=None):
        """
        Runs a query repeatedly until it finds a result.
        This can be used for querying log tables if we don't have SYSTEM FLUSH LOGS permissions,
        or to repeatedly hit the loadbalancer until it becomes sticky so we (almost) always end up on the same server.
        """
        original_feedback_level = self.shell.run_line_magic('config', 'SqlMagic.feedback')
        self.shell.run_line_magic('config', 'SqlMagic.feedback=0')
        try:
            r = self.run_query(query)
            if len(r) == 0:
                start_waiting = datetime.now()
                if wait_message:
                    print(wait_message + f' (timeout = {timeout_s} seconds)')
                while len(r) == 0 and (datetime.now() - start_waiting).seconds < timeout_s:
                    time.sleep(1)
                    r = self.run_query(query)
            if len(r) > 0:
                return r
            else:
                raise Exception("Timeout waiting for query to produce results")
        finally:
            self.shell.run_line_magic('config', f'SqlMagic.feedback={original_feedback_level}')

    def run_and_get_query_id(self, query, silent=False):
        """
        Runs a query and returns the query_id.
        """
        tag = f'jupyter tag {uuid.uuid4()}'
        tagged_query = add_setting_to_query(query, f"log_comment='{tag}'")

        if not silent:
            print(f"Query tagged by setting query_log.log_comment to '{tag}'")
        start_time = datetime.now(timezone.utc)
        self.run_query(tagged_query)
        stop_time = datetime.now(timezone.utc)
        delta = stop_time - start_time
        query_duration_ms = 1000*delta.total_seconds() + int(delta.microseconds / 1000)
        
        # Unfortunately we can't do this in readonly mode
        # self.run_query("SYSTEM FLUSH LOGS")

        yesterday = (start_time + timedelta(days=-1)).strftime("%Y-%m-%d") # allow plenty of margin. maybe some server won't be set to UTC?
        find_query_id = f"SELECT query_id FROM system.query_log WHERE log_comment='{tag}' AND event_time > '{yesterday}' LIMIT 1 SETTINGS log_comment='jupyter query_id probe'"
        wait_message = f'Waiting for query with tag {tag} to appear in system.query_log...' if not silent else None
        r = self.run_query_until_result(query=find_query_id, timeout_s=POLL_QUERY_LOG_TIMEOUT_SECONDS, wait_message=wait_message)
        query_id = r[0][0]
        if not silent:
            print (f'Query {query_id} ran for {query_duration_ms} ms.') 
        return query_id


    
    ################################################################################
    #                  Methods that run a query                                    #
    ################################################################################
    @line_cell_magic
    @magic_arguments()
    @argument(
        "-s", "--silent", action="store_true", help="Don't print query id and duration",
    )
    @argument(
        "-p", "--profile", action="store_true", help=f"Add '{PROFILER_SETTINGS}' to query SETTINGS",
    )
    @argument('querytext', nargs='*', help='The query to analyse')
    def qsql(self, line="", cell=""):
        """
        Runs a query and returns the query_id.
        """
        args = parse_argstring(self.qsql, line)

        if len(args.querytext) > 0:
            query = " ".join(args.querytext)
        else:
            query = cell

        if args.profile:
           query = add_setting_to_query(query, PROFILER_SETTINGS)

        return self.run_and_get_query_id(query, silent=args.silent)

    @line_cell_magic
    def tsql(self, line="", cell=""):
        """
        Runs a query and outputs the result as fixed-width text.
        Useful for getting readable results from EXPLAIN PLAN or EXPLAIN PIPELINE queries.
        """
        r = self.run_query(line if line != "" else cell)

        if len(r.field_names) == 1:
            colname = r.field_names[0]
            print(colname)
            print('-' * len(colname))
            for l in r:
                print(l[0])
        else:
            print(r)
        
        if cell=="":
            return r
        else:
            return None


    @line_cell_magic
    @magic_arguments()
    @argument(
        'query', type=str, help=f'The name of the query to run. ({", ".join(commonqueries.keys())})',
    )
    @argument(
        "-d", "--database", type=str, default='%', help="Optional database filter where applicable."
    )
    @argument(
        "-t", "--table", type=str, default='%', help="Optional table filter where applicable."
    )
    @argument(
        "-c", "--column", type=str, default='%', help="Optional column filter where applicable."
    )
    def csql(self, line="", cell=""):
        """
        Runs one of a number of predefined common queries.
        """
        args = parse_argstring(self.csql, line)

        if args.query not in commonqueries:
            raise Exception(f"Invalid query name '{args.query}'. Valid query names are: {', '.join(commonqueries.keys())}.")
        
        query = commonqueries[args.query].format(
            database=args.database,
            table=args.table,
            column=args.column)

        return self.run_query(query)

    
    
    ################################################################################
    #                  Methods to analyse queries                                  #
    ################################################################################
    @line_cell_magic
    @magic_arguments()
    @argument(
        "-l", "--create-link", action="store_true", help="Don't plot the pipeline, but generate a link to open the graph in another tab."
    )
    @argument(
        "-c", "--compact", action="store_true", help="Add `compact=1` to pass to `EXPLAIN PIPELINE graph=1, compact=1`",
    )
    @argument(
        '-q', '--query_id', type=str, default='', help='The query_id of a past query to analyse',
    )
    @argument('querytext', nargs='*', help='The query to analyse')
    # arser.add_argument('rest', nargs=argparse.REMAINDER, help='The rest of the command line arguments')
    def ch_pipeline(self, line="", cell=""):
        """
        Show the query pipeline as a graph
        """
        args = parse_argstring(self.ch_pipeline, line)
        
        if args.query_id == '':
            if len(args.querytext) > 0:
                query = " ".join(args.querytext)
            else:
                query = cell
        else:
            query_id = args.query_id
            guard_query_id(query_id)
            query_log_query = f"SELECT query FROM system.query_log WHERE query_id='{query_id}' LIMIT 1"
            r = self.run_query(query_log_query)
            if len(r) == 1:
                query = r[0][0]
            else:
                raise Exception(f"Query with id '{args.query_id}' not found.")

        if args.compact:
            is_compact = 1
        else:
            is_compact = 0
        pipeline_query = add_setting_to_query(f"EXPLAIN PIPELINE graph=1, compact={is_compact} { query }",
        "allow_experimental_analyzer = 1")
        
        r = self.run_query(pipeline_query)
        digraph = "\n\r".join([l[0] for l in r])

        if args.create_link:
            url = f"https://dreampuf.github.io/GraphvizOnline/#{urllib.parse.quote(digraph)}"
            display(HTML(f'<h3><a href="{url}">pipeline graph</a></h3>'))
        else:
            display(graphviz.Source(digraph))

    
    @line_cell_magic
    @magic_arguments()
    @argument(
        '-q', '--query_id', type=str, default='', help='The query_id of a past query to analyse',
    )
    @argument('querytext', nargs='*', help='The query to analyse')
    def ch_flame(self, line="", cell=""):
        """
        Show the trace_log profiling data as a flamegraph
        """
        args = parse_argstring(self.ch_flame, line)

        if args.query_id == '':
            if len(args.querytext) > 0:
                query = " ".join(args.querytext)
            else:
                query = cell
    
            query = cell if line == "" else line
            query = add_setting_to_query(query, PROFILER_SETTINGS)
            query_id = self.run_and_get_query_id(query)
        else:
            query_id = args.query_id
            guard_query_id(query_id)

        trace_log_query = f"SELECT arrayStringConcat(arrayReverse(arrayMap(x -> demangle(addressToSymbol(x)), trace)), ';') AS stack, count() AS samples FROM system.trace_log WHERE query_id = '{query_id}' GROUP BY trace SETTINGS allow_introspection_functions=1"

        r = self.run_query_until_result(
            query=trace_log_query,
            timeout_s=POLL_TRACE_LOG_TIMEOUT_SECONDS,
            wait_message=f'Waiting for trace for query {query_id} to appear in system.trace_log...')
        if len(r) == 0:
            raise Exception(f'No trace found for query {query_id}')
        result = '\n'.join([f'{row[0]} {row[1]}' for row in r])
        print (f'{len(r)} unique samples found.')

        # filename = f'{query_id}.flamegraph'
        # with open(filename, 'w') as text_file:
        #     text_file.write(result)
        # print(f'Download {filename} and import it in https://www.speedscope.app/')

        Path('profiling_data').mkdir(parents=True, exist_ok=True)
        data_filename = os.path.join('profiling_data', f'{query_id}.flamegraph.data')
        svg_filename = os.path.join('profiling_data', f'{query_id}.flamegraph.svg')
        with open(data_filename, 'w') as data_file:
            data_file.write(result)
        with open(svg_filename, "w") as svg_file:
            subprocess.run(["perl", 'flamegraph.pl', data_filename], check=True, stdout=svg_file)
        display(HTML(svg_filename))


# In order to actually use these magics, you must register them with a
# running IPython.
def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    # You can register the class itself without instantiating it.  IPython will
    # call the default constructor on it.
    ipython.register_magics(JupysqlTextOutputMagics)
