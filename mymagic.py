# This code can be put in any Python module, it does not require IPython
# itself to be running already.  It only creates the magics subclass but
# doesn't instantiate it yet.
from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
import urllib
from IPython.core.display import display, HTML
from IPython.display import FileLink
import graphviz
import uuid
import time
from datetime import datetime, timedelta, timezone
from IPython.core.magic_arguments import (argument, magic_arguments, parse_argstring)



def add_setting_to_query(query, setting):
    # TODO: better parsing. this will fail for queries like "SELECT 'there are no settings here'"
    if ' settings ' in query.replace("\r"," ").replace("\n"," ").lower():
        return query + ', ' + setting
    else:
        return query + ' SETTINGS ' + setting

# The class MUST call this class decorator at creation time
@magics_class
class JupysqlTextOutputMagics(Magics):
    def run_query(self, query):
        if 'jupysql' not in self.shell.magics_manager.magics['cell']:
            raise ModuleNotFoundError("Can't find %jupysql magic. Is the extension not loaded?")

        return self.shell.run_cell_magic('sql', '', query)
    
    def run_and_get_query_id(self, query, silent=False):
        QUERY_LOG_TIMEOUT_SECONDS = 10

        tag = f'jupyter tag {uuid.uuid4()}'
        tagged_query = add_setting_to_query(query, f"log_comment='{tag}'")

        if not silent:
            print(f'Query log_comment: {tag}')
        self.run_query(tagged_query)

        yesterday = (datetime.now(timezone.utc) + timedelta(days=-1)).strftime("%Y-%m-%d") # allow plenty of margin. maybe some server won't be set to UTC?
        original_feedback_level = self.shell.run_line_magic('config', 'SqlMagic.feedback')
        self.shell.run_line_magic('config', 'SqlMagic.feedback=0')

        # Unfortunately we can't do this in readonly mode
        # self.run_query("SYSTEM FLUSH LOGS")

        find_query_id = f"SELECT query_id, query_duration_ms FROM system.query_log WHERE log_comment='{tag}' AND event_time > '{yesterday}' ORDER BY query_duration_ms DESC LIMIT 1"
        r = self.run_query(find_query_id)
        if len(r) == 0:
            start_waiting = datetime.now()
            if not silent:
                print("Waiting for query to appear in system.query_log...")
            while len(r) == 0 and (datetime.now() - start_waiting).seconds < QUERY_LOG_TIMEOUT_SECONDS:
                time.sleep(0.5)
                r = self.run_query(find_query_id)
        
        self.shell.run_line_magic('config', f'SqlMagic.feedback={original_feedback_level}')
        if len(r) == 1:
            query_id = r[0][0]
            if not silent:
                query_duration_ms = r[0][1]
                print (f'Query {query_id} ran for {query_duration_ms} ms.') 
            return query_id
        else:    
            raise Exception("Timeout waiting for query to appear in system.query_log")
        
    @line_cell_magic
    @magic_arguments()
    @argument(
        "-s", "--silent", action="store_true", help="Don't print query id and duration",
    )
    @argument('querytext', nargs='*', help='The query to analyse')
    def qsql(self, line="", cell=""):
        args = parse_argstring(self.qsql, line)

        if len(args.querytext) > 0:
            query = " ".join(args.querytext)
        else:
            query = cell
        return self.run_and_get_query_id(query, silent=args.silent)

    
    @line_cell_magic
    def tsql(self, line="", cell=""):
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
        args = parse_argstring(self.ch_pipeline, line)
        
        if args.query_id == '':
            if len(args.querytext) > 0:
                query = " ".join(args.querytext)
            else:
                query = cell
        else:
            query_log_query = f"SELECT query FROM system.query_log WHERE query_id='{args.query_id}' LIMIT 1"
            r = self.run_query(query_log_query)
            if len(r) == 1:
                query = r[0][0]
                print(query)
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
        args = parse_argstring(self.ch_flame, line)

        if args.query_id == '':
            if len(args.querytext) > 0:
                query = " ".join(args.querytext)
            else:
                query = cell
    
            query = cell if line == "" else line
            query_id = self.run_and_get_query_id(query)
        else:
            query_id = args.query_id

        trace_query = f"SELECT arrayStringConcat(arrayReverse(arrayMap(x -> demangle(addressToSymbol(x)), trace)), ';') AS stack, count() AS samples FROM system.trace_log WHERE query_id = '{query_id}' GROUP BY trace SETTINGS allow_introspection_functions=1"
        r = self.run_query(trace_query)
        result = '\n'.join([f'{row[0]} {row[1]}' for row in r])

        filename = f'{query_id}.flamegraph'
        with open(filename, 'w') as text_file:
            text_file.write(result)
        print(f'Download {filename} and import it in https://www.speedscope.app/')


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
