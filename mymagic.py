# This code can be put in any Python module, it does not require IPython
# itself to be running already.  It only creates the magics subclass but
# doesn't instantiate it yet.
from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
import urllib
from IPython.core.display import display, HTML
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
    
    def run_and_get_query_id(self, query):    
        tag = f'jupyter tag {uuid.uuid4()}'
        tagged_query = add_setting_to_query(query, f"log_comment='{tag}'")
        self.run_query(tagged_query)

        yesterday = (datetime.now(timezone.utc) + timedelta(days=-1)).strftime("%Y-%m-%d") # allow plenty of margin. maybe some server won't be set to UTC?
        original_feedback_level = self.shell.run_line_magic('config', 'SqlMagic.feedback')
        self.shell.run_line_magic('config', 'SqlMagic.feedback', 0)

        # Unfortunately we can't do this in readonly mode
        # self.run_query("SYSTEM FLUSH LOGS")
        start_waiting = datetime.now()
        try:
            while (datetime.now() - start_waiting).seconds < 10:
                time.sleep(0.5)
                r = self.run_query(f"SELECT query_id FROM system.query_log WHERE log_comment='{tag}' AND event_time > '{yesterday}' ORDER BY event_time_microseconds DESC LIMIT 1")
                if r and len(r) == 1:
                    return r[0][0]
    
            raise Exception("Timeout waiting for query to appear in system.query_log")
        finally:
            self.shell.run_line_magic('config', 'SqlMagic.feedback', original_feedback_level)
        
    @line_cell_magic
    def qsql(self, line="", cell=""):
        if 'jupysql' not in self.shell.magics_manager.magics['cell']:
            raise ModuleNotFoundError("Can't find %jupysql magic. Is the extension not loaded?")

        query = cell if line == "" else line
        return self.run_and_get_query_id(query)

    
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
    @argument('query_parameters', nargs='*', help='Pass file patterns to omit')
    # arser.add_argument('rest', nargs=argparse.REMAINDER, help='The rest of the command line arguments')
    def ch_plotpipeline(self, line="", cell=""):
        args = parse_argstring(self.ch_plotpipeline, line)

        if 'jupysql' not in self.shell.magics_manager.magics['cell']:
            raise ModuleNotFoundError("Can't find %jupysql magic. Is the extension not loaded?")
        
        if len(args.query_parameters) > 0:
            query = " ".join(args.query_parameters)
        else:
            query = cell

        if args.compact:
            is_compact = 1
        else:
            is_compact = 0
        pipeline_query = add_setting_to_query(f"EXPLAIN PIPELINE graph=1, compact={is_compact} { query }",
        "allow_experimental_analyzer = 1")
        
        r = self.run_query(cell=pipeline_query)
        digraph = "\n\r".join([l[0] for l in r])

        if args.create_link:
            url = f"https://dreampuf.github.io/GraphvizOnline/#{urllib.parse.quote(digraph)}"
            display(HTML(f'<h3><a href="{url}">pipeline graph</a></h3>'))
        else:
            display(graphviz.Source(digraph))


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
