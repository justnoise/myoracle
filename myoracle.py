# Copyright (C) 2011-2012 by Brendan Cox

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

################################################################################
#
# myoracle.py, a little script for interactive querying of Oracle DBs.
# features: readline and mysql-like output formatting.
#
# Contact: Brendan Cox
# Antispambot email: ''.join(['just', 'noise', '@', 'gmail', '.com'])
# 
# Bugs: This isn't super efficient for very large result sets (>
# 10,000 rows).  I'm not planning on optimizing for larger result sets
# since this for use on the console and most people keep their console
# output to <= 10,000 rows.  Besides, who wants to pick through >
# 1,000 rows on the command line (OK, I sometimes do...)?  Anyways,
# I've hardcoded the DB class to only return 10,000 rows.  This
# prevents users from watching millions of rows scroll by.  If you
# want something different, change it and test it.
#
# todo, features: add a bit of sugar for describe statements
#       get readline to recall entire queries from a multiline query
#       more macros (execute scripts + other syntactic sugar)
#       print errors to stderr instead of stdout
#       make sure that ReadlineHistoryFile is (1) needed, (2) gives us anything
#
################################################################################

import sys
import os
import types
import readline
import cx_Oracle
from optparse import OptionParser
import datetime
import re
import subprocess
import pdb 

# CONSTANTS
HORIZONTAL_TABLE = 0
HORIZONTAL_TABS= 1
VERTICAL = 2

# OPTIONS
null_display = 'NULL'
my_defaults = {'db_host' : 'host',
               'db_user' : 'user',
               'db_password' : 'password',
               'db_service_name' : ''}

#other things that we might want to make options:
# debugging (e.g. print out the query that's being passed to oracle)
# want_header
# column_separator

illegal_query_words = ('alter',
                       'checkpoint',
                       'comment',
                       'commit',
                       'constraint',
                       'create',
                       'delete',
                       'drop',
                       'insert',
                       'merge',
                       'savepoint',
                       'set',
                       'truncate',
                       'update');
#-------------------------------------------------------------------------------
class SimpleDB(object):
    def __init__(self, h, u, p, sn, fetchsize=10000, max_fetchsize = 10000):
        connect_str = '%s/%s@%s/%s' % (u,p,h,sn)    
        self.db = cx_Oracle.Connection(connect_str)
        self.cursor = self.db.cursor()
        self.cursor.arraysize = fetchsize
        self.max_fetchsize = max_fetchsize

    def cleanup(self):
        self.cursor.close()
        self.db.close()

    def query(self, q, headers=False, obj=False, vector=False, array=False):
        '''note: this doesn't work for certain types of queries such
        as 'describe <tablename> and probably doesn't work for inserts
        or updates to tables'''
        self.cursor.execute(q)
        result = []
        num_fetched = 0
        while True:
            r = self.cursor.fetchmany(self.cursor.arraysize)
            if not r:
                break
            result.extend(map(list,r))
            num_fetched += self.cursor.arraysize
            if num_fetched >= self.max_fetchsize:
                break
        if headers:
            return result, self.column_names()
        return result
    
    def commit(self):
        self.db.commit()
    
    def column_names(self):
        return list(zip(*self.cursor.description)[0])

    def column_types(self):
        return list(zip(*self.cursor.description)[1])


#-------------------------------------------------------------------------------
class NullType(object):
    """stupid hack to display Nones from the DB as 'NULL' """
    def __init__(self, null_string = 'NULL'):
        self.null_string = null_string
    def __str__(self):
        return self.null_string
    def __repr__(self):
        return self.null_string
    

#-------------------------------------------------------------------------------
class ReadlineHistoryFile(object):
    ''' A simple class to help in reading and writing readline history
    files in a safe manner (i.e. make some attempt not to clobber
    existing non-readline files)'''
    def __init__(self, history_file_name):
        self.valid_history_file = False
        self.history_file_name = history_file_name
        self.history_file_directory = os.path.expanduser('~/')
        self.history_file_path = self.history_file_directory + self.history_file_name
        self.open_history_file()

    def open_history_file(self):
        try:
            #todo, possibly try to see what file utility thinks this file is
            readline.read_history_file(self.history_file_path)
            self.valid_history_file = True
        except IOError:
            self.valid_history_file = False

    def write_history_file(self):
        # if it doesn't exist, create it and write it
        # or write the file if we previously opened it successfully
        if (not os.path.exists(self.history_file_path) or
            self.valid_history_file):
            readline.write_history_file(self.history_file_path)

            
#-------------------------------------------------------------------------------
class MultilineReadline(object):
    '''Simple class to adapt readline's raw input to allowing
    multiline statements.  To do this, we simply buffer data until we
    read in a statement terminator.  
    WARNING: Since raw_input strips newlines we will insert a space at
    the end of a line when the user doesn't complete a statement.
    This is suitable for SQL interpreter but probably not suitable for
    other languages and purposes'''
    def __init__(self, empty_buffer_prompt, multiline_prompt, statement_terminators):
        self.empty_buffer_prompt = empty_buffer_prompt + ' '
        self.multiline_prompt = multiline_prompt + ' '
        self.statement_terminators = statement_terminators
        self.statement_buffer = ''
        self.quit_statements = ('quit', 'quit;', 'exit', 'exit;')

    def dump_buffer(self):
        self.statement_buffer = ''

    def clean_buffer(self):
        ''' turn an all whitespace buffer into an empty buffer (keeps
        our prompt correct) and also insert a single space at the end
        of an incomplete buffer (helps to mitigate raw_input stripping trailing
        newlines'''
        tmp_buffer = self.statement_buffer
        tmp_buffer = tmp_buffer.strip()
        if not tmp_buffer:
            self.statement_buffer = ''
        else:
            self.statement_buffer = self.statement_buffer.rstrip() + ' '

    def get_prompt(self):
        if self.statement_buffer:
            return self.multiline_prompt
        else:
            return self.empty_buffer_prompt

    def check_for_quit(self):
        lower_statement_buffer = self.statement_buffer.lower()
        for quit_command in self.quit_statements:
            if lower_statement_buffer == quit_command:
                raise EOFError

    def get_statement_from_buffer(self):
        # get first statement available, if no statement then return empty string
        smallest_index = len(self.statement_buffer) + 1
        for term in self.statement_terminators:
            i = self.statement_buffer.find(term)
            if i >= 0 and i < smallest_index:
                smallest_index = i + len(term)
        part = ''
        if smallest_index <= len(self.statement_buffer):
            part = self.statement_buffer[:(smallest_index)]
            self.statement_buffer = self.statement_buffer[smallest_index:]
            self.statement_buffer = self.statement_buffer.lstrip()
        return part

    def get_query(self):
        # see if we have a complete statement, if so return it
        # otherwise get the next line of the statement
        new_statement = self.get_statement_from_buffer()
        if new_statement:
            return new_statement
        else:
            more_input = raw_input(self.get_prompt())
            self.statement_buffer += more_input
            self.check_for_quit()
            new_statement = self.get_statement_from_buffer()
            if new_statement:
                return new_statement
            else:
                self.clean_buffer()


#-------------------------------------------------------------------------------
class View(object):
    def __init__(self):
        self.output = ''

    def get_result_set_summary(self, num_rows):
        if num_rows > 0:
            return '%d rows in set\n\n' % num_rows
        else:
            return 'Empty set\n\n'


class HorizontalView(View):
    def __init__(self):
        super(HorizontalView, self).__init__()

    def format_results(self, results, header):
        if not results:
            self.print_result_set_summary(0)
            return self.output
        ## enchancement, todo, get the actual type of each column and use that for formatting and left/right justification
        left_justification = self.get_left_justification(results)
        translate_none_to_null(results)
        if self.want_header:
            results = [header] + results
        string_results = [map(str, row) for row in results]  # create a 2d list of strings
        col_width = self.get_col_width(string_results)
        format_string = self.create_format_string(col_width, left_justification)
        self.print_header_footer(col_width)
        for row_num, row in enumerate(string_results):
            self.output += format_string % tuple(row)
            if row_num == 0:
                self.print_header_footer(col_width)
        self.print_header_footer(col_width)
        self.print_result_set_summary(row_num) # remember, header is included in this count
        return self.output

class HorizontalTabView(HorizontalView):
    def __init__(self):
        super(HorizontalTabView, self).__init__()
        self.header_footer_separator = ''
        self.separator = '\t'
        self.want_header = False

    def create_format_string(self, col_width, left_justification):
        # simply make a list of %s separated by \t
        fmt_list = ['%s' for unused in col_width]
        fmt = self.separator.join(fmt_list) + '\n'
        return fmt
    
    def get_left_justification(self, results):
        '''we want certain types to be displayed left justified.  This could be done
        without iterating through the entire result set by looking at the column
        types returned from the DB, however, those aren't portable from 1 DB to the next.
        Since we've made the assumption of small result sets, just iterate through results'''
        left_justify = [False] * len(results[0])
        return left_justify

    def get_col_width(self, string_results):
        col_width = [0] * len(string_results[0])
        return col_width

    def print_header_footer(self, col_width):
        pass

    def print_result_set_summary(self, num_rows):
        pass        


class HorizontalTableView(HorizontalView):
    def __init__(self):
        super(HorizontalTableView, self).__init__()
        self.header_footer_separator = '+'
        self.separator = '|'
        self.want_header = True

    def create_format_string(self, col_width, left_justification):
        # It's shit like this that makes people hate you Python... But don't worry, I still love you.
        #basically we're concating a few strings but the multiplication
        #is used to decide whether to insert a '-' in there (based on whether
        #left justify is True or False)
        # make a list of string that look like ' %-45s '
        fmt_list = [' %' + '-' * left_justify + '0' + str(width) + 's ' for width, left_justify in zip(col_width, left_justification)]
        fmt = self.separator + self.separator.join(fmt_list) + self.separator  + '\n'
        return fmt
    
    def get_header_footer(self, col_width):
        line_segments = ['-' + ('-' * width) + '-' for width in col_width]
        the_line = self.header_footer_separator + self.header_footer_separator.join(line_segments) + self.header_footer_separator
        return the_line
    
    def get_left_justification(self, results):
        '''we want certain types to be displayed left justified.  This could be done
        without iterating through the entire result set by looking at the column
        types returned from the DB, however, those aren't portable from 1 DB to the next.
        Since we've made the assumption of small result sets, just iterate through results'''
        left_justify = [False] * len(results[0])
        left_justified_types = (str,)
        for row in results:
            for i, col in enumerate(row):
                if col is not None  and left_justify[i] is False and type(col) in left_justified_types:
                    left_justify[i] = True
        return left_justify

    def get_col_width(self, string_results):
        col_width = [0] * len(string_results[0])
        for i, row in enumerate(string_results):
            for j, col in enumerate(row):
                if len(col) > col_width[j]:
                    col_width[j] = len(col)
        return col_width

    def print_header_footer(self, col_width):
        self.output += self.get_header_footer(col_width) + '\n'

    def print_result_set_summary(self, num_rows):
        self.output += self.get_result_set_summary(num_rows)


class VerticalView(View):
    def __init__(self):
        super(VerticalView, self).__init__()

    def format_results(self, results, header):
        def get_header_width(header):
            ''' get the maximum width of the strings in our header (but
            not greater than 40 chars)'''
            return min(40, max(map(len, header)))
        def get_row_separator(row_num):
            stars = '*' * 30
            return '%s %d. row %s\n' % (stars, row_num, stars)
        def get_single_format(width):
            return '%' + str(width) + 's: %s\n'
    
        if not results:
            self.output += self.get_result_set_summary(0)
            return self.output
        header_width = get_header_width(header)
        translate_none_to_null(results)
        string_results = [map(str, row) for row in results]  # create a 2d list of strings
        fmt = get_single_format(header_width)
        for row_num, row in enumerate(string_results):
            self.output += get_row_separator(row_num + 1)
            for h, col in zip(header, row):
                self.output += fmt % (h, col)
        self.output += self.get_result_set_summary(row_num+1)
        return self.output


#-------------------------------------------------------------------------------
def get_preamble():
    preamble = 'Welcome to MyOracle!\n'
    preamble += 'Commands end with ; or \g.  Use \c to cancel a query.\n'
    preamble += '\n'
    preamble += 'Type Ctrl-d (EOF) to quit\n'
    return preamble

def translate_none_to_null(results):
    ''' Nulls show up as NoneType and that gets translated into
    None which can be misleading, translate NoneType into a NullType
    that has a better string representation'''
    for i, row in enumerate(results):
        for j, col in enumerate(row):
            if isinstance(col, types.NoneType):
                results[i][j] = NullType(null_display) 

def handle_illegal_query(query_statement):
    # todo, print this out to stderr
    msg =  'ERROR: Illegal query!\n'
    msg += 'In its current form, this tool should only be used for select queries.\n\n'
    msg += query_statement
    print msg + '\n'
    
def handle_bad_macro(query_object):
    msg =  'ERROR: Bad Macro!\n'
    msg += 'It looks like there is a bad macro specified near.\n'
    msg += query_object.bad_macro_string
    print msg + '\n'
    
def get_view(view_type):
    if view_type == HORIZONTAL_TABS:
        return HorizontalTabView()
    elif view_type == HORIZONTAL_TABLE:
        return HorizontalTableView()
    elif view_type == VERTICAL:
        return VerticalView()
    return None

#-------------------------------------------------------------------------------
class SqlQuery(object):
    def __init__(self, query_statement, cmd_line_options):
        self.query_statement = query_statement
        self.cmd_line_options = cmd_line_options
        self.view_type = HORIZONTAL_TABLE
        self.cancelled = False
        self.illegal_query = False
        self.bad_macro = False
        self.bad_macro_string = ''
        self.parse_query()

    def parse_query(self):
        """ok, so it's not really parsing... kind of inspecting"""
        if self.query_statement.endswith('\\c') or self.query_statement.endswith('\\C'):
            self.cancelled = True
            return
        
        if self.is_illegal_query():
            self.illegal_query = True
            
        self.do_macro_substitution()
        
        if self.cmd_line_options.execute_query:
            self.view_type = HORIZONTAL_TABS
            if self.query_statement.endswith(';'):
                self.query_statement = self.query_statement[:-1]

        elif self.query_statement.endswith('\\g') or self.query_statement.endswith('\\G'):
            self.query_statement = self.query_statement[:-2]
            self.view_type = VERTICAL
            
        elif self.query_statement.endswith(';'):
            self.query_statement = self.query_statement[:-1]
            self.view_type = HORIZONTAL_TABLE

    def do_macro_substitution(self):
        self.date_macro_substitution()
    
    def date_macro_substitution(self):
        # I got tired of writing things like to_date(fmt_stmnt, date_str) so this helps
        def get_actual_date(date_str):
            time_formats = ['%Y-%m-%d %H:%M:%S',
                            '%Y-%m-%d']
            for t_fmt in time_formats:
                # attempt to parse, if it fails, try the next thing
                try:
                    time_tuple = datetime.datetime.strptime(date_str.strip(), t_fmt)
                    return "to_date('%s', 'YYYY-MM-DD HH24:MI:SS')" % time_tuple.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue
            return ''

        date_macro_re = re.compile(r'#date\(\'(.*?)\'\)')
        macro_match = date_macro_re.search(self.query_statement)
        while macro_match:
            match_start, match_end = macro_match.span()
            the_date_str = macro_match.group(1)
            actual_date = get_actual_date(the_date_str)
            if not actual_date:
                self.bad_macro = True
                self.bad_macro_string = the_date_str
                return
            self.query_statement = self.query_statement[:match_start] + actual_date + self.query_statement[match_end:]
            macro_match = date_macro_re.search(self.query_statement)
        #print self.query_statement

    def is_illegal_query(self):
        lower_query = self.query_statement.strip().lower()
        for word in illegal_query_words:
            if lower_query.startswith(word):
                return True
        return False

def handle_single_query(db, query_object, options):
    try:
        success = True
        if query_object.cancelled:
            success = True
        elif query_object.illegal_query:
            handle_illegal_query(query_object.query_statement)
            success = False            
        elif query_object.bad_macro:
            handle_bad_macro(query_object)
            success = False
        else:
            # do it!
            results, header = db.query(query_object.query_statement, True)
            view = get_view(query_object.view_type)
            result_str = view.format_results(results, header)
            # write it out to stdout or a pager
            if options.pager:
                pipe = subprocess.Popen([options.pager], stdin=subprocess.PIPE)
                pipe.communicate(result_str)
                print
            else:
                print result_str
            success = True
        return success
    except cx_Oracle.Error, e:
        print "Error executing statement:"
        print e
        return False    

def run_ui_query_loop(db, options):
    readline_history = ReadlineHistoryFile('.myoracle_history')
    statement_terminators = (';', '\\c', '\\C', '\\g', '\\G')
    multiline_readline = MultilineReadline('myoracle>', '       ->', statement_terminators)
    print 'Connecting to %s/%s@%s/%s' % (options.db_user, options.db_password, options.db_host, options.db_service_name)
    print get_preamble()
    try:
        while True:
            query_statement = multiline_readline.get_query()
            if query_statement:
                query_object = SqlQuery(query_statement, options)
                success = handle_single_query(db, query_object, options)
                if not success:
                    multiline_readline.dump_buffer()
    except (EOFError, KeyboardInterrupt):
        # quit gracefully when the user hits Ctrl-d or Ctrl-c
        print '\nExiting MyOracle'
        readline_history.write_history_file()

def run_it(options):
    db = SimpleDB(h = options.db_host, u = options.db_user, p = options.db_password, sn=options.db_service_name)
    if options.execute_query:
        query_object = SqlQuery(options.execute_query, options)
        handle_single_query(db, query_object, options)
    else:
        run_ui_query_loop(db, options)
    sys.exit(0)

#-------------------------------------------------------------------------------
def parse_arguments():
    parser = OptionParser()
    parser.add_option('-s', '--server', dest='db_host', default=my_defaults['db_host'],
                      help='Oracle server')
    parser.add_option('-u', '--user', dest='db_user', default=my_defaults['db_user'],
                      help='Oracle user name')
    parser.add_option('-p', '--password', dest='db_password', default=my_defaults['db_password'],
                      help='Oracle password')
    parser.add_option('-n', '--service_name', dest='db_service_name', default=my_defaults['db_service_name'],
                      help='Oracle service_name or SID')
    parser.add_option('-e', '--execute', dest='execute_query', default='',
                      help='Single query to execute, tab delimeted results are printed to stdout')
    parser.add_option('-P', '--pager', dest='pager', default='',
                      help='Pipe results through the specified pager application')

    (options, args) = parser.parse_args()
    db_host = options.db_host
    db_user = options.db_user
    db_password = options.db_password
    db_service_name = options.db_service_name
    return options, args

#-------------------------------------------------------------------------------
if __name__ == '__main__':
    options, args = parse_arguments()
    run_it(options)
