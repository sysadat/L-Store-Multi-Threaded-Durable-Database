from lstore.table import Table, Record
from lstore.index import Index
import threading
import sys

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.uncommittedQueries = [] #tuples of the key value and the index structure pointer
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result, table, rid = query(*args)
            self.uncommittedQueries.append((query, rid, table))
            if result == False: # If the query has failed the transaction should abort
                # print("aborted " + str(threading.get_ident()))
                self.uncommittedQueries.pop() #no need to "undo" the aborted transaction
                return self.abort(table)
        # print("committed "+ str(threading.get_ident()))
        return self.commit(table)

    def abort(self, table):
        #TODO: do roll-back and any other necessary operations
        thread_lock = threading.RLock()
        for query in reversed(self.uncommittedQueries):
            fn_name = query[0].__name__
            if fn_name == 'update' or fn_name == 'increment':
                thread_lock.acquire()
                table = query[2]
                rid = query[1]
                table.__undo_update__(rid)
                thread_lock.release()
            else:
                pass
                # print("didn't find an update, val is " + fn_name)

        thread_lock.acquire()
        table.release_locks()
        thread_lock.release()
        return False

    def commit(self, table):
        thread_lock = threading.RLock()
        # TODO: commit to database
        while len(self.uncommittedQueries) != 0:
            query, key, index = self.uncommittedQueries.pop()

        thread_lock.acquire()
        table.release_locks()
        thread_lock.release()
        return True
