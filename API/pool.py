from contextlib import contextmanager
import Queue

@contextmanager
def pooled_connection(pool):
    """
    Provides some syntactic sugar for using a ConnectionPool. Example use:
    
        pool = ConnectionPool(SolrConnection, 'http://localhost:8080/solr')
        with pooled_connection(pool) as conn:
            docs = conn.query('*:*')
    """
    conn = pool.get()
    try:
        yield conn
    except:
        raise
    else:
        # only return connection to pool if an exception wasn't raised
        pool.put(conn)

class ConnectionPool(object):
    "Thread-safe connection pool."
    
    def __init__(self, klass, *args, **kwargs):
        """
        Initialize a new connection pool, where klass is the connection class.
        Provide any addition args or kwargs to pass during initialization of new connections.
        
        If a kwarg named pool_size is provided, it will dictate the maximum number of connections to retain in the pool.
        If none is provided, it will default to 20.
        """
        self._args = args
        self._kwargs = kwargs
        self._queue = Queue.Queue(self._kwargs.pop('pool_size', 20))
        self._klass = klass
    
    def get(self):
        "Get an available connection, creating a new one if needed."
        try:
            return self._queue.get_nowait()
        except Queue.Empty:
            return self._klass(*self._args, **self._kwargs)
    
    def put(self, conn):
        "Return a connection to the pool."
        try:
            self._queue.put_nowait(conn)
        except Queue.Full:
            pass