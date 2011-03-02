from __future__ import with_statement
import time
from pytyrant import PyTyrant, TyrantError
from operator import add
from pool import ConnectionPool, pooled_connection


# If running locally just have this be localhost.
# If not change at will
node = '127.0.0.1'
_nodes = [node+":7705"]
    
# Holds or persisted connections (until close() is called)
_tt = {}

def close():
    '''Closes all the open Tokyo Tyrant socket connections.'''
    for nodekey in _tt.keys():
        _tt[nodekey].get().close()
        del _tt[nodekey]

def _conn():
    '''Retrieves or opens correctly hashed connection pool for your key.''' 
    nodekey = _nodes[0]
    if _tt.has_key(nodekey):
        return _tt[nodekey]
    else:
        host, port = nodekey.split(':')
        _tt[nodekey] = ConnectionPool(PyTyrant.open, host, int(port))
        return _tt[nodekey]

def delete(key):
    '''Deletes k/v pair based on key.'''
    with pooled_connection(_conn()) as conn:
        del conn[key]

def get(key):
    '''Returns k/v pair based on key.'''    

    with pooled_connection(_conn()) as conn:
        try:
            value = conn[key]
        except KeyError:
            value = None
    
    if value is not None:
        return value
    else:
        return None

def getmany_serially(keys):
    return [get(key) for key in keys]

def append(key, value):
    with pooled_connection(_conn()) as conn:
        conn.concat(key, value)

def set(key, value):
    with pooled_connection(_conn()) as conn:
        conn[key] = value

def count():
    with pooled_connection(_conn()) as conn:
        return len(conn)
