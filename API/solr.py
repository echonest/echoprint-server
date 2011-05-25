# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# $Id$

# many optimizations and changes to this by various EN employees over the years:

# Ryan McKinley, Brian Whitman, Adam Baratz, Aaron Mandel


"""

A simple Solr client for python.

Features
--------
 * Supports SOLR 1.2+
 * Supports http/https and SSL client-side certificates
 * Uses persistent HTTP connections by default
 * Properly converts to/from SOLR data types, including datetime objects
 * Supports both querying and update commands (add, delete). 
 * Supports batching of commands
 * Requires python 2.3+ 
 
Connections
-----------
`SolrConnection` can be passed in the following parameters. 
Only `url` is required,.

    url -- URI pointing to the SOLR instance. Examples:

        http://localhost:8080/solr
        https://solr-server/solr

        Your python install must be compiled with SSL support for the 
        https:// schemes to work. (Most pre-packaged pythons are.)

    persistent -- Keep a persistent HTTP connection open.  
        Defaults to true.

    timeout -- Timeout, in seconds, for the server to response. 
        By default, use the python default timeout (of none?)
        NOTE: This changes the python-wide timeout.

    ssl_key, ssl_cert -- If using client-side key files for 
        SSL authentication,  these should be, respectively, 
        your PEM key file and certificate file


Once created, a connection object has the following public methods:

    query (q, fields=None, highlight=None, 
           score=True, sort=None, **params)

            q -- the query string.
    
            fields -- optional list of fields to include. It can be either
                a string in the format that SOLR expects ('id,f1,f2'), or 
                a python list/tuple of field names.   Defaults to returning 
                all fields. ("*")

            score -- boolean indicating whether "score" should be included
                in the field list.  Note that if you explicitly list
                "score" in your fields value, then this parameter is 
                effectively ignored.  Defaults to true. 

            highlight -- indicates whether highlighting should be included.
                `highlight` can either be `False`, indicating "No" (the 
                default),  `True`, incidating to highlight any fields 
                included in "fields", or a list of field names.

            sort -- list of fields to sort by. 

            Any parameters available to SOLR 'select' calls can also be 
            passed in as named parameters (e.g., fq='...', rows=20, etc).  
    
            Many SOLR parameters are in a dotted notation (e.g., 
            `hl.simple.post`).  For such parameters, replace the dots with 
            underscores when calling this method. (e.g., 
            hl_simple_post='</pre'>)


            Returns a Response object

    add(**params)
    
            Add a document.  Pass in all document fields as 
            keyword parameters:
            
                add(id='foo', notes='bar')
                    
            You must "commit" for the addition to be saved.
            This command honors begin_batch/end_batch.
                    
    add_many(lst)
    
            Add a series of documents at once.  Pass in a list of 
            dictionaries, where each dictionary is a mapping of document
            fields:
            
                add_many( [ {'id': 'foo1', 'notes': 'foo'}, 
                            {'id': 'foo2', 'notes': 'w00t'} ] )
            
            You must "commit" for the addition to be saved.
            This command honors begin_batch/end_batch.
            
    delete(id)
    
            Delete a document by id. 
            
            You must "commit" for the deletion to be saved.
            This command honors begin_batch/end_batch.

    delete_many(lst)

            Delete a series of documents.  Pass in a list of ids.
            
            You must "commit" for the deletion to be saved.
            This command honors begin_batch/end_batch.

    delete_query(query)
    
            Delete any documents returned by issuing a query. 
            
            You must "commit" for the deletion to be saved.
            This command honors begin_batch/end_batch.


    commit(wait_flush=True, wait_searcher=True)

            Issue a commit command. 

            This command honors begin_batch/end_batch.

    optimize(wait_flush=True, wait_searcher=True)

            Issue an optimize command. 

            This command honors begin_batch/end_batch.

    begin_batch()
    
            Begin "batch" mode, in which all commands to be sent
            to the SOLR server are queued up and sent all at once. 
            
            No update commands will be sent to the backend server
            until end_batch() is called. Not that "query" commands
            are not batched.
            
            begin_batch/end_batch transactions can be nested. 
            The transaction will not be sent to the backend server
            until as many end_batch() calls have been made as 
            begin_batch()s. 

            Batching is completely optional. Any update commands 
            issued outside of a begin_batch()/end_batch() pair will 
            be immediately processed. 

    end_batch(commit=False)
    
            End a batching pair.  Any pending commands are sent
            to the backend server.  If "True" is passed in to 
            end_batch, a <commit> is also sent. 

    raw_query(**params)

            Send a query command (unprocessed by this library) to
            the SOLR server. The resulting text is returned un-parsed.

                raw_query(q='id:1', wt='python', indent='on')
                
            Many SOLR parameters are in a dotted notation (e.g., 
            `hl.simple.post`).  For such parameters, replace the dots with 
            underscores when calling this method. (e.g., 
            hl_simple_post='</pre'>)

            

Query Responses
---------------

    Calls to connection.query() return a Response object. 
    
    Response objects always have the following properties: 
    
        results -- A list of matching documents. Each document will be a 
            dict of field values. 
            
        results.start -- An integer indicating the starting # of documents
        
        results.numMatches -- An integer indicating the total # of matches.
        
        header -- A dict containing any responseHeaders.  Usually:
        
            header['params'] -- dictionary of original parameters used to
                        create this response set. 
                        
            header['QTime'] -- time spent on the query
            
            header['status'] -- status code.
            
            See SOLR documentation for other/typical return values.
            This may be settable at the SOLR-level in your config files.
        

        next_batch() -- If only a partial set of matches were returned
            (by default, 10 documents at a time), then calling 
            .next_batch() will return a new Response object containing 
            the next set of matching documents. Returns None if no
            more matches.  
            
            This works by re-issuing the same query to the backend server, 
            with a new 'start' value.
            
        previous_batch() -- Same as next_batch, but return the previous
            set of matches.  Returns None if this is the first batch. 

    Response objects also support __len__ and iteration. So, the following
    shortcuts work: 
    
        responses = connection.query('q=foo')
        print len(responses)
        for document in responses: 
            print document['id'], document['score']


    If you pass in `highlight` to the SolrConnection.query call, 
    then the response object will also have a highlight property, 
    which will be a dictionary.



Quick examples on use:
----------------------

Example showing basic connection/transactions

    >>> from solr import *
    >>> c = SolrConnection('http://localhost:8983/solr') 
    >>> c.add(id='500', name='python test doc', inStock=True)
    >>> c.delete('123')
    >>> c.commit()



Examples showing the search wrapper

    >>> response = c.query('test', rows=20)
    >>> print response.results.start
     0
    >>> for match in response: 
    ...     print match['id'], 
      0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
    >>> response = response.next_batch()
    >>> print response.results.start
     20
 

Add 3 documents and delete 1, but send all of them as a single transaction.
    
    >>> c.begin_batch()
    >>> c.add(id="1")
    >>> c.add(id="2")
    >>> c.add(id="3")
    >>> c.delete(id="0")
    >>> c.end_batch(True)


Enter a raw query, without processing the returned HTML contents.
    
    >>> print c.query_raw(q='id:[* TO *]', wt='python', rows='10')

"""
import sys
import socket
import httplib
import urlparse
import codecs
import urllib
import datetime
import time
from StringIO import StringIO
from xml.sax import make_parser
from xml.sax import _exceptions
from xml.sax.handler import ContentHandler
from xml.sax.saxutils import escape, quoteattr
from xml.dom.minidom import parseString
from types import BooleanType, FloatType, IntType, ListType, LongType, StringType, UnicodeType
from contextlib import contextmanager
import Queue


__version__ = "1.3.0"

__all__ = ['SolrException', 'SolrHTTPException', 'SolrContentException',
           'SolrConnection', 'Response']



# EN special-use methods


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
    except Exception:
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

class SolrConnectionPool(ConnectionPool):
    def __init__(self, url, **kwargs):
        ConnectionPool.__init__(self, SolrConnection, url, **kwargs)

    
def str2bool(s):
    if(isinstance(s,bool)):
        return s
    if s in ['Y', 'y']:
        return True
    if s in ['N', 'n']:
        return False
    if s in ['True', 'true']:
        return True
    elif s in ['False', 'false']:
        return False
    else:
        raise ValueError, "Bool-looking string required."

def reallyunicode(s, encoding="utf-8"):
    """
    Try the user's encoding first, then others in order; break the loop as 
    soon as we find an encoding we can read it with. If we get to ascii,
    include the "replace" argument so it can't fail (it'll just turn 
    everything fishy into question marks).
    
    Usually this will just try utf-8 twice, because we will rarely if ever
    specify an encoding. But we could!
    """
    if type(s) is StringType:
        for args in ((encoding,), ('utf-8',), ('latin-1',), ('ascii', 'replace')):
            try:
                s = s.decode(*args)
                break
            except UnicodeDecodeError:
                continue
    if type(s) is not UnicodeType:
        raise ValueError, "%s is not a string at all." % s
    return s

def reallyUTF8(s):
    return reallyunicode(s).encode("utf-8")

def makeNiceLucene(text):
    #http://lucene.apache.org/java/docs/queryparsersyntax.html#Escaping%20Special%20Characters
    text = re.sub(r'\bAND\b', '\AND', text)
    text = re.sub(r'\bOR\b', '\OR', text)
    text = re.sub(r'\bNOT\b', '\NOT', text)
    return re.sub(r"([\+\-\&\|\!\(\)\{\}\[\]\;\^\"\~\*\?\:\\])",r"\\\1", text)
    


# ===================================================================
# Exceptions
# ===================================================================
class SolrException(Exception):
    """ An exception thrown by solr connections """
    def __init__(self, httpcode, reason=None, body=None):
        self.httpcode = httpcode
        self.reason = reason
        self.body = body

    def __repr__(self):
        return 'HTTP code=%s, Reason=%s, body=%s' % (
                    self.httpcode, self.reason, self.body)

    def __str__(self):
        return 'HTTP code=%s, reason=%s' % (self.httpcode, self.reason)

class SolrHTTPException(SolrException):
    pass

class SolrContentException(SolrException):
    pass

# ===================================================================
# Connection Object
# ===================================================================
class SolrConnection:
    """ 
    Represents a Solr connection. 

    Designed to work with the 2.2 response format (SOLR 1.2+).
    (though 2.1 will likely work.)

    """

    def __init__(self, url,
                 persistent=True,
                 timeout=None, 
                 ssl_key=None, 
                 ssl_cert=None,
                 invariant="",
                 post_headers={}):

        """
            url -- URI pointing to the SOLR instance. Examples:

                http://localhost:8080/solr
                https://solr-server/solr

                Your python install must be compiled with SSL support for the 
                https:// schemes to work. (Most pre-packaged pythons are.)

            persistent -- Keep a persistent HTTP connection open.  
                Defaults to true

            timeout -- Timeout, in seconds, for the server to response. 
                By default, use the python default timeout (of none?)
                NOTE: This changes the python-wide timeout.

            ssl_key, ssl_cert -- If using client-side key files for 
                SSL authentication,  these should be, respectively, 
                your PEM key file and certificate file

        """

                
        self.scheme, self.host, self.path = urlparse.urlparse(url, 'http')[:3]
        self.url = url

        assert self.scheme in ('http','https')
        
        self.persistent = persistent
        self.reconnects = 0
        self.timeout = timeout
        self.ssl_key = ssl_key
        self.ssl_cert = ssl_cert
        self.invariant = invariant
        
        if self.scheme == 'https': 
            self.conn = httplib.HTTPSConnection(self.host, 
                   key_file=ssl_key, cert_file=ssl_cert)
        else:
            self.conn = httplib.HTTPConnection(self.host)

        self.batch_cnt = 0  #  this is int, not bool!
        self.response_version = 2.2 
        self.encoder = codecs.getencoder('utf-8')
        
        #responses from Solr will always be in UTF-8
        self.decoder = codecs.getdecoder('utf-8')

        # Set timeout, if applicable.
        if timeout:
            socket.setdefaulttimeout(timeout)

        self.xmlheaders = {'Content-Type': 'text/xml; charset=utf-8'}
        self.jsonheaders = {'Content-Type': 'text/json; charset=utf-8'}
        self.xmlheaders.update(post_headers)
        if not self.persistent: 
            self.xmlheaders['Connection'] = 'close'

        self.form_headers = {
                'Content-Type': 
                'application/x-www-form-urlencoded; charset=utf-8'}

        if not self.persistent: 
            self.form_headers['Connection'] = 'close'



    # ===================================================================
    # XML Parsing support
    # ===================================================================  
    def parse_query_response(self,data, params, connection):
        """
        Parse the XML results of a /select call. 
        """
        parser = make_parser()
        handler = ResponseContentHandler()
        parser.setContentHandler(handler)
        parser.parse(data)

        if handler.stack[0].children: 
            response = handler.stack[0].children[0].final
            response._params = params
            response._connection = connection
            return response
        else: 
            return None


    def parse_query_response_python(self,data, params, connection):
        """
        Parse the wt=python results of a /select call. 
        """
        #parser = make_parser()
        #handler = ResponseContentHandler()
        #parser.setContentHandler(handler)
        return eval(data)
        #parser.parse(data)

        #if handler.stack[0].children: 
        #    response = handler.stack[0].children[0].final
        #    response._params = params
        #    response._connection = connection
        #    return response
        #else: 
        #    return None

    def smartQuery(self, query, fq='', fields='name,id', sort='',limit=0, start=0, blockSize=1000,callback=None):
        "Queries the server with blocks"
        docs = []
        if(limit<1):
            limit = 1000000
        if(limit<blockSize):
            blockSize=limit
        for startAt in range(start,limit,blockSize):
            sys.stderr.write(str(startAt)+ ' ')
            response = self.query(query, fields=fields,rows=blockSize, start=startAt, sort=sort, fq=fq)
            if len(response) == 0:
                break
            if(callback is not None):
                callback(response.results, startAt)
            else:
                for r in response:
                    docs.append(r)
        sys.stderr.write('\n')
        return docs

    def query(self, q, fields=None, highlight=None, 
              score=True, sort=None, use_experimental_parser=False, **params):

        """
        q is the query string.
        
        fields is an optional list of fields to include. It can 
        be either a string in the format that SOLR expects, or 
        a python list/tuple of field names.   Defaults to 
        all fields. ("*")

        score indicates whether "score" should be included
        in the field list.  Note that if you explicitly list
        "score" in your fields value, then score is 
        effectively ignored.  Defaults to true. 

        highlight indicates whether highlighting should be included.
        highligh can either be False, indicating "No" (the default), 
        True, incidating to highlight any fields included in "fields", 
        or a list of fields in the same format as "fields". 

        sort is a list of fields to sort by. See "fields" for
        formatting.

        Optional parameters can also be passed in.  Many SOLR
        parameters are in a dotted notation (e.g., hl.simple.post). 
        For such parameters, replace the dots with underscores when 
        calling this method. (e.g., hl_simple_post='</pre'>)

        Returns a Response instance.

        """

       # Clean up optional parameters to match SOLR spec.
        params = dict([(key.replace('_','.'), unicode(value)) 
                      for key, value in params.items()])

        if type(q) == type(u''):
            q = q.encode('utf-8')
        if q is not None: 
            params['q'] = q
        
        if fields: 
            if not isinstance(fields, basestring): 
                fields = ",".join(fields)
        if not fields: 
            fields = '*'

        if sort: 
            if not isinstance(sort, basestring): 
                sort = ",".join(sort)
            params['sort'] = sort

        if score and not 'score' in fields.replace(',',' ').split(): 
            fields += ',score'
            
        
        # BAW 4/5/09 -- this would add bandwidht & parse time to long queries
        params['echoParams'] = "none"

        params['fl'] = fields
        if(params.has_key('qt')):
            if(params['qt'] == "None"):
                del params['qt']
        if(params.has_key('fq')):
            if(params['fq'] == "None"):
                del params['fq']
                
        
        if highlight: 
            params['hl'] = 'on'
            if not isinstance(highlight, (bool, int, float)): 
                if not isinstance(highlight, basestring): 
                    highlight = ",".join(highlight)
                params['hl.fl'] = highlight

        params['version'] = self.response_version
        if(use_experimental_parser):
            params['wt']='python'
        else:
            params['wt'] = 'standard'

        request = urllib.urlencode(params, doseq=True)
        try:
            tic = time.time()
            rsp = self._post(self.path + '/select'+self.invariant, 
                              request, self.form_headers)
            # If we pass in rsp directly, instead of using rsp.read())
            # and creating a StringIO, then Persistence breaks with
            # an internal python error. 
            
            #xml = StringIO(self._cleanup(reallyUTF8(rsp.read())))
            tic=time.time()
            s1 = rsp.read()
            s2 = reallyUTF8(s1)
            s3 = self._cleanup(s2)

            if(use_experimental_parser):
                data = self.parse_query_response_python(s3,  params=params, connection=self)
            else:
                xml = StringIO(s3)
                data = self.parse_query_response(xml,  params=params, connection=self)                
            
        finally:
            if not self.persistent: 
                self.conn.close()

        return data


    def begin_batch(self): 
        """
        Denote the beginning of a batch update. 

        No update commands will be sent to the backend server
        until end_batch() is called. 
        
        Any update commands issued outside of a begin_batch()/
        end_batch() series will be immediately processed. 

        begin_batch/end_batch transactions can be nested. 
        The transaction will not be sent to the backend server
        until as many end_batch() calls have been made as 
        begin_batch()s. 
        """
        if not self.batch_cnt: 
            self.__batch_queue = []

        self.batch_cnt += 1

        return self.batch_cnt
        

    def end_batch(self, commit=False):
        """
        Denote the end of a batch update. 
        
        Sends any queued commands to the backend server. 

        If `commit` is True, then a <commit/> command is included
        at the end of the list of commands sent. 
        """

        batch_cnt = self.batch_cnt - 1
        if batch_cnt < 0: 
            raise SolrContentException(
                "end_batch called without a corresponding begin_batch")
       
        self.batch_cnt = batch_cnt
        if batch_cnt: 
            return False

        if commit: 
            self.__batch_queue.append('<commit/>')

        return self._update("".join(self.__batch_queue))


    def delete(self, id):
        """
        Delete a specific document by id. 
        """
        xstr = u'<delete><id>%s</id></delete>' % escape(unicode(id))
        return self._update(xstr)


    def delete_many(self, ids): 
        """
        Delete documents using a list of IDs. 
        """
        self.begin_batch()
        [self.delete(id) for id in ids]
        self.end_batch()


    def delete_query(self, query):
        """
        Delete all documents returned by a query.
        """
        xstr = u'<delete><query>%s</query></delete>' % escape(query)
        return self._update(xstr)

    def add(self, _commit=False, **fields):
        """
        Add a document to the SOLR server.  Document fields
        should be specified as arguments to this function

        Example: 
            connection.add(id="mydoc", author="Me")
        """

        lst = [u'<add>']
        self.__add(lst, fields)
        lst.append(u'</add>')
        if _commit: 
            lst.append(u'<commit/>')
        xstr = ''.join(lst)
        return self._update(xstr)


    def add_many(self, docs, _commit=False, addHandler="/update"):
        """
        Add several documents to the SOLR server.

        docs -- a list of dicts, where each dict is a document to add 
            to SOLR.
        """
        lst = [u'<add>']
        for doc in docs:
            self.__add(lst, doc)
        lst.append(u'</add>')
        if _commit: 
            lst.append(u'<commit/>')
        xstr = ''.join(lst)
        return self._update(xstr, addHandler=addHandler)
        
    


    def commit(self, wait_flush=True, wait_searcher=True, _optimize=False):
        """
        Issue a commit command to the SOLR server. 
        """
        if not wait_searcher:  #just handle deviations from the default
            if not wait_flush: 
                options = 'waitFlush="false" waitSearcher="false"'
            else: 
                options = 'waitSearcher="false"'
        else:
            options = ''
            
        if _optimize: 
            xstr = u'<optimize %s/>' % options
        else:
            xstr = u'<commit %s/>' % options
            
        return self._update(xstr)


    def optimize(self, wait_flush=True, wait_searcher=True, ): 
        """
        Issue an optimize command to the SOLR server.
        """
        self.commit(wait_flush, wait_searcher, _optimize=True)


    def handler_update(self, handler, xml):
        try:
            rsp = self._post(self.path+'/'+handler+self.invariant, 
                              xml, self.xmlheaders)
            data = rsp.read()
        finally:
            if not self.persistent: 
                self.conn.close()
        return data


    def handler_update_dict(self, handler, dict):
        try:
            rsp = self._post(self.path+'/'+handler+self.invariant, 
                              str(dict), self.jsonheaders)
            data = rsp.read()
        finally:
            if not self.persistent: 
                self.conn.close()
        return data


    def handler_query(self, handler, **params):
        """
        Issue a query against a SOLR server. 
        Return the raw result.  No pre-processing or 
        post-processing happends to either
        input parameters or responses
        """
        # Clean up optional parameters to match SOLR spec.
        params = dict([(key.replace('_','.'), unicode(value)) 
                       for key, value in params.items()])
        request = urllib.urlencode(params, doseq=True)
        try:
            rsp = self._post(self.path+'/'+handler+self.invariant, 
                              request, self.form_headers)
            data = rsp.read()
        finally:
            if not self.persistent: 
                self.conn.close()
        return data
        

    def handler_update_params(self, handler, **params):
        # Clean up optional parameters to match SOLR spec.
        params = dict([(key.replace('_','.'), unicode(value)) 
                       for key, value in params.items()])

        request = urllib.urlencode(params, doseq=True)
        try:
            rsp = self._post(self.path+'/'+handler+self.invariant, 
                              request, self.form_headers)
            data = rsp.read()
        finally:
            if not self.persistent: 
                self.conn.close()

        return data

    def raw_query(self, **params):
        """
        Issue a query against a SOLR server. 

        Return the raw result.  No pre-processing or 
        post-processing happends to either
        input parameters or responses
        """

        # Clean up optional parameters to match SOLR spec.
        params = dict([(key.replace('_','.'), unicode(value)) 
                       for key, value in params.items()])


        request = urllib.urlencode(params, doseq=True)

        try:
            rsp = self._post(self.path+'/select'+self.invariant, 
                              request, self.form_headers)
            data = rsp.read()
        finally:
            if not self.persistent: 
                self.conn.close()

        return data

    
    def _update(self, request, addHandler="/update"):

        # If we're in batching mode, just queue up the requests for later. 
        if self.batch_cnt: 
            self.__batch_queue.append(request)
            return 
        try:
            rsp = self._post(self.path + addHandler + self.invariant, request, self.xmlheaders)
            data = rsp.read()
        finally:
            if not self.persistent: 
                self.conn.close()
                
        #detect old-style error response (HTTP response code of
        #200 with a non-zero status.
        if data.startswith('<result status="') and not data.startswith('<result status="0"'):
            data = self.decoder(data)[0]
            parsed = parseString(data)
            status = parsed.documentElement.getAttribute('status')
            if status != 0:
                reason = parsed.documentElement.firstChild.nodeValue
                raise SolrHTTPException(rsp.status, reason)
        return data

    def __add(self, lst, fields):
        lst.append(u'<doc>')
        for field, value in fields.items():
            # Handle multi-valued fields if values
            # is passed in as a list/tuple
            if not isinstance(value, (list, tuple)): 
                values = [value]
            else: 
                values = value 

            for val in values: 
                # Do some basic data conversion
                if isinstance(val, datetime.datetime): 
                    val = utc_to_string(val)
                elif isinstance(val, bool): 
                    val = val and 'true' or 'false'

                try:
                    lst.append('<field name=%s>%s</field>' % (
                        (quoteattr(field), 
                        escape(unicode(val)))))
                except UnicodeDecodeError:
                    lst.append('<field name=%s> </field>' % (
                        (quoteattr(field))))
        lst.append('</doc>')


    def __repr__(self):
        return ('<SolrConnection (url=%s, '
                'persistent=%s, post_headers=%s, reconnects=%s)>') % (
            self.url, self.persistent, 
            self.xmlheaders, self.reconnects)


    def _reconnect(self):
        self.reconnects += 1
        self.conn.close()
        try:
            self.conn.connect()
        except socket.error:
            print "Error re-connecting. I'm going to wait one minute for solr to restart. If it doesn't come back there's a problem."
            time.sleep(60)
            self.conn.connect()
            print "It re-connected ok."


    def _cleanup(self, body):
        # clean up the body
        #section 2.2 of the XML spec. Three characters from the 0x00-0x1F block are allowed: 0x09, 0x0A, 0x0D.
        body = body.replace("\x00","")
        body = body.replace("\x01","")
        body = body.replace("\x02","")
        body = body.replace("\x03","")
        body = body.replace("\x04","")
        body = body.replace("\x05","")
        body = body.replace("\x06","")
        body = body.replace("\x07","")
        body = body.replace("\x08","")
        body = body.replace("\x0b","")
        body = body.replace("\x0c","")
        body = body.replace("\x0e","")
        body = body.replace("\x0f","")
        body = body.replace("\x10","")
        body = body.replace("\x11","")
        body = body.replace("\x12","")
        body = body.replace("\x13","")
        body = body.replace("\x14","")
        body = body.replace("\x15","")
        body = body.replace("\x16","")
        body = body.replace("\x17","")
        body = body.replace("\x18","")
        body = body.replace("\x19","")
        body = body.replace("\x1A","")
        body = body.replace("\x1B","")
        body = body.replace("\x1C","")
        body = body.replace("\x1D","")
        body = body.replace("\x1E","")
        body = body.replace("\x1F","")
        return body
        
    def _post(self, url, body, headers):
        body = self._cleanup(body)
        
        maxattempts = attempts = 4
        while attempts: 
            caught_exception = False
            try:
                self.conn.request('POST', url, body.encode('utf-8'), headers)
                return check_response_status(self.conn.getresponse())
            except (SolrHTTPException,
                    httplib.ImproperConnectionState,
                    httplib.BadStatusLine):
                    # We include BadStatusLine as they are spurious
                    # and may randomly happen on an otherwise fine 
                    # SOLR connection (though not often)
                time.sleep(1)
                caught_exception = True
            except socket.error:
                msg = "Connection error. %s tries left; retrying...\n" % attempts
                sys.stderr.write(msg)
                time.sleep(3 + 2 ** (maxattempts - attempts))
                caught_exception = True
            if caught_exception:    
                self._reconnect()
                attempts -= 1
                if not attempts:
                    raise

    
# ===================================================================
# Response objects
# ===================================================================
class Response(object):
    """
    A container class for a 

    A Response object will have the following properties: 
     
          header -- a dict containing any responseHeader values

          results -- a list of matching documents. Each list item will
              be a dict. 
    """
    def __init__(self, connection):
        # These are set in ResponseContentHandler.endElement()
        self.header = {}
        self.results = []
        
        # These are set by parse_query_response().
        # Used only if .next_batch()/previous_batch() is called
        self._connection = connection
        self._params = {}

    def __len__(self):
        """
        return the number of matching documents contained in this set.
        """
        return len(self.results)

    def __iter__(self):
        """
        Return an iterator of matching documents
        """
        return iter(self.results)

    def next_batch(self):
        """
        Load the next set of matches. 

        By default, SOLR returns 10 at a time. 
        """
        try:
            start = int(self.results.start)
        except AttributeError: 
            start = 0

        start += len(self.results)
        params = dict(self._params)
        params['start'] = start 
        q = params['q']
        del params['q']
        return self._connection.query(q, **params)

    def previous_batch(self):
        """
        Return the previous set of matches
        """
        try:
            start = int(self.results.start)
        except AttributeError:
            start = 0
  
        if not start: 
            return None

        rows = int(self.header.get('rows', len(self.results)))
        start = max(0, start - rows)
        params = dict(self._params)
        params['start'] = start
        params['rows'] = rows
        q = params['q']
        del params['q']
        return self._connection.query(q, **params)
 


# ===================================================================
# XML Parsing support
# ===================================================================  
#def parse_query_response(data, params, connection):
#    """
#    Parse the XML results of a /select call. 
#    """
#    parser = make_parser()
#    handler = ResponseContentHandler()
#    parser.setContentHandler(handler)
#    parser.parse(data)
#    if handler.stack[0].children: 
#        response = handler.stack[0].children[0].final
#        response._params = params
#        response._connection = connection
#        return response
#    else: 
#        return None


class ResponseContentHandler(ContentHandler): 
    """
    ContentHandler for the XML results of a /select call. 
    (Versions 2.2 (and possibly 2.1))
    """
    def __init__(self):
        self.stack = [Node(None, {})]
        self.in_tree = False
        
    def startElement(self, name, attrs): 
        if not self.in_tree:
            if name != 'response': 
                raise SolrContentException(
                    "Unknown XML response from server: <%s ..." % (
                        name))
            self.in_tree = True

        element = Node(name, attrs)
        
        # Keep track of new node
        self.stack.append(element)
        
        # Keep track of children
        self.stack[-2].children.append(element)


    def characters (self, ch):
        self.stack[-1].chars.append(ch)


    def endElement(self, name):
        node = self.stack.pop()

        name = node.name
        value = "".join(node.chars)
        
        if name == 'int': 
            node.final = int(value.strip())
            
        elif name == 'str': 
            node.final = value
            
        elif name == 'null': 
            node.final = None
            
        elif name == 'long': 
            node.final = long(value.strip())

        elif name == 'bool': 
            node.final = value.strip().lower().startswith('t')
            
        elif name == 'date': 
             node.final = utc_from_string(value.strip())
            
        elif name in ('float','double', 'status','QTime'):
            node.final = float(value.strip())
            
        elif name == 'response': 
            node.final = response = Response(self)
            for child in node.children: 
                name = child.attrs.get('name', child.name)
                if name == 'responseHeader': 
                    name = 'header'
                elif child.name == 'result': 
                    name = 'results'
                setattr(response, name, child.final)

        elif name in ('lst','doc'): 
            # Represent these with a dict
            node.final = dict(
                    [(cnode.attrs['name'], cnode.final) 
                        for cnode in node.children])

        elif name in ('arr',): 
            node.final = [cnode.final for cnode in node.children]

        elif name == 'result': 
            node.final = Results([cnode.final for cnode in node.children])


        elif name in ('responseHeader',): 
            node.final = dict([(cnode.name, cnode.final)
                        for cnode in node.children])

        else:
            raise SolrContentException("Unknown tag: %s" % name)

        for attr, val in node.attrs.items(): 
            if attr != 'name': 
                setattr(node.final, attr, val)


class Results(list): 
    """
    Convenience class containing <result> items
    """
    pass



class Node(object):
    """
    A temporary object used in XML processing. Not seen by end user.
    """
    def __init__(self, name, attrs): 
        """
        final will eventually be the "final" representation of 
        this node, whether an int, list, dict, etc.
        """
        self.chars = []
        self.name = name
        self.attrs = attrs
        self.final = None
        self.children = []
        
    def __repr__(self):
        return '<%s val="%s" %s>' % (
            self.name, 
            "".join(self.chars).strip(),
            ' '.join(['%s="%s"' % (attr, val) 
                            for attr, val in self.attrs.items()]))


# ===================================================================
# Misc utils
# ===================================================================
def check_response_status(response):
    if response.status != 200:
        ex = SolrHTTPException(response.status, response.reason)
        try:
            ex.body = response.read()
        except:
            pass
        raise ex
    return response


def stringToPython(f):
    """Convert a doc encoded as strings to native python types using EN's schema."""
    for key in f.keys():
        # Only convert f_ i_ etc type fields
        if(key[1]=='_'):
            # Make sure it's a list type (canonical docs get lists stripped)
            if(type(f[key]) != type([])):
                f[key] = [f[key]]
            if(key.startswith('f_')):
                f[key] = map(float,f[key])
            if(key.startswith('i_')):
                f[key] = map(int,f[key])
            if(key.startswith('l_')):
                f[key] = map(long,f[key])
            if(key.startswith('b_')):
                f[key] = map(str2bool,f[key])
            if(key.startswith('d_')):
                f[key] = map(utc_from_string,f[key])
            if(key.startswith('s_')):
                f[key] = f[key]
            if(key.startswith('v_')):
                f[key] = f[key]
            if(key.startswith('t_')):
                f[key] = f[key]
            if(key.startswith('n_')):
                f[key] = f[key]
        # Also convert indexed & modified, they special
        if(key == "indexed" or key=="modified"):
            f[key] = utc_from_string(f[key])
    return f


# -------------------------------------------------------------------
# Datetime extensions to parse/generate SOLR date formats
# -------------------------------------------------------------------
# A UTC class, for parsing SOLR's returned dates.
class UTC(datetime.tzinfo):
    """UTC timezone"""

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)

utc = UTC()

def utc_to_string(value):
    """
    Convert datetimes to the subset
    of ISO 8601 that SOLR expects...
    """
    try:
        value = value.astimezone(utc).isoformat()
    except ValueError:
        value = value.isoformat()
    if '+' in value:
        value = value.split('+')[0]
    value += 'Z'
    return value
    
if sys.version < '2.5.': 
    def utc_from_string(value):
        """
        Parse a string representing an ISO 8601 date.
        Note: this doesn't process the entire ISO 8601 standard, 
        onle the specific format SOLR promises to generate. 
        """
        try:
            if not value.endswith('Z') and value[10] == 'T': 
                raise ValueError(value)
            year = int(value[0:4])
            month = int(value[5:7])
            day = int(value[8:10])
            hour = int(value[11:13])
            minute = int(value[14:16])
            microseconds = int(float(value[17:-1]) * 1000000.0)
            second, microsecond = divmod(microseconds, 1000000)
            return datetime.datetime(year, month, day, hour, 
                minute, second, microsecond, utc)
        except ValueError: 
            raise ValueError ("'%s' is not a valid ISO 8601 SOLR date" % value)
else: 
    def utc_from_string(value): 
        """
        Parse a string representing an ISO 8601 date.
        Note: this doesn't process the entire ISO 8601 standard, 
        onle the specific format SOLR promises to generate. 
        """
        if(isinstance(value, datetime.datetime)):
            return value
        try:
            utc = datetime.datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
            try:
                utc = utc.replace(microsecond = 1000 * int(value[20:-1]))
            except ValueError:
                try:
                    utc = utc.replace(microsecond = int(value[20:-1]))                
                # I've seen a date like this: <date name="d_when">2008-12-03T02:07:52Z</date> , e.g. no microseconds.
                except ValueError:
                    utc = utc.replace(microsecond = 0)
            return utc
        except ValueError:
            return None
            
