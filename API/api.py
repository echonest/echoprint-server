#!/usr/bin/env python
# encoding: utf-8
"""
run.py

Created by Brian Whitman
"""
from __future__ import with_statement

import web

try:
    import json
except ImportError:
    import simplejson as json


# Very simple web facing API for FP dist

urls = (
    '/query', 'query',
    '/ingest', 'ingest',
)


class ingest:
    def POST(self):
        pass
    
class query:
    def GET(self):
        stuff = web.input(code="",seconds="",filename="",results=15, elbow=10)
        query = {}
        # -- 

application = web.application(urls, globals()).wsgifunc()
        
#if __name__ == "__main__":
#    application.run()

