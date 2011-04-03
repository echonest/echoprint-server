#!/usr/bin/env python
# encoding: utf-8
"""
api.py

Created by Brian Whitman on 2010-06-16.
Copyright (c) 2010 The Echo Nest Corporation. All rights reserved.
"""
from __future__ import with_statement

import web
import fp
import re

try:
    import json
except ImportError:
    import simplejson as json


# Very simple web facing API for FP dist

urls = (
    '/query', 'query',
    '/query?(.*)', 'query',
    '/ingest', 'ingest',
)


class ingest:
    def POST(self):
        stuff = web.input(track_id="default",fp_code="")
        if stuff.track_id == "default":
            track_id = fp.new_track_id()
        else:
            track_id = stuff.track_id
        
        # First see if this is a compressed code
        if re.match('[A-Za-z\/\+\_\-]', stuff.fp_code) is not None:
           code_string = fp.decode_code_string(stuff.fp_code)
           if code_string is None:
               return json.dumps({"track_id":track_id, "ok":False, "error":"cannot decode code string %s" % stuff.fp_code})

        fp.ingest({track_id:code_string}, do_commit=True, local=False)

        return json.dumps({"track_id":track_id, "ok":True})
        
    
class query:
    def POST(self):
        stuff = web.input(fp_code="")
        return self.GET(stuff.fp_code)
        
    def GET(self):
        stuff = web.input(fp_code="")
        response = fp.best_match_for_query(stuff.fp_code)
        return json.dumps({"ok":True, "query":stuff.fp_code, "message":response.message(), "match":response.match(), "score":response.score, \
                        "qtime":response.qtime, "track_id":response.TRID, "total_time":response.total_time})


application = web.application(urls, globals())#.wsgifunc()
        
if __name__ == "__main__":
    application.run()

