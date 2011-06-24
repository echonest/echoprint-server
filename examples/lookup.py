#!/usr/bin/python

# This script takes an audio file and performs an echoprint lookup on it.
# Note that it does a direct lookup on an echoprint server that you will
# need to boot yourself. See the README.md document for more information
# on how to do this.
# To do a lookup against a public echoprint server, see the example in the
# echoprint-codegen project, which uses the Echo Nest developer API.

# Requirements: The echoprint-codegen binary from the echoprint-codegen project

import sys
import os
import subprocess
try:
    import json
except ImportError:
    import simplejson as json

sys.path.insert(0, "../API")
import fp

codegen_path = os.path.abspath("../../echoprint-codegen/echoprint-codegen")

def codegen(file, start=0, duration=30):
    proclist = [codegen_path, os.path.abspath(file), "%d" % start, "%d" % duration]
    p = subprocess.Popen(proclist, stdout=subprocess.PIPE)                      
    code = p.communicate()[0]                                                   
    return json.loads(code)

def lookup(file):
    codes = codegen(file)
    if len(codes) and "code" in codes[0]:
        decoded = fp.decode_code_string(codes[0]["code"])
        result = fp.best_match_for_query(decoded)
        print "Got result:", result
        if result.TRID:
            print "ID: %s" % (result.TRID)
            print "Artist: %s" % (result.metadata.get("artist"))
            print "Song: %s" % (result.metadata.get("track"))
        else:
            print "No match. This track may not be in the database yet."
    else:
        print "Couldn't decode", file
            

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >>sys.stderr, "Usage: %s <audio file>" % sys.argv[0]
        sys.exit(1)
    lookup(sys.argv[1])