#!/usr/bin/env python
# encoding: utf-8
"""
little_eval.py

Created by Brian Whitman on 2011-04-30.
Copyright (c) 2011 The Echo Nest. All rights reserved.
"""

import sys
import os
import logging
import fileinput
import subprocess
import json

sys.path.append('../API')
import fp

"""
    Simple version of EN bigeval for distribution
"""    

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_codegen_path = "../../echoprint-codegen/codegen.Darwin-i386"


def codegen(filename, start=10, duration=30):
    if not os.path.exists(_codegen_path):
        raise Exception("Codegen binary not found.")

    command = _codegen_path + " \"" + filename + "\" " 
    if start >= 0:
        command = command + str(start) + " "
    if duration >= 0:
        command = command + str(duration)
        
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (json_block, errs) = p.communicate()

    try:
        return json.loads(json_block)
    except ValueError:
        logger.debug("No JSON object came out of codegen: error was %s" % (errs))
        return None



def main():
    if not len(sys.argv)==3:
        print "usage: python little_eval.py database_list query_list"
        sys.exit()
        
    fp_codes = {}
    database_list = open(sys.argv[1]).read().split("\n")
    query_list = open(sys.argv[2]).read().split("\n")
    for file in database_list:
        print file
        if len(fp_codes.keys()) > 10:
            break
        # TODO - use threaded codegen
        j = codegen(file)
        if len(j):
            code_str = fp.decode_code_string(j[0]["code"])
            fp_codes[file] = code_str
    print "ingesting"
    fp.ingest(fp_codes, local=True)
    
    for file in query_list:
        print file
        j = codegen(file)
        if len(j):
            r = fp.best_match_for_query(j[0]["code"], local=True)
            print "For %s: %s %d (%s)" % (file, r.TRID, r.score, r.message())
        

if __name__ == '__main__':
    main()

