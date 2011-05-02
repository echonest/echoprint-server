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

def get_winners(query_code_string, response, elbow=8):
    actual = {}
    original = {}
    for x in response.results:
        actual[x["track_id"]] = fp.actual_matches(query_code_string, x["fp"], elbow=elbow)
        original[x["track_id"]] = int(x["score"])

    sorted_actual_scores = sorted(actual.iteritems(), key=lambda (k,v): (v,k), reverse=True)
    (actual_score_top_track_id, actual_score_top_score) = sorted_actual_scores[0]
    sorted_original_scores = sorted(original.iteritems(), key=lambda (k,v): (v,k), reverse=True)
    (original_score_top_track_id, original_score_top_score) = sorted_original_scores[0]
    for x in sorted_actual_scores:
        print "actual: %s %d" % (x[0], x[1])
    for x in sorted_original_scores:
        print "original: %s %d" % (x[0], x[1])
        
    return (actual_score_top_track_id, original_score_top_track_id)
    

def main():
    if not len(sys.argv)==4:
        print "usage: python little_eval.py [database_list | disk] query_list [limit]"
        sys.exit()
        
    fp_codes = {}
    limit = int(sys.argv[3])
    if sys.argv[1] == "disk":
        fp.local_load("disk.pkl")
    else:
        database_list = open(sys.argv[1]).read().split("\n")[0:limit]
        for line in database_list:
            (track_id, file) = line.split(" ### ")
            print track_id
            # TODO - use threaded codegen
            j = codegen(file, start=-1, duration=-1)
            if len(j):
                code_str = fp.decode_code_string(j[0]["code"])
                fp_codes[track_id] = code_str
        fp.ingest(fp_codes, local=True)
        fp.local_save("disk.pkl")

    counter = 0
    actual_win = 0
    original_win = 0
    bm_win = 0
    query_list = open(sys.argv[2]).read().split("\n")[0:limit]
    for line in query_list:
        (track_id, file) = line.split(" ### ")
        print track_id
        j = codegen(file)
        if len(j):
            counter+=1
            response = fp.query_fp(fp.decode_code_string(j[0]["code"]), rows=20, local=True, get_data=True)
            (winner_actual, winner_original) = get_winners(fp.decode_code_string(j[0]["code"]), response, elbow=8)
            response = fp.best_match_for_query(j[0]["code"], local=True)
            if(response.TRID == track_id):
                bm_win+=1
            if(winner_actual == track_id):
                actual_win+=1
            if(winner_original == track_id):
                original_win+=1
    print "%d / %d actual (%2.2f%%) %d / %d original (%2.2f%%) %d / %d bm (%2.2f%%)" % (actual_win, counter, (float(actual_win)/float(counter))*100.0, \
        original_win, counter, (float(original_win)/float(counter))*100.0, \
        bm_win, counter, (float(bm_win)/float(counter))*100.0)
    
if __name__ == '__main__':
    main()

