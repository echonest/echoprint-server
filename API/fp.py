#!/usr/bin/env python
# encoding: utf-8
"""
fp.py

Created by Brian Whitman on 2010-06-16.
Copyright (c) 2010 The Echo Nest Corporation. All rights reserved.
"""
from __future__ import with_statement
import logging
import solr
import pickle
from collections import defaultdict
import zlib, base64, re, time, random, string, math

try:
    import json
except ImportError:
    import simplejson as json

_fp_solr = solr.SolrConnectionPool("http://localhost:8502/solr/fp")
_hexpoch = int(time.time() * 1000)
logger = logging.getLogger(__name__)


class Response(object):
    # Response codes
    NOT_ENOUGH_CODE, CANNOT_DECODE, SINGLE_BAD_MATCH, SINGLE_GOOD_MATCH, NO_RESULTS, MULTIPLE_GOOD_MATCH_HISTOGRAM_INCREASED, \
        MULTIPLE_GOOD_MATCH_HISTOGRAM_DECREASED, MULTIPLE_BAD_HISTOGRAM_MATCH, MULTIPLE_GOOD_MATCH = range(9)

    def __init__(self, code, TRID=None, score=0, qtime=0, tic=0):
        self.code = code
        self.qtime = qtime
        self.TRID = TRID
        self.score = score
        self.total_time = int(time.time()*1000) - tic

    def __len__(self):
        if self.TRID is not None:
            return 1
        else:
            return 0
        
    def message(self):
        if self.code == self.NOT_ENOUGH_CODE:
            return "query code length is too small"
        if self.code == self.CANNOT_DECODE:
            return "could not decode query code"
        if self.code == self.SINGLE_BAD_MATCH or self.code == self.NO_RESULTS or self.code == self.MULTIPLE_BAD_HISTOGRAM_MATCH:
            return "no results found (type %d)" % (self.code)
        return "OK (match type %d)" % (self.code)
    
    def match(self):
        return self.TRID is not None
     

def inflate_code_string(s):
    """ Takes an uncompressed code string consisting of 0-padded fixed-width
        sorted hex and converts it to the standard code string."""
    n = int(len(s) / 10.0) # 5 hex bytes for hash, 5 hex bytes for time (40 bits)

    def pairs(l, n=2):
        """Non-overlapping [1,2,3,4] -> [(1,2), (3,4)]"""
        # return zip(*[[v for i,v in enumerate(l) if i % n == j] for j in range(n)])
        end = n
        res = []
        while end <= len(l):
            start = end - n
            res.append(tuple(l[start:end]))
            end += n
        return res

    # Parse out n groups of 5 timestamps in hex; then n groups of 8 hash codes in hex.
    end_timestamps = n*5
    times = [int(''.join(t), 16) for t in pairs(s[:end_timestamps], 5)]
    codes = [int(''.join(t), 16) for t in pairs(s[end_timestamps:], 5)]

    assert(len(times) == len(codes)) # these should match up!
    return ' '.join('%d %d' % (c, t) for c,t in zip(codes, times))
    
        


def decode_code_string(compressed_code_string):
    compressed_code_string = compressed_code_string.encode('utf8')
    # do the zlib/base64 stuff
    try:
        # this will decode both URL safe b64 and non-url-safe
        actual_code = zlib.decompress(base64.urlsafe_b64decode(compressed_code_string))
    except (zlib.error, TypeError):
        logger.warn("Could not decode base64 zlib string %s" % (compressed_code_string))
        import traceback; logger.warn(traceback.format_exc())
        return None
    # If it is a deflated code, expand it from hex
    if ' ' not in actual_code:
        actual_code = inflate_code_string(actual_code)
    return actual_code

def best_match_for_query(code_string, elbow=10, local=False):
    # DEC strings come in as unicode so we have to force them to ASCII
    code_string = code_string.encode("utf8")
    tic = int(time.time()*1000)

    # First see if this is a compressed code
    if re.match('[A-Za-z\/\+\_\-]', code_string) is not None:
        code_string = decode_code_string(code_string)
        if code_string is None:
            return Response(Response.CANNOT_DECODE, tic=tic)
    
    code_len = len(code_string.split(" ")) / 2
    if code_len < elbow:
        logger.warn("Query code length (%d) is less than elbow (%d)" % (code_len, elbow))
        return Response(Response.NOT_ENOUGH_CODE, tic=tic)

    # Query the FP flat directly.
    response = query_fp(code_string, rows=30, local=local, get_data=True)
    logger.debug("solr qtime is %d" % (response.header["QTime"]))
    
    if len(response.results) == 0:
        return Response(Response.NO_RESULTS, qtime=response.header["QTime"], tic=tic)

    # If we just had one result, make sure that it is close enough. We rarely if ever have a single match so this is not helpful (and probably doesn't work well.)
    top_match_score = int(response.results[0]["score"])
    if len(response.results) == 1:
        if code_len - top_match_score < elbow:
            return Response(Response.SINGLE_GOOD_MATCH, TRID=response.results[0]["track_id"], score=top_match_score, qtime=response.header["QTime"], tic=tic)
        else:
            return Response(Response.SINGLE_BAD_MATCH, qtime=response.header["QTime"], tic=tic)

    # Get the actual score for all responses
    original_scores = {}
    actual_scores = {}
    
    # For each result compute the "actual score" (based on the histogram matching)
    for r in response.results:
        original_scores[r["track_id"]] = int(r["score"])
        actual_scores[r["track_id"]] = actual_matches(code_string, r["fp"], elbow = elbow)
    
    # Sort the actual scores
    sorted_actual_scores = sorted(actual_scores.iteritems(), key=lambda (k,v): (v,k), reverse=True)

    # Find if the highest is an outlier (then it's a match) - >1 SD higher than the mean
    scores = [s for (tr, s) in sorted_actual_scores]
    
    sum_scores = sum(scores)
    mean = float(sum_scores) / float(len(scores))
    variance = 0.0
    for s in scores:
        variance = variance + (s - mean) * (s - mean)
    variance = variance / (len(scores))
    std = math.sqrt(variance)
    
    score = None
    top_track = None

    if len(sorted_actual_scores) > 0:
        top_score = sorted_actual_scores[0][1]
        if top_score > mean + std:
            # We have a score that looks like it's pretty high. Check that the number of ordered codes
            # is close to the real number of codes (over 25%)
            trid = sorted_actual_scores[0][0]
            oscore = float(original_scores[trid])
            if float(top_score) / oscore >= 0.25:
                top_track = trid
                score = top_score
                logger.debug("top score %d, mean %d, std %d" % (top_score, mean, std))

    if top_track is not None:
        return Response(Response.MULTIPLE_GOOD_MATCH, TRID=top_track, score=int(score), qtime=response.header["QTime"], tic=tic)
    else:
        return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime=response.header["QTime"], tic=tic)

def actual_matches(code_string_query, code_string_match, slop = 1, elbow = 10):
    code_query = code_string_query.split(" ")
    code_match = code_string_match.split(" ")
    if (len(code_match) < (elbow*2)):
        return 0

    slop = 32 * slop
    time_diffs = {}

    #
    # Invert the query codes
    query_codes = {}
    for (qcode, qtime) in zip(code_query[::2], code_query[1::2]):
        qtime = int(qtime) / slop
        if qcode in query_codes:
            query_codes[qcode].append(qtime)
        else:
            query_codes[qcode] = [qtime]

    #
    # Walk the document codes, handling those that occur in the query
    match_counter = 1
    for match_code in code_match[::2]:
        if match_code in query_codes:
            match_code_time = int(code_match[match_counter])/slop
            min_dist = 32767
            for qtime in query_codes[match_code]:
                dist = abs(match_code_time - qtime)
                if dist < min_dist:
                    min_dist = dist
            if min_dist < 32767:
                if time_diffs.has_key(min_dist):
                    time_diffs[min_dist] += 1
                else:
                    time_diffs[min_dist] = 1
        match_counter += 2

    # sort the histogram, pick the top 2 and return that as your actual score
    actual_match_list = sorted(time_diffs.iteritems(), key=lambda (k,v): (v,k), reverse=True)

    if(len(actual_match_list)>1):
        return actual_match_list[0][1] + actual_match_list[1][1]
    if(len(actual_match_list)>0):
        return actual_match_list[0][1]
    return 0        

"""
    fp can query the live production flat or the alt flat, or it can query and ingest in memory.
    the following few functions are to support local query and ingest that ape the response of the live server
    This is useful for small collections and testing, deduplicating, etc, without having to boot a server.
    The results should be equivalent but i need to run tests. 
    
    NB: delete is not supported locally yet
    
"""
_fake_solr = {"index": {}, "store": {}}

class FakeSolrResponse(object):
    def __init__(self, results):
        self.header = {'QTime': 0}
        self.results = []
        for r in results:
            # If the result list has more than 2 elements we've asked for data as well
            if len(r) > 2:
                self.results.append({"score":r[1], "track_id":r[0], "fp":r[2]})
            else:
                self.results.append({"score":r[1], "track_id":r[0]})
    
def local_load(filename):
    global _fake_solr
    print "Loading from " + filename
    disk = open(filename,"rb")
    _fake_solr = pickle.load(disk)
    disk.close()
    print "Done"
    
def local_save(filename):
    print "Saving to " + filename
    disk = open(filename,"wb")
    pickle.dump(_fake_solr,disk)
    disk.close()
    print "Done"
    
def local_ingest(code_string_dict):
    for track in code_string_dict.keys():
        _fake_solr["store"][track] = code_string_dict[track]
        keys = set(code_string_dict[track].split(" ")[0::2]) # just one code indexed
        for k in keys:
            _fake_solr["index"].setdefault(k,[]).append(track)

def local_delete(tracks):
    for track in tracks:
        codes = set(_fake_solr["store"][track].split(" ")[0::2])
        del _fake_solr["store"][track]
        for code in codes:
            _fake_solr["index"][code].remove(track)
            if len(_fake_solr["index"][code]) == 0:
                del _fake_solr["index"][code]

def local_dump():
    print "Stored tracks:"
    for t in _fake_solr["store"].keys():
        print t
    print "Keys:"
    for k in _fake_solr["index"].keys():
        print "%s -> %s" % (k, ", ".join(_fake_solr["index"][k]))

def local_query_fp(code_string,rows=10,get_data=False):
    keys = code_string.split(" ")[0::2]
    track_hist = []
    for k in keys:
        track_hist += _fake_solr["index"].get(k, [])
    top_matches = defaultdict(int)
    for track in track_hist:
        top_matches[track] += 1
    if not get_data:
        # Make a list of lists that have track_id, score
        return FakeSolrResponse(sorted(top_matches.iteritems(), key=lambda (k,v): (v,k), reverse=True)[0:rows])
    else:
        # Make a list of lists that have track_id, score, then fp
        lol = sorted(top_matches.iteritems(), key=lambda (k,v): (v,k), reverse=True)[0:rows]
        lol = map(list, lol)
        for x in lol:
            x.append(_fake_solr["store"][x[0]])
        return FakeSolrResponse(lol)

def local_fp_code_for_track_id(track_id):
    return _fake_solr["store"][track_id]
    
"""
    and these are the server-hosted versions of query, ingest and delete 
"""

def delete(track_ids, do_commit=True, local=False):
    # delete one or more track_ids from the fp flat. 
    if not isinstance(track_ids, list):
        track_ids = [track_ids]

    # delete a code from FP flat
    if local:
        print "not implemented yet"
        return
    with solr.pooled_connection(_fp_solr) as host:
        host.delete_many(track_ids)
    if do_commit:
        commit()

    
def ingest(fingerprint_list, do_commit=True, local=False):
    """ Ingest some fingerprints into the fingerprint database.
        The fingerprints should be of the form {"track_id": id, "fp": fp, "artist": artist, "release": release, "track": track, "length": length}
        or a list of the same.
        artist, release and track are not required but highly recommended.
        length is the length of the track being ingested in milliseconds
        if track_id is empty, one will be generated.
    """
    if not isinstance(fingerprint_list, list):
        fingerprint_list = [fingerprint_list]

    if local:
        return local_ingest(fingerprint_list)

    docs = []
    for fprint in fingerprint_list:
        docs.append(fprint)

    with solr.pooled_connection(_fp_solr) as host:
        host.add_many(docs)

    if do_commit:
        commit()

def commit(local=False):
    with solr.pooled_connection(_fp_solr) as host:
        host.commit()

def query_fp(code_string, rows=15, local=False, get_data=False):
    if local:
        return local_query_fp(code_string, rows, get_data=get_data)
    
    try:
        # query the fp flat
        if get_data:
            fields = "track_id,artist,release,track,length"
        else:
            fields = "track_id"
        with solr.pooled_connection(_fp_solr) as host:
            resp = host.query(code_string, qt="/hashq", rows=rows, fields=fields)
        return resp
    except solr.SolrException:
        return None

def fp_code_for_track_id(track_id, local=False):
    if local:
        return local_fp_code_for_track_id(track_id)
    # Get it from solr
    with solr.pooled_connection(_fp_solr) as host:
        resp = host.query("track_id:"+track_id, rows=1, fields="fp")
    if len(resp.results):
        return resp.results[0]["fp"]
    else:
        return None

def new_track_id():
    rand5 = ''.join(random.choice(string.letters) for x in xrange(5)).upper()
    global _hexpoch
    _hexpoch += 1
    hexpoch = str(hex(_hexpoch))[2:].upper()
    ## On 32-bit machines, the number of milliseconds since 1970 is 
    ## a longint. On 64-bit it is not.
    hexpoch = hexpoch.rstrip('L')
    return "TR" + rand5 + hexpoch


    

