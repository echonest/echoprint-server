#!/usr/bin/env python
# encoding: utf-8
"""
bigeval.py

Created by Brian Whitman on 2010-07-02.
Copyright (c) 2010 The Echo Nest Corporation. All rights reserved.
"""

import getopt
import sys
import os
import time
import socket
try:
    import json
except ImportError:
    import simplejson as json
import pyechonest.config as config
import pyechonest.song as song
import random
import math
sys.path.insert(0, "../API")
import fp

config.CODEGEN_BINARY_OVERRIDE = os.path.abspath("../../echoprint-codegen/echoprint-codegen")

# load the file list.
bige = os.path.join(os.path.dirname(__file__), 'bigeval.json')
if not os.path.exists(bige):
    print >>sys.stderr, "Cannot find bigeval.json. did you run fastingest with the -b flag?"
    sys.exit(1)
_local_bigeval = json.load(open(bige,'r'))

new_mus = os.path.join(os.path.dirname(__file__), 'new_music')
if os.path.exists(new_mus):
    _new_music_files = open(new_mus,'r').read().split('\n')
else:
    _new_music_files = []

_new_queries = float(len(_new_music_files))
_old_queries = float(len(_local_bigeval.keys()))
_total_queries = _new_queries + _old_queries

def munge(file, start=-1, duration=-1, bitrate=128, volume=-1, downsample_to_22 = False, speed_up = False, slow_down = False, lowpass_freq = -1, encode_to="mp3", channels=2):
    """
        duration: seconds of source file
        start: seconds to start reading
        volume 0-100, percentage of original
        bitrate: 8, 16, 32, 64, 96, 128, 160, 192, 256, 320
        downsample_to_22: True or False
        speed_up: True or False
        slow_down: True or False
        lowpass_freq: -1 (don't), 22050, 16000, 12000, 8000, 4000
        encode_to: mp3 (uses LAME), m4a (uses ffmpeg), wav, ogg (uses ffmpeg) (ogg does not work yet)
        channels: 1 or 2
    """
    
    # Get a tempfile to munge to
    me = "/tmp/temp_"+str(random.randint(1,32768))+".wav"

    # Decode the file (NB: use a better decoder? afconvert / quicktime?)
    cmd = "mpg123 -q -w " + me
    what = {}
    if(start > 0):
        what.update({"start":start})
        cmd = cmd + " -k " + str(int(start*44100 / 1152))
    if(duration > 0):
        what.update({"duration":duration})
        cmd = cmd + " -n " + str(int(duration*44100 / 1152))
    if(volume > 0):
        what.update({"volume":volume})
        cmd = cmd + " -f " + str(int( (float(volume)/100.0) * 32768.0 ))
    if(downsample_to_22):
        what.update({"downsample_22":True})
        cmd = cmd + " -2 "
    if(channels<2):
        what.update({"mono":True})
        cmd = cmd + " -0 "
    # speed_up and slow_down totally both break the FP. This is OK and expected but we should test it anyway.
    if speed_up and slow_down:
        pass # uh
    else:
        if(speed_up):
            what.update({"skip_every_other_frame":True})
            cmd = cmd + " -d 2 "
        if(slow_down):
            what.update({"play_every_frame_2x":True})
            cmd = cmd + " -h 2 "
    cmd = cmd + " \"" + file + "\""
    os.system(cmd)

    if not os.path.exists(me):
        print >> sys.stderr, "munge result not there"
        return (None, what)
        
    file_size = os.path.getsize(me)
    if(file_size<100000):
        print >> sys.stderr, "munge too small"
        os.remove(me)
        return (None, what)

    roughly_in_seconds = file_size / 176000
    what.update({"actual_file_length":roughly_in_seconds})

    if encode_to == "mp3":
        what.update({"encoder":"lame","encode_to":"mp3"})
        cmd = "lame --silent -cbr -b " + str(bitrate) + " "
        if(lowpass_freq > 0):
            what.update({"lowpass":lowpass_freq})
            cmd = cmd + " --lowpass " + str(lowpass_freq) + " "
        what.update({"bitrate":bitrate})
        cmd = cmd + me + " " + me + ".mp3"
    
    if encode_to == "m4a":
        what.update({"encoder":"ffmpeg","encode_to":"m4a"})
        cmd = "ffmpeg -i " + me + " -ab " + str(bitrate) + "k " + me + ".m4a 2>/dev/null"

    # NB ogg does not work on my copy of ffmpeg...
    if encode_to == "ogg":
        what.update({"encoder":"ffmpeg","encode_to":"ogg"})
        cmd = "ffmpeg -i " + me + " -ab " + str(bitrate) + "k " + me + ".ogg 2>/dev/null"

    if encode_to == "wav":
        what.update({"encoder":"none","encode_to":"wav"})
        return (me, what)
        
    os.system(cmd)
    try:
        os.remove(me)
    except OSError:
        return (None, what)
    return (me+"."+encode_to, what)

def prf(numbers_dict):
    # compute precision, recall, F, etc
    precision = recall = f = true_negative_rate = accuracy = 0
    tp = float(numbers_dict["tp"])
    tn = float(numbers_dict["tn"])
    fp = float(numbers_dict["fp-a"]) + float(numbers_dict["fp-b"])
    fn = float(numbers_dict["fn"])
    if tp or fp:
        precision = tp / (tp + fp)
    if fn or tp:
        recall = tp / (tp + fn)
    if precision or recall:
        f = 2.0 * (precision * recall)/(precision + recall)
    if tn or fp:
        true_negative_rate = tn / (tn + fp)
    if tp or tn or fp or fn:
        accuracy = (tp+tn) / (tp + tn + fp + fn)
    print "P %2.4f R %2.4f F %2.4f TNR %2.4f Acc %2.4f %s" % (precision, recall, f, true_negative_rate, accuracy, str(numbers_dict))
    return {"precision":precision, "recall":recall, "f":f, "true_negative_rate":true_negative_rate, "accuracy":accuracy}

def dpwe(numbers_dict):
    # compute dan's measures.. probability of error, false accept rate, correct accept rate, false reject rate
    car = far = frr = pr = 0
    r1 = float(numbers_dict["tp"])
    r2 = float(numbers_dict["fp-a"])
    r3 = float(numbers_dict["fn"])
    r4 = float(numbers_dict["fp-b"])
    r5 = float(numbers_dict["tn"])
    if r1 or r2 or r3:
        car = r1 / (r1 + r2 + r3)
    if r4 or r5:
        far = r4 / (r4 + r5)
    if r1 or r2 or r3:
        frr = (r2 + r3) / (r1 + r2 + r3)
    # probability of error
    pr = ((_old_queries / _total_queries) * frr) + ((_new_queries / _total_queries) * far)    
    print "PR %2.4f CAR %2.4f FAR %2.4f FRR %2.4f %s" % (pr, car, far, frr, str(numbers_dict))
    stats = {}
    stats.update(numbers_dict)    
    dpwe_nums = {"pr":pr, "car": car, "far":far, "frr":frr}
    stats.update(dpwe_nums)
    return dpwe_nums

def test_file(filename, local = False, expect_match=True, original_TRID=None, remove_file = True):
    """
        Test a single file. This will return a code like tn, tp, fp, err, etc
    """
    matching_TRID = None
    if filename is None:
        return "err-munge" # most likely a munge error (file too short, unreadable, etc)
    try:
        # don't pass start and duration to codegen, assume the munging takes care of codelength
        if not local:
            query_obj = song.util.codegen(filename, start=-1, duration=-1)
            s = fp.best_match_for_query(query_obj[0]["code"])
            if s.TRID is not None:
                matching_TRID = s.TRID
        else:
            query_obj = song.util.codegen(filename, start=-1, duration=-1)
            s = fp.best_match_for_query(query_obj[0]["code"], local=local)
            if s.TRID is not None:
                matching_TRID = s.TRID

    except IOError:
        print "TIMEOUT from API server"
        return "err-api"
    except TypeError: # codegen returned none
        return "err-codegen"
    if remove_file:
        if os.path.exists(filename):
            os.remove(filename)
        else:
            return "err-munge"

    # if is not None there was a response
    if s is not None:
        # there was a match
        if len(s) > 0:
            # if we're expecting a match, check that it's the right one
            if expect_match:
                if original_TRID == matching_TRID:
                    # we expected a match, it's the right one. TP
                    return "tp"
                else:
                    # we expected a match but it's the wrong one. FP-a
                    return "fp-a"
            else:
                # we did not expect a match. FP-b
                return "fp-b"
        else:
            # there was no match from the API
            if expect_match:
                # we expected a match. FN
                return "fn"
            else:
                # we did not expect a match. TN
                return "tn"
    else:
        # s is None, that means API error-- almost definitely codegen returned nothing.
        return "err-codegen"

def test_single(filename, local=False, **munge_kwargs):
    """
        Perform a test on a single file. Prints more diagnostic information than usual.
    """
    (new_file, what) = munge(filename, **munge_kwargs)
    query_obj = song.util.codegen(new_file, start=-1, duration=-1)
    if not local:
        s = fp.best_match_for_query(query_obj[0]["code"])
        if s.TRID is not None:
            metad = _local_bigeval[s.TRID]
            song_metadata = {"artist": metad["artist"], "release": metad["release"], "title": metad["title"]}
            print str(song_metadata)
        else:
            print "No match"
    else:
        s = fp.best_match_for_query(query_obj[0]["code"],local=local)
        if s.TRID is not None:
            metad = _local_bigeval[s.TRID]
            song_metadata = {"artist": metad["artist"], "release": metad["release"], "title": metad["title"]}
            print str(song_metadata)
        else:
            print "No match"
    
    decoded = fp.decode_code_string(query_obj[0]["code"])
    print str(len(decoded.split(" "))/2) + " codes in original"
    response = fp.query_fp(decoded, local=local, rows=15)
    if response is not None:
        print "From FP flat:"
        tracks = {}
        scores = {}
        for r in response.results:
            trid = r["track_id"].split("-")[0]
            metad = _local_bigeval[trid]
            m = {"artist": metad["artist"], "release": metad["release"], "title": metad["title"]}
            if m is not None:
                actual_match = fp.actual_matches(decoded, fp.fp_code_for_track_id(r["track_id"], local=local))
                tracks[r["track_id"]] = (m, r["score"], actual_match)
                scores[r["track_id"]] = actual_match
            else:
                print "problem getting metadata for " + r["track_id"]
        sorted_scores = sorted(scores.iteritems(), key=lambda (k,v): (v,k), reverse=True)
        for (trackid, score) in sorted_scores:
            (m, score, actual_match) = tracks[trackid]
            print trackid + " (" + str(int(score)) + ", " + str(actual_match) +") - " + m["artist"] + " - " + m["title"]
    else:
        print "response from fp flat was None -- decoded code was " + str(decoded)
    os.remove(new_file)

def test(how_many, diag=False, local=False, **munge_kwargs):
    """
        Perform a test. Takes both new files and old files, munges them, tests the FP with them, computes various measures.
        how_many: how many files to process
        munge_kwargs: you can pass any munge parameter, like duration=30 or volume=50
    """
    results = {"fp-a":0, "fp-b":0, "fn":0, "tp":0, "tn":0, "err-codegen":0, "err-munge":0, "err-api":0, "err-data":0, "total":0}
    
    docs_to_test = _local_bigeval.keys() + _new_music_files
    random.shuffle(docs_to_test)

    for x in docs_to_test:
        if results["total"] == how_many:
            return results

        if x.startswith("TR"): # this is a existing TRID
            original_TRID = x
            filename = _local_bigeval[x]["filename"]
            (new_file, what) = munge(filename, **munge_kwargs)
            result = test_file(new_file, expect_match = True, original_TRID = x, local=local)
                
        else: # this is a new file
            filename = x
            original_TRID = None
            (new_file, what) = munge(filename, **munge_kwargs)
            result = test_file(new_file, expect_match = False, local=local)
        
        if result is not "tp" and result is not "tn":
            print "BAD ### " + filename + " ### " + result + " ### " + str(original_TRID) + " ### " + str(what)
            if diag and not result.startswith("err"):
                test_single(filename, local=local, **munge_kwargs)
            
        results[result] += 1
        results["total"] += 1

        if results["total"] % 10 == 0:
            dpwe(results)

    return results
    
def usage():
    print "FP bigeval"
    print "\t-1\t--single  \tSingle mode, given a filename show an answer"
    print "\t-c\t--count   \tHow many files to process (required if not --single)"
    print "\t-s\t--start   \tIn seconds, when to start decoding (0)"
    print "\t-d\t--duration\tIn seconds, how long to decode, -1 is unchanged (30)"
    print "\t-b\t--bitrate \tIn kbps, encoded bitrate. only for mp3, m4a, ogg. (128)"
    print "\t-v\t--volume  \tIn %, volume of original. -1 for no adjustment. (-1)"
    print "\t-l\t--lowpass \tIn Hz, lowpass filter. -1 for no adjustment. (-1)"
    print "\t-e\t--encoder \tEncoder to use. wav, m4a, ogg, mp3. (wav)"
    print "\t-L\t--local   \tUse local data, not solr, with given JSON block (None)"
    print "\t-p\t--print   \tDuring bulk mode, show diagnostic on matches for types fp, fn, fp-a, fp-b (off)"
    print "\t-m\t--mono    \tMono decoder. (off)"
    print "\t-2\t--22kHz   \tDownsample to 22kHz (off)"
    print "\t-B\t--binary  \tPath to the binary to use for this test (codegen on path)"
    print "\t-h\t--help    \tThis help message."
    
def main(argv):
    global _local_bigeval
    
    single = None
    how_many = None
    start = 0
    duration = 30
    bitrate = 128
    volume = -1
    lowpass = -1
    encoder = "wav"
    local = None
    diag = False
    channels = 2
    downsample = False
    
    try:
        opts, args = getopt.getopt(argv, "1:c:s:d:b:v:l:L:e:B:pm2h", ["single=","count=","start=","duration=","bitrate=","volume=","lowpass=","encoder=","print","mono","local=","22kHz","help"])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    
    for opt,arg in opts:
        if opt in ("-1","--single"):
            single = arg
        if opt in ("-c","--count"):
            how_many = int(arg)
        if opt in ("-s","--start"):
            start = int(arg)
        if opt in ("-d","--duration"):
            duration = int(arg)
        if opt in ("-b","--bitrate"):
            bitrate = int(arg)
        if opt in ("-v","--volume"):
            volume = int(arg)
        if opt in ("-l","--lowpass"):
            lowpass = int(arg)
        if opt in ("-e","--encoder"):
            encoder = arg
        if opt in ("-L","--local"):
            local = arg
        if opt in ("-p","--print"):
            diag = True
        if opt in ("-m","--mono"):
            channels = 1
        if opt in ("-2","--22kHz"):
            downsample = True
        if opt in ("-B","--binary"):
            if not os.path.exists(arg):
                print "Binary %s not found. Exiting." % arg
                sys.exit(2)
            config.CODEGEN_BINARY_OVERRIDE = arg
        if opt in ("-h","--help"):
            usage()
    
    if (single is None) and (how_many is None):
        usage()
        sys.exit(2)
    if (single is not None) and (diag is not False):
        usage()
        sys.exit(2)
    
    if local is None:
        local = False
    else:
        # ingest
        codes = json.load(open(local,'r'))
        _reversed_bigeval = dict( (_local_bigeval[k], k) for k in _local_bigeval)
        code_dict = {}
        tids = {}
        for c in codes:
            fn = c["metadata"]["filename"]
            tid = _reversed_bigeval.get(fn, None)
            tids[tid] = True
            if tid is not None:
                if c.has_key("code"):
                    if len(c["code"]) > 4:
                        code_dict[tid] = fp.decode_code_string(c["code"])
                        
        fp.ingest(code_dict, local=True)
        lp = {}
        for r in _local_bigeval.keys():
            if tids.has_key(r):
                lp[r] = _local_bigeval[r]
        _local_bigeval = lp
        local = True
        
    if single is not None:
        test_single(single, local=local, start=start, duration = duration, bitrate = bitrate, volume = volume, lowpass_freq = lowpass, encode_to=encoder, downsample_to_22 = downsample, channels = channels)
    else:
        results = test(how_many, diag = diag, local=local, start=start, duration = duration, bitrate = bitrate, volume = volume, lowpass_freq = lowpass, encode_to=encoder, downsample_to_22 = downsample, channels = channels)
        prf(results)
        dpwe(results)

if __name__ == '__main__':
    main(sys.argv[1:])

