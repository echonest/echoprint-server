#!/usr/bin/python

# Copyright The Echo Nest 2011

# Ingest a dump from a master server.

import sys
import csv
import datetime

sys.path.insert(0, "../API")
import fp

now = datetime.datetime.utcnow()
now = now.strftime("%Y-%m-%dT%H:%M:%SZ")

def ingest(file):
    if file == "-":
        reader = csv.reader(sys.stdin)
    else:
        reader = csv.reader(open(file))
    ingest_list = []
    size = 0
    for line in reader:
        (trid, codever, codes, length, artist, release, track) = line
        ingest_list.append({"track_id": trid,
                            "codever": codever,
                            "fp": codes,
                            "length": length,
                            "artist": artist,
                            "release": release,
                            "track": track,
                            "import_date":now,
                            "source": "master"})
        size += 1
        if size % 1000 == 0:
            sys.stdout.write(".")
            sys.stdout.flush()
        if size == 10000:
            size = 0
            fp.ingest(ingest_list, do_commit=False, split=False)
            ingest_list = []
    fp.ingest(ingest_list, do_commit=True, split=False)
    print ""

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >>sys.stderr, "usage: %s replication [files ...]"
        print >>sys.stderr, "       use - for stdin"
        sys.exit(1)
    numfiles = len(sys.argv)-1
    count = 1
    for f in sys.argv[1:]:
        print "importing file %d of %d: %s" % (count, numfiles, f)
        count += 1
        ingest(f)