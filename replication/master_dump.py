#!/usr/bin/python

# Copyright The Echo Nest 2011

# Given an echoprint 'master' server, dump all tracks that haven't been dumped since the last time.
# We store the date of the last dump in the tokyo tyrant database under the key 'lastdump'
# If the key doesn't exist, we assume there has been no dump on this database, and dump everything.
# Files generated from this script can be ingested with the import_replication.py script

import sys
import os
sys.path.insert(0, "../API")
import fp
import pytyrant
import solr
import datetime
import csv

tyrant = pytyrant.PyTyrant.open("localhost", 1978)
now = datetime.datetime.utcnow()
now = now.strftime("%Y-%m-%dT%H:%M:%SZ")

ITEMS_PER_FILE=250000
FILENAME_TEMPLATE="echoprint-replication-out-%s-%d.csv"

def dump(start=0):
    try:
        lastdump = tyrant["lastdump"]
    except KeyError:
        lastdump = "*"

    filecount = 1
    itemcount = 1
    filename = FILENAME_TEMPLATE % (now, filecount)
    writer = csv.writer(open(filename, "w"))
    with solr.pooled_connection(fp._fp_solr) as host:
        items_to_dump = host.query("import_date:[%s TO %s]" % (lastdump, now), rows=10000, start=start)
        print "going to dump %s entries" % items_to_dump.results.numFound
        resultlen = len(items_to_dump)
        while resultlen > 0:
            print "writing %d results from start=%s" % (resultlen, items_to_dump.results.start)
            for r in items_to_dump.results:
                row = [r["track_id"],
                       r["codever"],
                       tyrant[str(r["track_id"])],
                       r["length"],
                       r.get("artist", ""),
                       r.get("release", ""),
                       r.get("track", "")
                      ]
                writer.writerow(row)
            itemcount += resultlen
            if itemcount > ITEMS_PER_FILE:
                filecount += 1
                filename = FILENAME_TEMPLATE % (now, filecount)
                print "Making new file, %s" % filename
                writer = csv.writer(open(filename, "w"))
                itemcount = resultlen
            items_to_dump = items_to_dump.next_batch()
            resultlen = len(items_to_dump)

    # Write the final completion time
    tyrant["lastdump"] = now

if __name__ == "__main__":
    if len(sys.argv) > 1:
        start = int(sys.argv[1])
    else:
        start = 0
    dump(start)