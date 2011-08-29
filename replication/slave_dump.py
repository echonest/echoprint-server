#!/usr/bin/python

# Copyright The Echo Nest 2011

# If fingerprints have been added to a local database, they must be contibuted back
# under the terms of the echoprint data license.

# This assumes there is one master. Files generated with this script can be imported
# with master_ingest

import sys
import os
sys.path.insert(0, "../API")
import fp
import pytyrant
import solr
import datetime
import csv

SLAVE_NAME="thisslave"

tyrant = pytyrant.PyTyrant.open("localhost", 1978)
now = datetime.datetime.utcnow()
now = now.strftime("%Y-%m-%dT%H:%M:%SZ")

ITEMS_PER_FILE=250000
FILENAME_TEMPLATE="echoprint-slave-%s-%s-%d.csv"

def check_for_fields():
    with solr.pooled_connection(fp._fp_solr) as host:
        results = host.query("-source:[* TO *]", rows=1, score=False)
        if len(results) > 0:
            print >>sys.stderr, "Missing 'source' field on at least one doc. Run util/upgrade_server.py"
            sys.exit(1)
        results = host.query("-import_date:[* TO *]", rows=1, score=False)
        if len(results) > 0:
            print >>sys.stderr, "Missing 'import_date' field on at least one doc. Run util/upgrade_server.py"
            sys.exit(1)        

def dump(start=0):
    check_for_fields()
    try:
        lastdump = tyrant["lastdump"]
    except KeyError:
        lastdump = "*"
    filecount = 1
    itemcount = 1
    filename = FILENAME_TEMPLATE % (SLAVE_NAME, now, filecount)
    writer = csv.writer(open(filename, "w"))
    with solr.pooled_connection(fp._fp_solr) as host:
        items_to_dump = host.query("source:local AND import_date:[%s TO %s]" % (lastdump, now), rows=10000, start=start)
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
                filename = FILENAME_TEMPLATE % (SLAVE_NAME, now, filecount)
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