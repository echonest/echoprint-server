#!/usr/bin/python

# Copyright The Echo Nest 2011

# This script updates an existing echoprint server to add fields to
# documents that are already in the index.

# The current version adds these fields:
#   * source - since 2011-08-25
#   * import_date - since 2011-08-25

import sys
import datetime

sys.path.append("../API")
import fp
import solr
import pytyrant

ROWS_PER_QUERY=1000
SOURCE = "master"

tyrant = pytyrant.PyTyrant.open("localhost", 1978)
now = datetime.datetime.utcnow()
IMPORTDATE = now.strftime("%Y-%m-%dT%H:%M:%SZ")

def process_results(results):
    response = []
    for r in results:
        if "source" in r and "import_date" in r:
            continue
        if "source" not in r:
            r["source"] = SOURCE
        if "import_date" not in r:
            r["import_date"] = IMPORTDATE
        r["fp"] = tyrant[r["track_id"]]
        response.append(r)
    return response

def main():
    print "setting source to '%s', import date to %s" % (SOURCE, IMPORTDATE)
    with solr.pooled_connection(fp._fp_solr) as host:
        # Find rows where source field doesn't exist
        results = host.query("-source:[* TO *]", rows=ROWS_PER_QUERY, score=False)
        resultlen = len(results)
        while resultlen > 0:
            print "got",resultlen,"results"
            processed = process_results(results.results)
            host.add_many(processed)
            host.commit()
            results = host.query("-source:[* TO *]", rows=ROWS_PER_QUERY, score=False)
            resultlen = len(results)
        print "done"
            
            
if __name__ == "__main__":
    main()