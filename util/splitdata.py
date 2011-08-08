#!/usr/bin/python

import os
import sys
try:
    import json
except ImportError:
    import simplejson as json
    
def split(file):
    parts = 5
    j = json.load(open(file))
    print "splitting %s into %d pieces" % (file, parts)
    l = len(j)
    p = int(l/parts)+1
    for i in range(parts):
        print "%d" % (i+1),
        namesplit = os.path.splitext(file)
        newname = "%s-%d%s" % (namesplit[0], (i+1), namesplit[1])
        newlist = j[i*p:i*p+p]
        json.dump(newlist, open(newname, 'w'))
    print ""

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >>sys.stderr, "usage: %s datafile [...]" % sys.argv[0]
        sys.exit(1)
    for f in sys.argv[1:]:
        print "loading %s" % f
        split(f)