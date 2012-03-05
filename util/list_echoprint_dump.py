#!/usr/bin/python

import sys
import json

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: %s echoprint_json_dump\n", sys.argv[0])
        sys.exit(1)
    json_dump = sys.argv[1]
    j = json.load(open(json_dump))
    for c in j:
        m = c['metadata']
        sys.stdout.write(m['track_id'] + ' --- ' + m['artist'] + ' --- ' +
                         m['release'] + ' --- ' + m['title'] + '\n')
