#!/usr/bin/python

import sys
import json

if __name__ == '__main__':
    prog_name = sys.argv[0]
    if len(sys.argv) < 3:
        sys.stderr.write("Usage: %s sort_key json_dump [json_dump ...]\n" % prog_name)
        sys.exit(1)
    sort_key = sys.argv[1]
    if sort_key != 'artist' and sort_key != 'release' and sort_key != 'title':
        sys.stderr.write('Error: %s: Unknown sort key `%s\'. Try `artist\', `release\' or `title\' instead\n' % (prog_name, sort_key))
        sys.exit(1)
    list_of_raw_dumps = sys.argv[2:]
    summary_list = []
    for d in list_of_raw_dumps:
        j = json.load(open(d))
        for c in j:
            m = c['metadata']
            summary_list.append({'track_id': m['track_id'],
                                 'artist': m['artist'],
                                 'release': m['release'],
                                 'title': m['title']})
    summary_list.sort(key=lambda x: x[sort_key].lower())
    for s in summary_list:
        sys.stdout.write(s['track_id'] + ' --- ' + s['artist'] + ' --- ' +
                         s['release'] + ' --- ' + s['title'] + '\n')
