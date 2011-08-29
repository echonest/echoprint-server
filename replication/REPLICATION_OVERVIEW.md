Echoprint replication
---------------------

The replication system deals with the case where there is one master server and many
slave servers. All slave servers send codes to the master, which collects these codes
and sends them out again to the other slaves. The replication process is currently
manual.

Dumping from a master
---------------------
All documents in the solr database have a field "import_date", marking the time that the document
was added to the database. A master dump simply finds all documents that are newer than the date
of the last dump. If a dump has not been performed, all documents are written out.
Run the master_dump script with no arguments to perform a dump. Files are written based on
the current date.
The date of the run is stored in the tokyo tyrant store with the key "lastdump"

    $ python master_dump.py
    $ ls echoprint-replication-out*
      echoprint-replication-out-2011-08-25T17:13:28Z-1.csv
      echoprint-replication-out-2011-08-25T17:13:28Z-2.csv
      
By default, 250000 lines are written to a file before a new file is created. Each file is
about 800MB in size.
The format of the file is a CSV document with each record representing a document from the database.

Dumping from a slave
--------------------
The slave_dump script works similarly to the master_dump script. An additional field in the document,
"source" is used to determine if a record should be dumped.
When fingerprints are ingested into the database with the fast_ingest script or the ingest() method
in fp.py the source field is set to "local". The slave_dump script will dump all documents with a 
source of local and an import_date after the last dump.


Output files can be compressed to save bandwidth and storage while sharing with other servers.
Bzip2 achieves approximately 75% compression on the resulting files.

Ingest
------
To ingest, give the ingest script the files that have been downloaded:

    $ python slave_ingest.py echoprint-replication-out-2011-08-25T17:13:28Z-4.csv
    
The master ingest script requires an option to tell it what slave ingested the song:

    $ python master_ingest.py -s the_source echoprint-relication....csv

The ingest scripts can read from stdin, so you can stream compressed files directly to them:

    $ bzcat echoprint-replication-out-2011-08-25T17:13:28Z-4.csv.bz2 | python slave_ingest.py -

Caveats/Bugs:
-------------
* There is no de-duplication process on ingest yet. If two slaves provide codes for the same
  song then it will enter the master database twice.