# Server components for Echoprint.

Echoprint is an open source music fingerprint and resolving framework powered by the [The Echo Nest](http://the.echonest.com/ "The Echo Nest"). The [code generator](http://github.com/echonest/echoprint-codegen "echoprint-codegen") (library to convert PCM samples from a microphone or file into Echoprint codes) is MIT licensed and free for any use. The [server component](http://github.com/echonest/echoprint-server "echoprint-server") that stores and resolves queries is Apache licensed and free for any use. The [data for resolving to millions of songs](http://echoprint.me/data "Echoprint Data") is free for any use provided any changes or additions are merged back to the community. 

## What is included

The Echoprint server is a custom component for Apache Solr to index Echoprint codes and hash times. In order to keep the index fast, the Echoprint codes are stored in a Tokyo Tyrant key/value store. We also include the python API layer code necessary to match tracks based on the response from the custom component as well as a demo (non-production) API meant to illustrate how to setup and run the Echoprint service.

Non-included requirements for the server:

* java 1.6
* python 2.5 or higher
* simplejson (if python < 2.6)
* [Tokyo Tyrant](http://fallabs.com/tokyotyrant/)

Additional non-included requirements for the demo:

* web.py


## What's inside

    API/ - python libraries for querying and ingesting into the Echoprint server
    API/api.py - web.py sample API wrapper for evaluation
    API/fp.py - main python module for Echoprint
    API/solr.py - Solr's python module (with slight enhancements)

    Hashr/ - java project for a custom solr field type to handle Echoprint data

    solr/ - complete solr install with Hashr already in the right place and with the right schema and config to make it work.
    
    util/ - Utilities for importing and evaluating Echoprint
    util/fastingest.py - import codes into the database
    util/bigeval.py - evaluate the search accuracy of the database


## How to run the server

1. Start the server like this (change your home directory to where you have echoprint-server/solr/solr)

        java -Dsolr.solr.home=/home/path/to/echoprint-server/solr/solr/solr/ -Djava.awt.headless=true -jar start.jar

    If you run this server somewhere else other than localhost, update the pointer to it in fp.py:

        _fp_solr = solr.SolrConnection("http://localhost:8502/solr/fp")

2. Start the Tokyo Tyrant server.

        ttservctl start

    Again, if the location of the TT server differs, update fp.py:

        _tyrant_address = ['localhost', 1978]

## Running in Python

fp.py has all the methods you'll need.

    >>> import fp
    >>> fp.ingest({"my_track_id":"123 40 123 60 123 80 123 90 123 110 123 130"})
    >>> fp.commit()
    >>> r = fp.best_match_for_query("123 40 123 60 123 80 123 90 123 110 123 130 123 150 123 160 123 80")
    >>> r.message()
    'no results found (type 2)'
    >>> example_code = "eJx1VFuSxCAIvBLyEDyOot7_CAtkp2qmdvaniYqdpiEBVGL4Dt3-geHfwSCuWfcPmPABm7_DAPsHKFjG5L-w7Rcm-AsY_oLyLyxOUPuA6e_gHHuu-bTgBSeuXYynK_wAtQEvcP4Hrn0HRAciirvk8RQaAyIQh7vfYfgLNvyChGsP9PDgDUaLvcH2AVHRNwAYg_qJ2M5qUWI4KtAiRqGe53cP6xFd-s08Z4RU3Jgk84zWldw_dDK_rR2eAWxR0fd9xE5Z6bC5k9dO8bzi7CEk-WBorsGm2Rdd--5buml62A-Hs_GxP7fnOSyD5EHHchaQsomg16ScDvNvRKFburT3OjfZI_l8Lsn3yVqlK6rR5F2bUYvnQPqw7srrcNn7Kp_QS28zLv92fCwZB5OWz5Pzgqvma4BZH54Y0lsyV88IA2SWb50yIj58bUzKfB0lD9YxK948yLyWlYXuI30WDUj67D4x73Vpp_yIPqauZoTy6Gy5XtxO-adzZz5NxVx3jwbkubJXP9HLV7XVs16LT5De8nqMZPm3hal4YZSeTe1UPV79l7s5jQiaGpg1tOapL4LUhYBnVN3iu_qpVR_tps9c6i67xE_N5YSaY4uOZbwoE6t_k1rNLSypPvTqf_wa6v13tupfdBczdovfRPnYMPULPHMfp2v_ADyGOfw="
    >>> r = fp.best_match_for_query(example_code)
    >>> r.message()
    'OK (match type 5)'
    >>> r.TRID
    u'thisone'

## Running the example API server

1. Run the api.py webserver as a test

        cd API
        python api.py 8080

2. Ingest codes with http://localhost:8080/ingest:

    POST the following variables:

        fp_code : packed code from codegen
        track_id : if you want your own track_ids. If you don't give one we'll generate one.
        length : the length of the track in seconds
        codever : the version of the codegen
        artist : the artist of the track (optional)
        release : the release of the track (optional)
        track : the track name (optional)

    For example:

        curl http://localhost:8080/ingest -d "fp_code=eJx1W...&track_id=thisone&length=300&codever=4.1"

3. Query with http://localhost:8080/query?fp_code=XXX

    POST or GET the following:

        fp_code : packed code from codegen

## Generating and importing data

1. Download and compile the echoprint-codegen
2. Generate a list of files to fingerprint

        find /music -name "*.mp3" > music_to_ingest
4. Generate fingerprint codes for your files

        ./codegen -s < music_to_ingest > allcodes.json
5. Ingest the generated json.

        python fastingest.py [-b] allcodes.json
    The -b flag creates a file named bigeval.json that can be used to evaluate the accuracy of the fingerprint and server (see below)

## Using the community data

Publicly available fingerprint data is available under the Echoprint Database License. If you want to use this data
you can download it from http://echoprint.me/data/

Use the fastingest.py tool to import this data like above:

    python fastingest.py [-b] ~/Downloads/echoprint-dump*.json

You can run fastingest many times on one or more machines, as long as you update the configuration information for solr and tokyo tyrant in fp.py

## Evaluating fingerprint accuracy
We provide an evaluation tool, _bigeval_, that can be used to test the accuracy of the fingerprint and server.

Run bigeval.py without any arguments to get a usage statement. This command will test 1000 random files.

    python bigeval.py -c 1000

For every 10 files tested, bigeval will print out a line that looks like this.

    PR 0.0875 CAR 0.9125 FAR 0.0000 FRR 0.0875 {'tn': 0, 'err-api': 0, 'fp-a': 1, 'tp': 73, 'err-codegen': 0, 'fp-b': 0, 'err-data': 0, 'total': 80, 'fn': 6, 'err-munge': 0}

This is what the fields mean:

    PR           "probability of error"  a weighted measure of the overall goodness of the FP
    CAR          "correct accept rate"   probability that you will correctly identify a known song
    FAR          "false accept rate"     probability that you will say a song is there that is not
    FRR          "false reject rate"     probability that you will say a song is not there that is
    err-api      API error               # of times the API had a timeout or error
    err-data     data problem            # of times our datastore had an issue (missing data is the biggest culprit)
    err-codegen  codegen fail            # of times codegen did not return properly with data
    err-munge    munger err              # of times the munging process (downsampling, filtering, re-encoding etc) did not generate a playable file
    fp-a         false pos A             we had a false positive where the wrong song was identified
    fp-b         false pos B             we said a song was there that was not actually there
    tp           true pos                correct song chosen
    tn           true neg                song correctly identified as not there
    fn           false neg               song there but we said it wasn't

If an error occurs during the matching, a describing the error will be printed.
Use the -p flag to print extra information about the scores obtained from solr  when an error occurs to see how the server is choosing its winner.
Use -1 <file> to test a single file and print its score information

A number of _munge_ parameters are available to bigeval. These parameters alter the input file before generating a fingerprint, to simulate noisy signals.
Run bigeval.py --help to see the available options. These options require mpg123 and ffmpeg to be installed.

You can test for _true negatives_ by creating a list of tracks that you know are not in the database:

    find /new_music -type f > new_music

Name the file new_music and put it in the same directory as bigeval.py.

## Notes

* You can run Echoprint in "local" mode which uses a python dict to store and index codes instead of Solr. You can store and index about 100K tracks in 1GB or so in practice using this mode. This is only useful for small scale testing. Each fp.py method takes an optional "local" kwarg.

