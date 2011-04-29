#!/usr/bin/env python
# encoding: utf-8

import sys
sys.path.append('../API')

import solr
fp_solr = solr.SolrConnection("http://localhost:8502/solr/fp")
fp_solr.delete_query("track_id:[* TO *]")
fp_solr.commit()
