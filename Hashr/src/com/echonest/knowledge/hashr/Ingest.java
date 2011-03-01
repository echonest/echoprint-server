package com.echonest.knowledge.hashr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * A class to ingest finger print files and index them using SolrJ.
 */
public class Ingest {

    private static Logger logger = Logger.getLogger(Ingest.class.getName());

    private static void processFile(String f, SolrServer solr) throws IOException {
        BufferedReader r = new BufferedReader(new FileReader(f));
        String l;
        int ln = 0;
        int added = 0;
        List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        long start = System.currentTimeMillis();
        while((l = r.readLine()) != null) {
            ln++;
            int pos = l.indexOf(' ');
            if(pos < 0) {
                System.err.format("Weird line at %d of file %s\n", ln, f);
                continue;
            }
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("track_id", l.substring(0, pos));
            doc.addField("fp", l.substring(pos + 1));
            docs.add(doc);
            added++;
            if(docs.size() == 300) {
                long now = System.currentTimeMillis();
                try {
                    solr.add(docs);
                    logger.info(String.format("Added %d docs in %dms %.2fdocs/s", added, now - start,
                            ((double) added / (now - start)) * 1000));
                } catch(SolrServerException ex) {
                    logger.log(Level.SEVERE, "Error indexing docs", ex);
                }
//                try {
//                    solr.commit(false, false);
//                } catch(SolrServerException ex) {
//                    logger.log(Level.SEVERE, "Error committing docs", ex);
//                }
                docs.clear();
            }
        }
        if(docs.size() > 0) {
            try {
                solr.add(docs);
//                solr.commit(false, false);
            } catch(SolrServerException ex) {
                logger.log(Level.SEVERE, "Error indexing docs", ex);
            }
        }

        r.close();
    }

    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
            System.err.println(
                    "Usage: Ingest <solr URL> <dat file> <dat file>...");
            return;
        }
//        HttpClient client = new HttpClient();
//        client.getParams().setAuthenticationPreemptive(true);
//        client.getState().setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
//                "solr-admin", "futureofmusic"));
//        SolrServer solr = new CommonsHttpSolrServer(args[0], client);
        SolrServer solr = new CommonsHttpSolrServer(args[0]);
        for(int i = 1; i < args.length; i++) {
            processFile(args[i], solr);
        }
    }
}
