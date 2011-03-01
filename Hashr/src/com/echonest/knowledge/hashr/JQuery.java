package com.echonest.knowledge.hashr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * A SolrJ query interface.
 */
public class JQuery implements Runnable {

    private static final Logger logger =
            Logger.getLogger(JQuery.class.getName());

    SolrServer solr;

    String path;

    List<String> l;

    long tt;

    public JQuery(List<String> l, String solr, String path) throws
            MalformedURLException {
        this.solr = new CommonsHttpSolrServer(solr);
        this.path = path;
        logger.info(String.format("path: %s", path));
        this.l = l;
    }

    public void run() {
        int n = 0;
        for(String q : l) {
            ModifiableSolrParams params = new ModifiableSolrParams();
            params.add(CommonParams.Q, q);
            params.add(CommonParams.FL, "*,score");
            SolrRequest query = new QueryRequest(params);
            query.setPath(path);
            long start = System.currentTimeMillis();
            try {
                QueryResponse resp = new QueryResponse(solr.request(query), solr);
                long elapsed = System.currentTimeMillis() - start;
                tt += elapsed;
                n++;
                if(n % 200 == 0) {
                    logger.info(String.format("%s nq: %d avg: %.2f", Thread.currentThread().getName(), n, (double) tt/n));
                }
            } catch(Exception ex) {
                logger.log(Level.SEVERE, "Error running query");
                return;
            }
        }
        logger.info(String.format("%s nq: %d avg: %.2f", Thread.currentThread().
                getName(), n, (double) tt / n));
    }

    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
            System.err.println(
                    "Usage: JQuery <solr URL> <component> <query file> <n threads>");
            return;
        }

        BufferedReader r = new BufferedReader(new FileReader(args[2]));
        List<String> l = new ArrayList<String>();
        String q;
        while((q = r.readLine()) != null) {
            l.add(q);
        }

        int nThreads = Integer.parseInt(args[3]);

        if(nThreads == 1) {
            JQuery jq = new JQuery(l, args[0], args[1]);
            jq.run();
            logger.info(String.format(
                    "Over %d threads: %d queries avg time: %.2f", nThreads, l.size(), 
                    (double) jq.tt / l.size()));
            return;

        }

        int per = l.size() / nThreads;
        JQuery[] qs = new JQuery[nThreads];
        Thread[] ts = new Thread[nThreads];

        for(int i = 0; i < nThreads; i++) {
            List<String> tl = new ArrayList<String>();
            for(int j = 0; j < per; j++) {
                tl.add(l.remove(0));
            }
            qs[i] = new JQuery(tl, args[0], args[1]);
        }

        for(int i = 0; i < nThreads; i++) {
            ts[i] = new Thread(qs[i]);
            ts[i].start();
        }

        for(int i = 0; i < nThreads; i++) {
            ts[i].join();
        }

        long tt = 0;
        long n = 0;
        for(int i = 0; i < qs.length; i++) {
            tt += qs[i].tt;
            n += qs[i].l.size();
        }
        logger.info(String.format("Over %d threads: %d queries avg time: %.2f", nThreads, n, (double) tt / n));
    }
}
