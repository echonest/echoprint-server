package com.echonest.knowledge.hashr;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;

/**
 * A query component that takes a number of fingerprint hashes as query terms
 * and returns the documents with the most occurrences of those fingerprints.
 */
public class HashQueryComponent extends SearchComponent {

    private static final Logger logger = Logger.getLogger(HashQueryComponent.class.
            getName());

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {

        //
        // Make sure that we specify the fields in the response builder.
        SolrQueryRequest req = rb.req;
        SolrParams params = req.getParams();
        SolrQueryResponse rsp = rb.rsp;

        // Set field flags
        String fl = params.get(CommonParams.FL);
        int fieldFlags = 0;
        if(fl != null) {
            fieldFlags |= SolrPluginUtils.setReturnFields(fl, rsp);

            //
            // We'll always get the score, since that's what we're interested in
            // after all.
            fieldFlags |= SolrIndexSearcher.GET_SCORES;
        }
        rb.setFieldFlags(fieldFlags);

    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
        SolrQueryRequest req = rb.req;
        SolrQueryResponse rsp = rb.rsp;
        SolrParams params = rb.req.getParams();


        //
        // Get the parameters of interest.  One possible addition: the threshold
        // over which we need to resort to more complicated processing.
        String q = params.get("q");
        int rows = params.getInt("rows", 10);
        int start = params.getInt("start", 0);
        String[] qs = q.split(" ");
        int half = qs.length / 2;
        if(qs.length % 2 != 0) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    String.format("Hash query with %d hashes has %d offsets",
                    half + 1, half));
        }

        //
        // Get the terms and offsets.
        String[] terms = new String[half];
        int[] offsets = new int[half];
        for(int i = 0, j = 0; i < qs.length; i += 2, j++) {
            terms[j] = qs[i];
            try {
                offsets[j] = Integer.parseInt(qs[i + 1]);
            } catch(NumberFormatException ex) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                        String.format("Hash %s has non-integer offset %s",
                        qs[i], qs[i + 1]));
            }
        }


        //
        // Run the search.
        SolrIndexSearcher searcher = req.getSearcher();
        DocList dl = eval(searcher.getIndexReader(), terms, rb, rows, start);
        SolrIndexSearcher.QueryResult qr = new SolrIndexSearcher.QueryResult();
        qr.setDocList(dl);
        rb.setResult(qr);
        rsp.add("response", rb.getResults().docList);
        rsp.getToLog().add("hits", rb.getResults().docList.matches());
        
        SolrPluginUtils.optimizePreFetchDocs(rb.getResults().docList, rb.
                getQuery(), req, rsp);
    }

    /**
     * Checks to see whether the given document and count should be placed on
     * the heap of results that we're building.
     * @param h the heap
     * @param hsize the number of docs and counts we want to collect
     * @param doc the document under consideration
     * @param count the count associated with the document
     */
    private void heapCheck(PriorityQueue<DocTermCount> h, int hsize, int doc, int count) {
        if(h.size() < hsize) {
            h.offer(new DocTermCount(doc, count));
        } else {
            if(h.peek().count < count) {
                DocTermCount dtc = h.poll();
                dtc.setDoc(doc);
                dtc.setCount(count);
                h.offer(dtc);
            }
        }
    }

    /**
     * Evaluates this fingerprint query against the index.  The basic idea: for
     * each sub-reader in this reader, we iterate through all of the query terms.
     * We pull the <code>TermDocs</code> for that term and put the document IDs
     * into an array called <code>alld</code> that we'll expand as necessary.  Note
     * that a document ID might end up in alld multiple times as it may occur in
     * multiple terms.
     *
     * <p>
     *
     * Once all the terms have been processed, we sort <code>alld</code> which
     * will make <code>alld</code> contain runs of the same document ID.
     * We use sorting rather than doing merging, because this is faster in
     * general, especially for larger numbers of documents.
     *
     * <p>
     *
     * We then walk down <code>alld</code> counting the number of occurrences of
     * a given ID.  When we've counted all the occurrences, we look at the heap
     * (actually, a min-heap) of return values that we're maintaining.  If the heap doesn't have enough
     * stuff in it yet, we just add the current document ID, after transforming
     * the document ID from a sub-reader specific document ID to a global ID.  If
     * the heap already has enough stuff, we only add the current document if it's
     * count is greater than the one at the root of the heap.
     *
     * <p>
     *
     * Once we're done that, we just take the values off the heap in reverse order
     * and return the resulting list of documents.
     *
     *
     *
     * @param reader the index reader
     * @param queryTerms the terms we want to look up
     * @param rb the response builder
     * @throws IOException
     */
    private DocList eval(IndexReader reader, String[] queryTerms,
            ResponseBuilder rb, int rows, int start) throws IOException {
        int hsize = start + rows;
        PriorityQueue<DocTermCount> h = new PriorityQueue<DocTermCount>(hsize);
        Set<String> termSet = new HashSet<String>();

        //
        // Uniquify the query terms before processing to avoid multiple counts.
        termSet.addAll(Arrays.asList(queryTerms));
        
        int[] docs = new int[32];
        int[] freqs = new int[32];
        int[] alld = new int[2048];
        int base = 0;
        int nHits = 0;
        for(IndexReader sub : reader.getSequentialSubReaders()) {
            int p = 0;
            for(String t : termSet) {
                TermDocs td = sub.termDocs(new Term("fp", t));
                int pos = td.read(docs, freqs);
                while(pos != 0) {
                    for(int i = 0; i < pos; i++) {
                        if(p >= alld.length) {
                            alld = Arrays.copyOf(alld, alld.length * 2);
                        }
                        alld[p++] = docs[i];
                    }
                    pos = td.read(docs, freqs);
                }
                td.close();
            }

            //
            // We only need to process this sub if we got some hits.
            if(p > 0) {
                Arrays.sort(alld, 0, p);
                int curr = alld[0];
                int count = 0;
                for(int i = 0; i < p; i++) {
                    int doc = alld[i];
                    if(doc == curr) {
                        count++;
                    } else {
                        nHits++;
                        curr += base;
                        heapCheck(h, hsize, curr, count);
                        curr = doc;
                        count = 1;
                    }
                }
                //
                // Handle the last document that was collected.
                heapCheck(h, hsize, curr+base, count);
            }
            base += sub.maxDoc();
        }

        int outSize = Math.min(hsize, h.size());
        int[] rd = new int[outSize];
        float[] rs = new float[outSize];
        for(int i = outSize - 1; i >= 0; i--) {
            DocTermCount top = h.poll();
            rd[i] = top.getDoc();
            rs[i] = top.getCount();
        }
        //
        // Make sure we handle an empty set.
        return new DocSlice(0, outSize, rd, rs, nHits, outSize == 0 ? 0 : rs[0]);
    }

    @Override
    public String getDescription() {
        return "Queries for a number of fingerprint hashes, "
                + "returning the documents with the most occurrences";
    }

    @Override
    public String getSourceId() {
        return "HashQueryComponent";
    }

    @Override
    public String getSource() {
        return "HashQueryComponent";
    }

    @Override
    public String getVersion() {
        return "HashQueryComponent";
    }

    public static class DocTermCount implements Comparable<DocTermCount> {

        private int doc;

        private int count;

        public DocTermCount(int doc, int count) {
            this.doc = doc;
            this.count = count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public int getCount() {
            return count;
        }

        public int getDoc() {
            return doc;
        }

        public void setDoc(int doc) {
            this.doc = doc;
        }

        public int compareTo(DocTermCount o) {
            return count - o.count;
        }
    }
}
