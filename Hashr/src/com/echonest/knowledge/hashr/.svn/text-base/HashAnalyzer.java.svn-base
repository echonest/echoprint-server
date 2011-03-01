package com.echonest.knowledge.hashr;

import java.io.Reader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

/**
 * An analyzer for a fingerprint hash field that has the form:
 *
 * <pre>
 * <hash> <offset> <hash> <offset>...
 * </pre>
 */
public class HashAnalyzer extends Analyzer {

    protected final static Logger logger = Logger.getLogger(
            HashAnalyzer.class.getName());

    @Override
    public TokenStream tokenStream(String string, Reader reader) {
        return new HashFilter(new WhitespaceTokenizer(reader));
    }
}
