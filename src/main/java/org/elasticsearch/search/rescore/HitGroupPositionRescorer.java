/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.rescore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.internal.SearchContext;


public class HitGroupPositionRescorer implements Rescorer {

    public static final Rescorer INSTANCE = new HitGroupPositionRescorer();
    public static final String NAME = "hit_group_position";

    private final ESLogger logger;

    private final static Comparator<ScoreDoc> DOC_COMPARATOR = new Comparator<ScoreDoc>() {
        @Override
        public int compare(ScoreDoc a, ScoreDoc b) {
            return a.doc - b.doc;
        }
    };

    private final static Comparator<ScoreDoc> SCORE_DOC_COMPARATOR = new Comparator<ScoreDoc>() {
        @Override
        public int compare(ScoreDoc a, ScoreDoc b) {
            if (a.score > b.score) {
                return -1;
            }
            else if (a.score < b.score) {
                return 1;
            }
            return a.doc - b.doc;
        }
    };

    public HitGroupPositionRescorer() {
        this.logger = Loggers.getLogger(HitGroupPositionRescorer.class);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TopDocs rescore(TopDocs topDocs, SearchContext context, RescoreSearchContext rescoreContext) throws IOException {
        assert rescoreContext != null;
        if (topDocs == null || topDocs.totalHits == 0 || topDocs.scoreDocs.length == 0) {
            return topDocs;
        }

        final HitGroupPositionRescoreContext rescoreCtx = (HitGroupPositionRescoreContext) rescoreContext;

        TopDocs topNFirstPass = topN(topDocs, rescoreContext.window());
        Arrays.sort(topNFirstPass.scoreDocs, DOC_COMPARATOR);

        List<AtomicReaderContext> readerContexts = context.searcher().getIndexReader().leaves();
        Iterator<AtomicReaderContext> readerContextIterator = readerContexts.iterator();
        AtomicReaderContext currentReaderContext = readerContextIterator.next();

        FieldMapper mapper = context.smartNameFieldMapper(rescoreCtx.groupField);
        SortedBinaryDocValues fieldValues = context.fieldData().getForField(mapper).load(currentReaderContext).getBytesValues();

        Map<BytesRef,List<ScoreDoc>> groupedDocs = new HashMap<BytesRef,List<ScoreDoc>>();
        Map<Integer,AtomicReaderContext> docLeafContexts = new HashMap<Integer,AtomicReaderContext>();

        for (ScoreDoc hit : topNFirstPass.scoreDocs) {
            AtomicReaderContext prevReaderContext = currentReaderContext;
                
            // find segment with current document
            int docId = hit.doc - currentReaderContext.docBase;
            while (docId >= currentReaderContext.reader().maxDoc()) {
                currentReaderContext = readerContextIterator.next();
                docId = hit.doc - currentReaderContext.docBase;
            }

            docLeafContexts.put(hit.doc, currentReaderContext);

            if (currentReaderContext != prevReaderContext) {
                fieldValues = context.fieldData().getForField(mapper).load(currentReaderContext).getBytesValues();
            }

            fieldValues.setDocument(docId);
            BytesRef val = fieldValues.valueAt(0);
            if (!groupedDocs.containsKey(val)) {
                groupedDocs.put(val, new ArrayList<ScoreDoc>());
            }
            groupedDocs.get(val).add(hit);
        }

        ScoreDoc[] rescoredHits = new ScoreDoc[topNFirstPass.scoreDocs.length];
        Map<AtomicReaderContext,Map<Integer,Integer>> docPositions = new HashMap<AtomicReaderContext,Map<Integer,Integer>>();
        for (List<ScoreDoc> docs : groupedDocs.values()) {
            CollectionUtil.timSort(docs, SCORE_DOC_COMPARATOR);

            int pos = 0;
            for (ScoreDoc doc : docs) {
                AtomicReaderContext leafContext = docLeafContexts.get(doc.doc);
                if (!docPositions.containsKey(leafContext)) {
                    docPositions.put(leafContext, new HashMap<Integer,Integer>());
                }
                docPositions.get(leafContext).put(doc.doc - leafContext.docBase, pos);
                pos++;
            }
        }

        SearchScript boostScript = rescoreCtx.boostScript;
        int i = 0;
        for (List<ScoreDoc> docs : groupedDocs.values()) {
            for (ScoreDoc doc : docs) {
                AtomicReaderContext leafContext = docLeafContexts.get(doc.doc);
                int docId = doc.doc - leafContext.docBase;
                boostScript.setNextReader(leafContext);
                boostScript.setNextDocId(docId);
                boostScript.setNextVar("_pos", docPositions.get(leafContext).get(docId));
                float newScore = doc.score * boostScript.runAsFloat();
                rescoredHits[i] = new ScoreDoc(doc.doc, newScore);
                i++;
            }
        }

        Arrays.sort(rescoredHits, SCORE_DOC_COMPARATOR);

        if (rescoreCtx.window() < topNFirstPass.scoreDocs.length) {
            ScoreDoc[] subset = new ScoreDoc[rescoreCtx.window()];
            System.arraycopy(rescoredHits, 0, subset, 0, rescoreCtx.window());
            rescoredHits = subset;
        }
            
        return new TopDocs(topDocs.totalHits, rescoredHits, rescoredHits[0].score);
    }

    @Override
    public Explanation explain(int topLevelDocId, SearchContext context, RescoreSearchContext rescoreContext,
                               Explanation sourceExplanation) throws IOException {
        // FIXME: We need document position within its group to explain score
        return sourceExplanation;
    }    

    @Override
    public RescoreSearchContext parse(XContentParser parser, SearchContext context) throws IOException {
        Token token;
        String currentName = null;
        String groupField = null, boostScript = null;
        Map<String, Object> params = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token.isValue()) {
                if ("group_field".equals(currentName)) {
                    groupField = parser.text();
                } else if ("boost_script".equals(currentName)) {
                    boostScript = parser.text();
                } else if ("params".equals(currentName)) {
                    params = parser.map();
                } else {
                    throw new ElasticsearchIllegalArgumentException("hit_group_position rescore doesn't support [" + currentName + "]");
                }
            }
        }

        if (groupField == null) {
            throw new ElasticsearchIllegalArgumentException("Must specify group_field for hit_group_position rescore ");
        }
        if (boostScript == null) {
            throw new ElasticsearchIllegalArgumentException("Must specify boost_script for hit_group_position rescore ");
        }
        
        String scriptLang = null;
        ScriptService.ScriptType scriptType = ScriptService.ScriptType.INLINE;
        logger.info(context.scriptService().toString());
        SearchScript searchScript = context.scriptService().search(context.lookup(), scriptLang, boostScript, scriptType, params);
        
        return new HitGroupPositionRescoreContext(this, groupField, searchScript);
    }

    @Override
    public void extractTerms(SearchContext context, RescoreSearchContext rescoreContext, Set<Term> termsSet) {
    }

    private TopDocs topN(TopDocs in, int topN) {
        if (in.totalHits < topN) {
            assert in.scoreDocs.length == in.totalHits;
            return in;
        }

        ScoreDoc[] subset = new ScoreDoc[topN];
        System.arraycopy(in.scoreDocs, 0, subset, 0, topN);

        return new TopDocs(in.totalHits, subset, in.getMaxScore());
    }

    public static class HitGroupPositionRescoreContext extends RescoreSearchContext {

        final private String groupField;
        final private SearchScript boostScript;
        
        public HitGroupPositionRescoreContext(HitGroupPositionRescorer rescorer, String groupField, SearchScript boostScript) {
            super(NAME, 10, rescorer);
            this.groupField = groupField;
            this.boostScript = boostScript;
        }
    }
}
