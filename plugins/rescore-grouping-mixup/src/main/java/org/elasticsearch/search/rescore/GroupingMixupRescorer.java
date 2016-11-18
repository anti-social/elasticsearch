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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GroupingMixupRescorer implements Rescorer {
    public static final String NAME = "grouping_mixup";
    public static final GroupingMixupRescorer INSTANCE = new GroupingMixupRescorer();

    protected final org.apache.logging.log4j.Logger logger = org.elasticsearch.common.logging.Loggers.getLogger(getClass());

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

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TopDocs rescore(TopDocs topDocs, SearchContext context, RescoreSearchContext rescoreContext)
            throws IOException
    {
        assert rescoreContext != null;
        if (topDocs == null || topDocs.totalHits == 0 || topDocs.scoreDocs.length == 0) {
            return topDocs;
        }

        final Context rescoreCtx = (Context) rescoreContext;

        ScoreDoc[] hits = topDocs.scoreDocs;
        int windowSize = Math.min(rescoreCtx.window(), hits.length);
        Arrays.sort(hits, 0, windowSize, DOC_COMPARATOR);

        List<LeafReaderContext> readerContexts = context.searcher().getIndexReader().leaves();
        int currentReaderIx = 0;
        LeafReaderContext currentReaderContext = readerContexts.get(currentReaderIx);

        MappedFieldType groupFieldType = context.smartNameFieldType(rescoreCtx.groupingField);
        SortedBinaryDocValues fieldValues = context.fieldData()
                .getForField(groupFieldType)
                .load(currentReaderContext)
                .getBytesValues();

        final Map<Integer,BytesRef> groupValues = new HashMap<Integer,BytesRef>();
        Map<Integer,LeafReaderContext> docLeafContexts = new HashMap<Integer,LeafReaderContext>();

        BytesRefBuilder valueBuilder = new BytesRefBuilder();

        for (int hitIx = 0; hitIx < windowSize; hitIx++) {
            ScoreDoc hit = hits[hitIx];
            LeafReaderContext prevReaderContext = currentReaderContext;

            // find segment that contains current document
            int docId = hit.doc - currentReaderContext.docBase;
            while (docId >= currentReaderContext.reader().maxDoc()) {
                currentReaderIx++;
                currentReaderContext = readerContexts.get(currentReaderIx);
                docId = hit.doc - currentReaderContext.docBase;
            }

            docLeafContexts.put(hit.doc, currentReaderContext);

            if (currentReaderContext != prevReaderContext) {
                fieldValues = context.fieldData()
                        .getForField(groupFieldType)
                        .load(currentReaderContext)
                        .getBytesValues();
            }

            fieldValues.setDocument(docId);

            if (fieldValues.count() == 0) {
                valueBuilder.clear();
            } else {
                valueBuilder.copyBytes(fieldValues.valueAt(0));
            }
            groupValues.put(hit.doc, valueBuilder.toBytesRef());
        }

        // Sort by group value
        Arrays.sort(hits, 0, windowSize, new Comparator<ScoreDoc>() {
            @Override
            public int compare(ScoreDoc a, ScoreDoc b) {
                int cmp = groupValues.get(a.doc).compareTo(groupValues.get(b.doc));
                if (cmp == 0) {
                    return SCORE_DOC_COMPARATOR.compare(a, b);
                }
                return cmp;
            }
        });

        // Calculate new scores
        int pos = 0;
        BytesRef curGroupValue = null, prevGroupValue = null;
        SearchScript boostScript = rescoreCtx.declineScript;
        for (int i = 0; i < windowSize; i++) {
            ScoreDoc hit = hits[i];
            curGroupValue = groupValues.get(hit.doc);
            if (!curGroupValue.equals(prevGroupValue)) {
                pos = 0;
            }

            LeafReaderContext leafContext = docLeafContexts.get(hit.doc);
            LeafSearchScript leafBoostScript = boostScript.getLeafSearchScript(leafContext);
            leafBoostScript.setDocument(hit.doc - leafContext.docBase);
            leafBoostScript.setNextVar("_pos", pos);
            float oldScore = hit.score;
            hit.score = hit.score * (float) leafBoostScript.runAsDouble();
            logger.warn(String.format("hit.doc: %s, pos: %s, score: %s, old_score: %s", hit.doc, pos, hit.score, oldScore));

            pos++;
            prevGroupValue = curGroupValue;
        }

        // Sort by new score
        Arrays.sort(hits, 0, windowSize, SCORE_DOC_COMPARATOR);

        return new TopDocs(topDocs.totalHits, hits, hits[0].score);
    }

    @Override
    public Explanation explain(int topLevelDocId, SearchContext context, RescoreSearchContext rescoreContext,
                               Explanation sourceExplanation) throws IOException {
        // We cannot explain new scores because we only have single document at this point
        return sourceExplanation;
    }

    @Override
    public void extractTerms(SearchContext context, RescoreSearchContext rescoreContext, Set<Term> termsSet) {}

    public static class Context extends RescoreSearchContext {

        static final int DEFAULT_WINDOW_SIZE = 10;

        private final String groupingField;
        private final SearchScript declineScript;

        public Context(GroupingMixupRescorer rescorer, String groupingField, SearchScript declineScript) {
            super(GroupingMixupRescorerBuilder.NAME, DEFAULT_WINDOW_SIZE, rescorer);
            this.groupingField = groupingField;
            this.declineScript = declineScript;
        }
    }
}
