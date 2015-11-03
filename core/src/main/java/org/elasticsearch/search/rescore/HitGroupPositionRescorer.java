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
import java.lang.Math;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;


public class HitGroupPositionRescorer implements Rescorer {

    public final static Rescorer INSTANCE = new HitGroupPositionRescorer();
    public final static String NAME = "hit_group_position";

    private final static ESLogger logger = Loggers.getLogger(HitGroupPositionRescorer.class);

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
    public TopDocs rescore(TopDocs topDocs, SearchContext context, RescoreSearchContext rescoreContext) throws IOException {
        assert rescoreContext != null;
        if (topDocs == null || topDocs.totalHits == 0 || topDocs.scoreDocs.length == 0) {
            return topDocs;
        }

        final HitGroupPositionRescoreContext rescoreCtx = (HitGroupPositionRescoreContext) rescoreContext;

        ScoreDoc[] hits = topDocs.scoreDocs;
        int windowSize = Math.min(rescoreCtx.window(), hits.length);
        Arrays.sort(hits, 0, windowSize, DOC_COMPARATOR);

        List<LeafReaderContext> readerContexts = context.searcher().getIndexReader().leaves();
        int currentReaderIx = 0;
        LeafReaderContext currentReaderContext = readerContexts.get(currentReaderIx);

        MappedFieldType groupFieldType = context.smartNameFieldType(rescoreCtx.groupField);
        SortedBinaryDocValues fieldValues = context.fieldData().getForField(groupFieldType).load(currentReaderContext).getBytesValues();

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
                fieldValues = context.fieldData().getForField(groupFieldType).load(currentReaderContext).getBytesValues();
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
        SearchScript boostScript = rescoreCtx.boostScript;
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
            hit.score = hit.score * leafBoostScript.runAsFloat();

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
        // FIXME: We need document position within its group to explain score
        return sourceExplanation;
    }    

    @Override
    public RescoreSearchContext parse(XContentParser parser, SearchContext context) throws IOException {
        // Supports only groovy scripts

        ParseField BOOST_SCRIPT = new ParseField("boost_script");

        Token token;
        String currentName = null;
        Script script = null;
        String groupField = null, scriptValue = null, scriptLang = null;
        ScriptService.ScriptType scriptType = ScriptService.ScriptType.INLINE;
        Map<String, Object> scriptParams = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.parseFieldMatcher().match(currentName, BOOST_SCRIPT)) {
                    script = Script.parse(parser, context.parseFieldMatcher());
                } else {
                    throw new SearchParseException(context, "hit_group_position rescorer doesn't support [" + currentName + "]", parser.getTokenLocation());
                }
            } else if (token.isValue()) {
                if ("group_field".equals(currentName)) {
                    groupField = parser.text();
                } else if ("boost_script".equals(currentName)) {
                    scriptValue = parser.text();
                    scriptType = ScriptService.ScriptType.INLINE;
                } else if ("boost_script_id".equals(currentName)) {
                    scriptValue = parser.text();
                    scriptType = ScriptService.ScriptType.INDEXED;
                } else if ("boost_script_file".equals(currentName)) {
                    scriptValue = parser.text();
                    scriptType = ScriptService.ScriptType.FILE;
                } else if ("boost_script_params".equals(currentName)) {
                    scriptParams = parser.map();
                } else {
                    throw new SearchParseException(context, "hit_group_position rescorer doesn't support [" + currentName + "]", parser.getTokenLocation());
                }
            }
        }

        if (groupField == null) {
            throw new SearchParseException(context, "Must specify group_field for hit_group_position rescore ", parser.getTokenLocation());
        }

        if (script == null) {
            if (scriptValue == null) {
                throw new SearchParseException(context, "Must specify boost_script for hit_group_position rescore ", parser.getTokenLocation());
            }

            script = new Script(scriptValue, scriptType, scriptLang, scriptParams);
        }

        if (script == null) {
            throw new SearchParseException(context, "Must specify boost_script for hit_group_position rescore", parser.getTokenLocation());
        }

        if (script.getLang() != null && script.getLang().equals("expression")) {
            throw new SearchParseException(context, "'expression' language is not supported for boost_script parameter in hit_group_position rescorer", parser.getTokenLocation());
        }

        SearchScript boostScript = context.scriptService()
            .search(context.lookup(),
                script,
                ScriptContext.Standard.SEARCH,
                Collections.<String, String>emptyMap());

        return new HitGroupPositionRescoreContext(this, groupField, boostScript);
    }

    @Override
    public void extractTerms(SearchContext context, RescoreSearchContext rescoreContext, Set<Term> termsSet) {
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
