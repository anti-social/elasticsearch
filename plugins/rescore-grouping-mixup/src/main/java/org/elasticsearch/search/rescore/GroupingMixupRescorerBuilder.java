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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.Collections;

public class GroupingMixupRescorerBuilder extends RescoreBuilder<GroupingMixupRescorerBuilder> {
    public static final String NAME = "grouping_mixup";
    public static final RescoreParser<GroupingMixupRescorerBuilder> PARSER = new Parser();

    protected final org.apache.logging.log4j.Logger logger = org.elasticsearch.common.logging.Loggers.getLogger(getClass());

    private String groupingField;
    private Script declineScript;

    public GroupingMixupRescorerBuilder(String groupingField, Script declineScript) {
        this.groupingField = groupingField;
        this.declineScript = declineScript;
    }

    public GroupingMixupRescorerBuilder(StreamInput in) throws IOException {
        super(in);
        groupingField = in.readString();
        declineScript = new Script(in);
        // throw new IOException(String.format("GroupingMixupRescorerBuilder::new - %s, %s", groupingField, declineScript));
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(groupingField);
        declineScript.writeTo(out);
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(Parser.GROUPING_FIELD_FIELD.getPreferredName(), groupingField);
        builder.field(Parser.SCRIPT_FIELD.getPreferredName(), declineScript);
        builder.endObject();
    }

    @Override
    public RescoreSearchContext build(QueryShardContext context) throws IOException {
        SearchScript searchScript = context.getSearchScript(declineScript, ScriptContext.Standard.SEARCH);
        GroupingMixupRescorer.Context rescoreCtx = new GroupingMixupRescorer.Context(
                GroupingMixupRescorer.INSTANCE, groupingField, searchScript);
        if (windowSize != null) {
            rescoreCtx.setWindowSize(windowSize);
        }
        return rescoreCtx;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static class Parser implements RescoreParser<GroupingMixupRescorerBuilder> {
        protected final org.apache.logging.log4j.Logger logger = org.elasticsearch.common.logging.Loggers.getLogger(getClass());

        private static ParseField GROUPING_FIELD_FIELD = new ParseField("field");
        private static ParseField SCRIPT_FIELD = new ParseField("decline_script");

        @Override
        public GroupingMixupRescorerBuilder fromXContent(QueryParseContext parseContext)
                throws IOException, ParsingException
        {
            String groupingField = null;
            Script script = null;
            String fieldName = null;
            XContentParser parser = parseContext.parser();
            ParseFieldMatcher fieldMatcher = parseContext.getParseFieldMatcher();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (fieldMatcher.match(fieldName, SCRIPT_FIELD)) {
                        script = Script.parse(parser, fieldMatcher);
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                                "grouping_mixup rescorer doesn't support [" + fieldName + "]");
                    }
                } else if (token.isValue()) {
                    if (fieldMatcher.match(fieldName, GROUPING_FIELD_FIELD)) {
                        groupingField = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                                "grouping_mixup rescorer doesn't support [" + fieldName + "]");
                    }
                }
            }

            if (groupingField == null) {
                throw new ParsingException(parser.getTokenLocation(),
                        "grouping_mixup rescorer requires [field] field");
            }

            return new GroupingMixupRescorerBuilder(groupingField, script);
        }
    }
}
