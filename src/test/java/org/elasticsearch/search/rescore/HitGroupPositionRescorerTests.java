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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;


public class HitGroupPositionRescorerTests extends ElasticsearchIntegrationTest {
    @Test
    public void test() throws IOException {
        assertAcked(prepareCreate("test")
                    .addMapping("product",
                                jsonBuilder()
                                .startObject().startObject("product").startObject("properties")
                                .startObject("name").field("type", "string").field("analyzer", "whitespace").endObject()
                                .startObject("description").field("type", "string").field("analyzer", "whitespace").endObject()
                                .startObject("company_id").field("type", "integer").endObject()
                                .endObject().endObject().endObject())
                    .setSettings(ImmutableSettings.settingsBuilder()
                                 .put(indexSettings())
                                 .put("index.number_of_shards", 1)));

        client().prepareIndex("test", "product", "1")
            .setSource("name", "the quick brown fox", "company_id", 1)
            .execute()
            .actionGet();
        client().prepareIndex("test", "product", "2")
            .setSource("name", "the quick lazy huge fox jumps over the tree", "company_id", 2)
            .execute()
            .actionGet();
        client().prepareIndex("test", "product", "3")
            .setSource("name", "quick huge brown fox", "description", "the quick lazy huge brown fox jumps over the tree", "company_id", 1)
            .execute()
            .actionGet();
        client().prepareIndex("test", "product", "4")
            .setSource("name", "the quick lonely fox")
            .execute()
            .actionGet();
        ensureYellow();
        refresh();

        SearchResponse searchResponse;
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "the quick brown").operator(MatchQueryBuilder.Operator.OR);

        // No rescore
        searchResponse = client().prepareSearch()
            .setQuery(queryBuilder)
            .execute()
            .actionGet();
        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "3", "4", "2");

        // With rescoring
        searchResponse = client().prepareSearch()
            .setQuery(queryBuilder)
            .setRescorer(RescoreBuilder.hitGroupPositionRescorer("company_id", "1 / (_pos + 1)"))
            .setRescoreWindow(5)
            .execute()
            .actionGet();
        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "4", "2", "3");

        // Small size
        searchResponse = client().prepareSearch()
            .setQuery(queryBuilder)
            .setSize(2)
            .setRescorer(RescoreBuilder.hitGroupPositionRescorer("company_id", "1 / (_pos + 1)"))
            .setRescoreWindow(5)
            .execute()
            .actionGet();
        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "4");

        // Small rescoring window
        searchResponse = client().prepareSearch()
            .setQuery(queryBuilder)
            .setRescorer(RescoreBuilder.hitGroupPositionRescorer("company_id", "1 / (_pos + 1)"))
            .setRescoreWindow(3)
            .execute()
            .actionGet();
        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "4", "3", "2");
    }
}
