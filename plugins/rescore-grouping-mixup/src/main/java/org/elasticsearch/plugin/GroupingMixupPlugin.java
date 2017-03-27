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

package org.elasticsearch.plugin;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.PositionRecipScript;
import org.elasticsearch.search.rescore.GroupingMixupRescorerBuilder;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

public class GroupingMixupPlugin extends Plugin
        implements SearchPlugin, ScriptPlugin
{

    @Override
    public List<SearchPlugin.RescoreSpec<?>> getRescorers() {
        List<SearchPlugin.RescoreSpec<?>> rescorers = new ArrayList<>();
        for (String name : GroupingMixupRescorerBuilder.NAME.getAllNamesIncludedDeprecated()) {
            rescorers.add(new SearchPlugin.RescoreSpec<>
                (name,
                 GroupingMixupRescorerBuilder::new,
                 GroupingMixupRescorerBuilder.PARSER));
        }
        return rescorers;
    }

    @Override
    public List<NativeScriptFactory> getNativeScripts() {
        return singletonList(new PositionRecipScript.Factory());
    }
}
