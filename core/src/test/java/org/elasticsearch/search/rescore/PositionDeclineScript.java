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

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractFloatSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.Map;


public class PositionDeclineScript extends AbstractFloatSearchScript
        implements ExecutableScript {

    private final float m;
    private final float a;
    private final float b;

    private float pos = 0.0f;
    
    private PositionDeclineScript(@Nullable Map<String, Object> params) {
        if (params == null) {
            params = (Map<String, Object>) ImmutableMap.<String, Object>of();
        }
        m = params.containsKey("m") ? ((Number) params.get("m")).floatValue() : 1.0f;
        a = params.containsKey("a") ? ((Number) params.get("a")).floatValue() : 1.0f;
        b = params.containsKey("b") ? ((Number) params.get("b")).floatValue() : 1.0f;
    }

    @Override
    public void setNextVar(String name, Object value) {
        if (name.equals("_pos")) {
            pos = (float) ((Integer) value).intValue();
        } else {
            throw new IllegalArgumentException("Only _pos variable is allowed");
        }
    }
    
    @Override
    public float runAsFloat() {
        return (float) m / (a * pos + b);
    }

    public static class Factory implements NativeScriptFactory {

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new PositionDeclineScript(params);
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }
}
