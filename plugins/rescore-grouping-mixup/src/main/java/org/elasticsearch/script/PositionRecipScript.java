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

package org.elasticsearch.script;

import org.elasticsearch.common.Nullable;

import java.util.Collections;
import java.util.Map;

public class PositionRecipScript extends AbstractDoubleSearchScript
        implements ExecutableScript {

    private final double m;
    private final double a;
    private final double b;
    private final double c;

    private double pos = 0.0;

    private PositionRecipScript(@Nullable Map<String, Object> params) {
        if (params == null) {
            params = Collections.emptyMap();
        }
        m = params.containsKey("m") ? ((Double) params.get("m")).doubleValue() : 1.0;
        a = params.containsKey("a") ? ((Double) params.get("a")).doubleValue() : 1.0;
        b = params.containsKey("b") ? ((Double) params.get("b")).doubleValue() : 1.0;
        c = params.containsKey("c") ? ((Double) params.get("c")).doubleValue() : 0.0;
    }

    @Override
    public void setNextVar(String name, Object value) {
        if (name.equals("_pos")) {
            pos = ((Integer) value).intValue();
        } else {
            throw new IllegalArgumentException("Only _pos variable is allowed");
        }
    }

    @Override
    public double runAsDouble() {
        return m / (a * pos + b) + c;
    }

    public static class Factory implements NativeScriptFactory {
        private static final String NAME = "position_recip";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new PositionRecipScript(params);
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }
}
