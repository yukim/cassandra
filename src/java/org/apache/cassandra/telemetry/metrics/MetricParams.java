/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.telemetry.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

/**
 * OpenTelemetry parameters, translated from Codahale metrics.
 */
public class MetricParams
{
    private final String metricName;

    private final Attributes attributes;

    public static Builder builder(String metricName)
    {
        return new Builder(metricName);
    }

    private MetricParams(String metricName, Attributes attributes)
    {
        this.metricName = metricName;
        this.attributes = attributes;
    }

    public String getMetricName()
    {
        return metricName;
    }

    public Attributes getAttributes()
    {
        return attributes;
    }

    @Override
    public String toString() {
        return "MetricParams{" +
                "metricName='" + metricName + '\'' +
                ", attributes=" + attributes +
                '}';
    }

    public static class Builder
    {
        private final String metricName;

        private final AttributesBuilder attributesBuilder = Attributes.builder();

        private Builder(String metricName)
        {
            this.metricName = metricName;
        }

        public Builder put(String key, String value)
        {
            attributesBuilder.put(key, value);
            return this;
        }

        public MetricParams build()
        {
            return new MetricParams(metricName, attributesBuilder.build());
        }
    }
}
