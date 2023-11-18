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

package org.apache.cassandra.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.telemetry.metrics.OpenTelemetryMetricRegistryListener;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Holds references to OpenTelemetry objects
 */
public final class Telemetry
{
    private static final OpenTelemetry otel;

    static
    {
        if (CassandraRelevantProperties.ENABLE_OPENTELEMETRY.getBoolean())
        {
            otel = AutoConfiguredOpenTelemetrySdk.builder().addResourceCustomizer((r, config) ->
                r.toBuilder()
                        .put("cassandra.cluster.name", DatabaseDescriptor.getClusterName())
                        .put("service.namespace", DatabaseDescriptor.getClusterName())
                        .put("cassandra.address", InetAddressAndPort.getLocalHost().toString())
                        // This may be a host ID
                        .put("service.instance.id", InetAddressAndPort.getLocalHost().toString())
                        .put("service.version", FBUtilities.getReleaseVersionString())
                        .build()
            ).build().getOpenTelemetrySdk();
            // Metrics
            CassandraMetricsRegistry.Metrics.addListener(new OpenTelemetryMetricRegistryListener(otel.getMeter("org.apache.cassandra.metrics")));
            // TODO runtime metrics
            // Logging
            OpenTelemetryAppender.install(otel);
        }
        else
        {
            otel = OpenTelemetry.noop();
        }
    }

    private static final Tracer requestTracer = otel.getTracer("org.apache.cassandra.request");

    private Telemetry() {}

    public static OpenTelemetry getOpenTelemetry()
    {
        return otel;
    }

    /**
     * Returns OpenTelemetry {@link Tracer} to trace client requests
     *
     * @return Client request {@link Tracer}
     */
    public static Tracer getRequestTracer()
    {
        return requestTracer;
    }
}
