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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class that translates Cassandra metric names to OpenTelemetry instrumentation names.
 *
 * <h2>OpenTelemetry instrument naming rule</h2>
 * Instrument names MUST conform to the following syntax (described using the Augmented Backus-Naur Form):
 *
 * <p><code>
 * instrument-name = ALPHA 0*62 ("_" / "." / "-" / ALPHA / DIGIT)
 * ALPHA = %x41-5A / %x61-7A; A-Z / a-z
 * DIGIT = %x30-39 ; 0-9
 * </code></p>
 *
 * - They are not null or empty strings.
 * - They are case-insensitive, ASCII strings.
 * - The first character must be an alphabetic character.
 * - Subsequent characters must belong to the alphanumeric characters, ‘_’, ‘.’, and ‘-’.
 * - They can have a maximum length of 63 characters.
 */
public class MetricNameTranslator
{
    private static final Map<Pattern, Function<MatchResult, MetricParams>> config;
    private static final Pattern SPLITTER = Pattern.compile("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])");

    static
    {
        LinkedHashMap<Pattern, Function<MatchResult, MetricParams>> configBuilder = new LinkedHashMap<>();
        // Table Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.Table\\.(\\w+)\\.(\\w+)\\.(\\w+)"),
                (group) -> MetricParams.builder("table_" + toSnakeCase(group.group(1)))
                            .put("cassandra.keyspace", group.group(2))
                            .put("cassandra.table", group.group(3))
                            .build()
        );
        // Keyspace Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.Keyspace\\.(\\w+)\\.(\\w+)"),
                (group) -> MetricParams.builder("keyspace_" + toSnakeCase(group.group(1)))
                        .put("cassandra.keyspace", group.group(2))
                        .build()
        );
        // ThreadPool Metrics (one type is repair.task so we just ignore the second part)
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.ThreadPools\\.(\\w+)\\.(\\w+)\\.(\\w+).*"),
                (group) -> MetricParams.builder("thread_pools_" + toSnakeCase(group.group(1)))
                        .put("cassandra.pool_type", group.group(2))
                        .put("cassandra.pool_name", group.group(3))
                        .build()
        );
        // ClientRequest Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.ClientRequest\\.(\\w+)\\.(\\w+)$"),
                (group) -> MetricParams.builder("client_request_" + toSnakeCase(group.group(1)))
                        .put("cassandra.request_type", group.group(2))
                        .build()
        );
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.ClientRequest\\.(\\w+)\\.(\\w+)-(\\w+)$"),
                (group) -> MetricParams.builder("client_request_" + toSnakeCase(group.group(1)) + "_cl")
                        .put("cassandra.request_type", group.group(2))
                        .put("cassandra.cl", group.group(3))
                        .build()
        );
        // Cache Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.Cache\\.(\\w+)\\.(\\w+)"),
                (group) -> MetricParams.builder("cache_" + toSnakeCase(group.group(1)))
                        .put("cassandra.cache_name", group.group(2))
                        .build()
        );
        // CQL Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.CQL\\.(\\w+)"),
                (group) -> MetricParams.builder("cql_" + toSnakeCase(group.group(1))).build()
        );
        // Dropped Message Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.DroppedMessage\\.(\\w+)\\.(\\w+)"),
                (group) -> MetricParams.builder("dropped_message_" + toSnakeCase(group.group(1)))
                        .put("cassandra.message_type", group.group(2))
                        .build()
        );
        // Streaming Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.Streaming\\.(\\w+)\\.(.+)"),
                (group) -> MetricParams.builder("streaming_" + toSnakeCase(group.group(1)))
                        .put("cassandra.peer_ip", group.group(2))
                        .build()
        );
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.Streaming\\.(\\w+)$"),
                (group) -> MetricParams.builder("streaming_" + toSnakeCase(group.group(1))).build()
        );
        // CommitLog Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.CommitLog\\.(\\w+)"),
                (group) -> MetricParams.builder("commit_log_" + toSnakeCase(group.group(1))).build()
        );
        // Compaction Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.Compaction\\.(\\w+)"),
                (group) -> MetricParams.builder("compaction_" + toSnakeCase(group.group(1))).build()
        );
        // Storage Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.Storage\\.(\\w+)"),
                (group) -> MetricParams.builder("storage_" + toSnakeCase(group.group(1))).build()
        );
        // Batch Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.Batch\\.(\\w+)"),
                (group) -> MetricParams.builder("batch_" + toSnakeCase(group.group(1))).build()
        );
        // Client Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.Client\\.(\\w+)"),
                (group) -> MetricParams.builder("client_" + toSnakeCase(group.group(1))).build()
        );
        // BufferPool Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.BufferPool\\.(\\w+)"),
                (group) -> MetricParams.builder("buffer_pool_" + toSnakeCase(group.group(1))).build()
        );
        // Index Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.Index\\.(\\w+)"),
                (group) -> MetricParams.builder("sstable_index_" + toSnakeCase(group.group(1))).build()
        );
        // Hint Metrics
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.HintsService\\.Hint_delays-(\\w+)"),
                (group) -> MetricParams.builder("hint_delays")
                        .put("cassandra.peer_ip", group.group(1))
                        .build()
        );
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.HintsService\\.Hints_created-(\\w+)"),
                (group) -> MetricParams.builder("hints_created")
                        .put("cassandra.peer_ip", group.group(1))
                        .build()
        );
        configBuilder.put(Pattern.compile("org\\.apache\\.cassandra\\.metrics\\.HintsService\\.([^-]+)"),
                (group) -> MetricParams.builder("hints_" + toSnakeCase(group.group(1))).build()
        );
        // Misc
        //     *      - source_labels: ["mcac"]
        //  *        regex: org\.apache\.cassandra\.metrics\.memtable_pool\.(\w+)
        //  *        target_label: __name__
        //     *        replacement: mcac_memtable_pool_${1}
        //  *      - source_labels: ["mcac"]
        //     *      - source_labels: ["mcac"]
        //  *        regex: org\.apache\.cassandra\.metrics\.read_coordination\.(.*)
        //  *        target_label: read_type
        //     *        replacement: $1
        //     *      - source_labels: ["mcac"]
        //  *        regex: org\.apache\.cassandra\.metrics\.read_coordination\.(.*)
        //  *        target_label: __name__
        //     *        replacement: mcac_read_coordination_requests
        config = Collections.unmodifiableMap(configBuilder);
    }

    private MetricNameTranslator() {}

    public static MetricParams translate(String name)
    {
        for (Map.Entry<Pattern, Function<MatchResult, MetricParams>> entry : config.entrySet())
        {
            Matcher result = entry.getKey().matcher(name);
            if (result.matches())
            {
                return entry.getValue().apply(result);
            }
        }
        return null;
    }


    private static String toSnakeCase(String s)
    {
        s = String.join("_", SPLITTER.split(s));
        s = s.replaceAll("\\._", "\\.");
        s = s.replaceAll("_+", "_");

        return s.toLowerCase();
    }
}
