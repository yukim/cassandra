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

import io.opentelemetry.api.common.AttributeKey;

public class CassandraSemanticAttributes
{
    private static final String NAMESPACE = "cassandra.";

    public static final AttributeKey<String> QUERY_MESSAGE_TYPE = AttributeKey.stringKey(NAMESPACE + "query.message_type");
    public static final AttributeKey<String> QUERY_CLIENT_IP = AttributeKey.stringKey(NAMESPACE + "query.client.ip");
    public static final AttributeKey<String> QUERY_COORDINATOR_IP = AttributeKey.stringKey(NAMESPACE + "query.coordinator.ip");
    public static final AttributeKey<Long> QUERY_COORDINATOR_PORT = AttributeKey.longKey(NAMESPACE + "query.coordinator.port");
    public static final AttributeKey<Long> QUERY_PAGE_SIZE = AttributeKey.longKey(NAMESPACE + "query.page_size");
    public static final AttributeKey<String> QUERY_CONSISTENCY_LEVEL = AttributeKey.stringKey(NAMESPACE + "query.consistency_level");
    public static final AttributeKey<String> QUERY_SERIAL_CONSISTENCY_LEVEL = AttributeKey.stringKey(NAMESPACE + "query.serial_consistency_level");

    public static final AttributeKey<String> NET_VERB = AttributeKey.stringKey(NAMESPACE + "net.verb");

    private CassandraSemanticAttributes() {}
}
