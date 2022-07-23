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

import io.netty.util.concurrent.FastThreadLocal;
import io.opentelemetry.context.Context;
import org.apache.cassandra.concurrent.ExecutorLocal;
import org.apache.cassandra.concurrent.ExecutorLocals;

/**
 * ContextStorage that stores OpenTelemetry's {@link Context} to
 * Cassandra's {@link ExecutorLocals}.
 */
public class ContextHolder implements ExecutorLocal<Context>
{
    public static ContextHolder instance = new ContextHolder();

    private static final FastThreadLocal<Context> holder = new FastThreadLocal<>();

    private ContextHolder() {}

    @Override
    public void set(Context context)
    {
        holder.set(context);
    }

    @Override
    public Context get()
    {
        return holder.get();
    }
}
