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
package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.utils.ChecksumType;

public class ChecksummedRandomAccessReader
{
    public static final class Builder extends RandomAccessReader.Builder
    {
        private final DataIntegrityMetadata.ChecksumValidator validator;

        @SuppressWarnings("resource")
        public Builder(File file, File crcFile) throws IOException
        {
            super(new ChannelProxy(file));
            this.validator = new DataIntegrityMetadata.ChecksumValidator(ChecksumType.CRC32,
                                                                         RandomAccessReader.open(crcFile),
                                                                         file.getPath());
        }

        @Override
        protected Rebufferer createRebufferer()
        {
            return new ChecksummedRebufferer(channel, validator);
        }

        @Override
        public RandomAccessReader build()
        {
            // Always own and close the channel.
            return buildWithChannel();
        }
    }
}
