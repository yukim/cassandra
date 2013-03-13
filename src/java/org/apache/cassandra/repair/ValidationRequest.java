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
package org.apache.cassandra.repair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;

/**
 * ValidationRequest
 */
public class ValidationRequest
{
    public static IVersionedSerializer<ValidationRequest> serializer = new ValidationRequestSerializer();

    public final int gcBefore;

    public ValidationRequest(int gcBefore)
    {
        this.gcBefore = gcBefore;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValidationRequest that = (ValidationRequest) o;
        return gcBefore == that.gcBefore;
    }

    @Override
    public int hashCode()
    {
        return gcBefore;
    }

    public static class ValidationRequestSerializer implements IVersionedSerializer<ValidationRequest>
    {
        public void serialize(ValidationRequest request, DataOutput dos, int version) throws IOException
        {
            dos.writeInt(request.gcBefore);
        }

        public ValidationRequest deserialize(DataInput dis, int version) throws IOException
        {
            return new ValidationRequest(dis.readInt());
        }

        public long serializedSize(ValidationRequest request, int version)
        {
            return TypeSizes.NATIVE.sizeof(request.gcBefore);
        }
    }
}
