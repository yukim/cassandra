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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.utils.FBUtilities;

public final class Util
{
    private Util()
    {
    }

    /**
     * This is used by standalone tools to force static initialization of DatabaseDescriptor, and fail if configuration
     * is bad.
     */
    public static void initDatabaseDescriptor()
    {
        try
        {
            DatabaseDescriptor.forceStaticInitialization();
        }
        catch (ExceptionInInitializerError e)
        {
            Throwable cause = e.getCause();
            boolean logStackTrace = !(cause instanceof ConfigurationException) || ((ConfigurationException) cause).logStackTrace;
            System.out.println("Exception (" + cause.getClass().getName() + ") encountered during startup: " + cause.getMessage());

            if (logStackTrace)
            {
                cause.printStackTrace();
                System.exit(3);
            }
            else
            {
                System.err.println(cause.getMessage());
                System.exit(3);
            }
        }
    }

    public static <T> Stream<T> iterToStream(Iterator<T> iter)
    {
        Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
    }

    /**
     * Construct table schema from info stored in SSTable's Stats.db
     *
     * @param desc SSTable's descriptor
     * @return Restored CFMetaData
     * @throws IOException when Stats.db cannot be read
     */
    public static CFMetaData metadataFromSSTable(Descriptor desc) throws IOException
    {
        if (!desc.version.storeRows())
            throw new IOException("pre-3.0 SSTable is not supported.");

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);

        IPartitioner partitioner = SecondaryIndexManager.isIndexColumnFamily(desc.cfname)
                                   ? new LocalPartitioner(header.getKeyType())
                                   : FBUtilities.newPartitioner(validationMetadata.partitioner);

        CFMetaData.Builder builder = CFMetaData.Builder.create("keyspace", "table").withPartitioner(partitioner);
        header.getStaticColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addStaticColumn(ident, entry.getValue());
                });
        header.getRegularColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addRegularColumn(ident, entry.getValue());
                });
        builder.addPartitionKey("PartitionKey", header.getKeyType());
        for (int i = 0; i < header.getClusteringTypes().size(); i++)
        {
            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
        }
        return builder.build();
    }
}
