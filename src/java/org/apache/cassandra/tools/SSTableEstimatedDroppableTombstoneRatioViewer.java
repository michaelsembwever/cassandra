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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMetadata;

/**
 * Shows the EstimatedDroppableTombstoneRatio from a sstable metadata
 */
public class SSTableEstimatedDroppableTombstoneRatioViewer
{
    /**
     * @param args a timestamp in seconds to base gcBefore off, and a list of sstables whose metadata we're interested in
     */
    public static void main(String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length < 2)
        {
            out.println("Usage: sstabletombstonesestimate <timestamp_in_seconds> <sstable filenames>");
            System.exit(1);
        }

        int gcBefore = Integer.parseInt(args[0]);
        for (String fname : Arrays.copyOfRange(args, 1, args.length))
        {
            if (new File(fname).exists())
            {
                Descriptor descriptor = Descriptor.fromFilename(fname);
                SSTableMetadata metadata = SSTableMetadata.serializer.deserialize(descriptor).left;

                out.println();
                out.printf("Estimated droppable tombstones: %s%n", metadata.getEstimatedDroppableTombstoneRatio(gcBefore));
                out.println();
                out.printf("Minimum timestamp: %s%n", metadata.minTimestamp);
                out.printf("Maximum timestamp: %s%n", metadata.maxTimestamp);
                out.printf("SSTable max local deletion time: %s%n", metadata.maxLocalDeletionTime);
                out.println("Estimated tombstone drop times:");
                for (Map.Entry<Double, Long> entry : metadata.estimatedTombstoneDropTime.getAsMap().entrySet())
                {
                    out.printf("%-10s:%10s%n",entry.getKey().intValue(), entry.getValue());
                }
            }
            else
            {
                out.println("No such file: " + fname);
            }
        }
    }
}
