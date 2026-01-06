/*
 * Copyright 2022-2025 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.trino;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import sleeper.core.key.Key;
import sleeper.core.range.Range;
import sleeper.core.schema.Schema;
import sleeper.trino.handle.SleeperColumnHandle;
import sleeper.trino.handle.SleeperPartitioningHandle;
import sleeper.trino.handle.SleeperSplit;
import sleeper.trino.utils.SleeperPageBlockUtils;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

/**
 * Determines which splits are read from which node, and which rows are written to which node. These two related
 * purposes appear to operate as follows:
 * <ul>
 * <li>During a SELECT statement, the methods {@link #getBucketNodeMap}, {@link #listPartitionHandles} and
 * {@link #getSplitBucketFunction} are called. These are (presumably) used to allow a split to be read on a specific
 * node, such as when the data is local to that node.</li>
 * <li>During an INSERT statement, the methods {@link #getBucketNodeMap}, {@link #getBucketFunction} and
 * {@link #getSplitBucketFunction} are called, although the split-bucket function that is returned is never called by
 * the framework. These allow individual rows of data to be allocated to a specific bucket, and hence a specific
 * node.</li>
 * </ul>
 * <p>
 * In this implementation, Trino partitions (aka. buckets) are not allocated to specific nodes, as the data is not
 * stored locally on any node and so there is no reason to constrain the framework in this way. During read operations,
 * partitioning is not applied, as each split already references a single Sleeper partition. Partitioning is applied
 * during write operations so that all the rows for a particular Sleeper partition end up on single node and therefore
 * just one file will be written into S3 for that partition.
 * <p>
 * Note that the Trino implementation is a little confused: some of the methods refer to the Trino partition a bucket
 * number, which is an integer, whereas other functions refer to the Trino partition as a
 * {@link ConnectorPartitionHandle}.
 */
public class SleeperNodePartitioningProvider implements ConnectorNodePartitioningProvider {
    /**
     * Obtain the mapping between the partition (bucket number) and the node where that partition is to be handled. In
     * this implementation, there is no fixed mapping.
     *
     * @param  transactionHandle  the transaction to run under
     * @param  session            the session to run under
     * @param  partitioningHandle the partitioning scheme to use
     * @return                    the bucket-node mapping
     */
    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMapping(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle) {
        SleeperPartitioningHandle sleeperPartitioningHandle = (SleeperPartitioningHandle) partitioningHandle;
        return Optional.of(ConnectorBucketNodeMap.createBucketNodeMap(sleeperPartitioningHandle.getNoOfPartitions()));
    }

    /**
     * Obtain a function which identifies which partition (bucket number) the supplied split is in. This is used during
     * read operartions, even when the {@link #listPartitionHandles} method returns NOT_PARTITIONED. It is also used
     * during write operations, although in this case, the returned function is never called.
     *
     * @param  transactionHandle  the transaction to run under
     * @param  session            the session to run under
     * @param  partitioningHandle the partitioning scheme to use
     * @return                    a function which takes a {@link SleeperSplit} and returns a bucket number, as an
     *                            integer
     */
    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle) {
        SleeperPartitioningHandle sleeperPartitioningHandle = (SleeperPartitioningHandle) partitioningHandle;
        return connectorSplit -> {
            SleeperSplit sleeperSplit = (SleeperSplit) connectorSplit;
            Schema schema = sleeperSplit.getSleeperSchema();
            Key minKey = Key.create(sleeperSplit.getLeafPartitionQuery().getPartitionRegion().getRangesOrdered(schema).stream().map(Range::getMin).collect(ImmutableList.toImmutableList()));
            return sleeperPartitioningHandle.getPartitionNo(minKey);
        };
    }

    /**
     * Obtain a function which identifies which partition (bucket number) any row belongs to. This is used during write
     * operations to ensure that rows belonging to the same Trino paritition are sent to the same node to be written.
     * <p>
     * In this implementation, the supplied bucket count is ignored and the bucket number is taken from the supplied
     * partitioning handle. There is precedent for this in the standard
     * io.trino.plugin.hive.HiveNodePartitioningProvider,
     * but it is not clear whether this approach will cause difficulties elsewhere.
     *
     * @param  transactionHandle     the transaction to run under
     * @param  session               the session to run under
     * @param  partitioningHandle    the partitioning scheme to use
     * @param  partitionChannelTypes the Trino types of the channels
     * @param  bucketCount           the number of buckets, currently ignored
     * @return                       the {@link BucketFunction}
     */
    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes,
            int bucketCount) {
        SleeperPartitioningHandle sleeperPartitioningHandle = (SleeperPartitioningHandle) partitioningHandle;
        List<SleeperColumnHandle> sleeperColumnHandlesInOrder = sleeperPartitioningHandle.getSleeperColumnHandlesInOrder();
        List<Integer> rowKeyChannelNumbersInOrder = IntStream.range(0, sleeperColumnHandlesInOrder.size())
                .filter(channelNo -> sleeperColumnHandlesInOrder.get(channelNo).getColumnCategory().equals(SleeperColumnHandle.SleeperColumnCategory.ROWKEY))
                .boxed()
                .collect(ImmutableList.toImmutableList());
        // BucketFunction has a single method which can be replaced by the following lambda
        return (page, positionNo) -> {
            List<Object> keyFields = rowKeyChannelNumbersInOrder.stream()
                    .map(channelNo -> SleeperPageBlockUtils.readObjectFromPage(sleeperColumnHandlesInOrder, page, channelNo, positionNo))
                    .collect(ImmutableList.toImmutableList());
            Key key = Key.create(keyFields);
            return sleeperPartitioningHandle.getPartitionNo(key);
        };
    }
}
