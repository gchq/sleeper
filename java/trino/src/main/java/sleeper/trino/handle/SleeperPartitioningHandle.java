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
package sleeper.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.trino.spi.connector.ConnectorPartitioningHandle;

import sleeper.core.key.Key;
import sleeper.core.row.KeyComparator;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.trino.utils.SleeperSerDeUtils;
import sleeper.trino.utils.SleeperTypeConversionUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * This class describes the partitioning scheme which is to be used for a table. It is passed to methods in {@link
 * sleeper.trino.SleeperNodePartitioningProvider}, for example, so that any partitions which are generated will abide by
 * this scheme.
 * <p>
 * In this implementation, the handle stores a list of all of the lower-bounds of the Trino partitions. This ONLY works
 * when the key has a single dimension, as the Sleeper store splits on each dimension independently and this can produce
 * a difference between the key sort-order and the partition split points.
 * <p>
 * Note that the serialisation/deserialisation into JSON shares a messy approach with SleeperSplit and it may be
 * possible to improve on both of these classes.
 */
public class SleeperPartitioningHandle implements ConnectorPartitioningHandle {
    private static final String ENCODED_NULL = "!null!";

    private final List<SleeperColumnHandle> sleeperColumnHandlesInOrder;
    private final ImmutableSortedMap<Key, Integer> partitionMinKeyToPartitionNoSortedMap;

    public SleeperPartitioningHandle(SleeperTableHandle sleeperTableHandle, List<Key> partitionMinKeys) {
        this(sleeperTableHandle.getSleeperColumnHandleListInOrder(), partitionMinKeys);
    }

    public SleeperPartitioningHandle(List<SleeperColumnHandle> sleeperColumnHandlesInOrder, List<Key> partitionMinKeys) {
        this.sleeperColumnHandlesInOrder = sleeperColumnHandlesInOrder;
        List<PrimitiveType> rowKeySleeperPrimitiveTypesInOrder = sleeperColumnHandlesInOrder.stream()
                .filter(sleeperColumnHandle -> sleeperColumnHandle.getColumnCategory().equals(SleeperColumnHandle.SleeperColumnCategory.ROWKEY))
                .map(SleeperColumnHandle::getColumnTrinoType)
                .map(SleeperTypeConversionUtils::convertTrinoTypeToSleeperType)
                .map(PrimitiveType.class::cast)
                .collect(ImmutableList.toImmutableList());
        this.partitionMinKeyToPartitionNoSortedMap = IntStream.range(0, partitionMinKeys.size()).boxed()
                .collect(ImmutableSortedMap.toImmutableSortedMap(
                        new KeyComparator(rowKeySleeperPrimitiveTypesInOrder),
                        partitionMinKeys::get,
                        Function.identity()));
    }

    @JsonCreator
    public SleeperPartitioningHandle(
            @JsonProperty("sleeperColumnHandlesInOrder") List<SleeperColumnHandle> sleeperColumnHandlesInOrder,
            @JsonProperty("partitionMinKeysAsString") String partitionMinKeysAsString) {
        this(sleeperColumnHandlesInOrder,
                SleeperPartitioningHandle.decodeKeyListFromString(partitionMinKeysAsString));
    }

    private static String encodeKeyListToString(List<Key> keyListToEncode) {
        return keyListToEncode.stream()
                .map(SleeperPartitioningHandle::encodeKeyToString)
                .collect(Collectors.joining(":"));
    }

    private static String encodeKeyToString(Key keyToEncode) {
        List<Object> keyObjects = keyToEncode.getKeys();
        List<String> keyObjectTypesAsStrings = keyObjects.stream().map(obj -> obj.getClass().getCanonicalName()).collect(ImmutableList.toImmutableList());
        return Stream.of(keyObjectTypesAsStrings.stream(), keyObjects.stream())
                .flatMap(Function.identity())
                .map(obj -> (obj == null) ? ENCODED_NULL : Base64.getEncoder().encodeToString(obj.toString().getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.joining(","));
    }

    private static Key decodeKeyFromString(String keyAsString) {
        List<String> allStrings = Arrays.stream(keyAsString.split(",", -1))
                .map(str -> (str.equals(ENCODED_NULL)) ? null : new String(Base64.getDecoder().decode(str), StandardCharsets.UTF_8))
                .collect(Collectors.toList());
        // Separate the two different arrays
        int noOfOjectsInKey = allStrings.size() / 2;
        List<String> rowKeyObjectTypesAsStrings = allStrings.subList(0, noOfOjectsInKey);
        List<String> keyObjectValuesAsStrings = allStrings.subList(noOfOjectsInKey, 2 * noOfOjectsInKey);
        // Convert the key values to objects and return as a Sleeper key
        List<Object> keyObjects = IntStream.range(0, rowKeyObjectTypesAsStrings.size())
                .mapToObj(i -> SleeperSerDeUtils.convertStringToObjectOfNamedType(rowKeyObjectTypesAsStrings.get(i), keyObjectValuesAsStrings.get(i)))
                .collect(Collectors.toList());
        return Key.create(keyObjects);
    }

    private static List<Key> decodeKeyListFromString(String keyListAsString) {
        return Arrays.stream(keyListAsString.split(":", -1))
                .map(SleeperPartitioningHandle::decodeKeyFromString)
                .collect(ImmutableList.toImmutableList());
    }

    public int getNoOfPartitions() {
        return partitionMinKeyToPartitionNoSortedMap.size();
    }

    @JsonProperty
    public List<SleeperColumnHandle> getSleeperColumnHandlesInOrder() {
        return sleeperColumnHandlesInOrder;
    }

    public List<Key> getPartitionMinKeysInOrder() {
        return this.partitionMinKeyToPartitionNoSortedMap.keySet().asList();
    }

    @JsonProperty
    public String getPartitionMinKeysAsString() {
        return encodeKeyListToString(getPartitionMinKeysInOrder());
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public int getPartitionNo(Key key) {
        Key floorKey = partitionMinKeyToPartitionNoSortedMap.floorKey(key);
        return this.partitionMinKeyToPartitionNoSortedMap.get(floorKey);
    }
}
