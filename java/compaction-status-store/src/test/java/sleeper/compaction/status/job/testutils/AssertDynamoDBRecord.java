/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.status.job.testutils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import sleeper.compaction.status.DynamoDBRecordBuilder;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AssertDynamoDBRecord {

    private final Set<String> keys;
    private final Map<String, AttributeValue> attributesWithoutIgnored;

    private AssertDynamoDBRecord(Set<String> keys, Map<String, AttributeValue> attributesWithoutIgnored) {
        this.keys = new TreeSet<>(keys);
        this.attributesWithoutIgnored = new TreeMap<>(attributesWithoutIgnored);
    }

    public static AssertDynamoDBRecord actualIgnoringKeys(Map<String, AttributeValue> item, Set<String> timeKeys) {
        return new AssertDynamoDBRecord(item.keySet(), removeNonNullTimes(item, timeKeys));
    }

    public static AssertDynamoDBRecord expected(Set<String> keys, DynamoDBRecordBuilder item) {
        return new AssertDynamoDBRecord(keys, item.build());
    }

    public static Set<String> keySet(String... keys) {
        return Stream.of(keys).collect(Collectors.toSet());
    }

    private static Map<String, AttributeValue> removeNonNullTimes(Map<String, AttributeValue> item, Set<String> timeKeys) {
        return item.entrySet().stream()
                .filter(entry -> !timeKeys.contains(entry.getKey()) && entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AssertDynamoDBRecord that = (AssertDynamoDBRecord) o;
        return Objects.equals(keys, that.keys) && Objects.equals(attributesWithoutIgnored, that.attributesWithoutIgnored);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keys, attributesWithoutIgnored);
    }

    @Override
    public String toString() {
        return "CompactionJobStatusRecord{" +
                "keys=" + keys +
                ", attributesWithoutIgnored=" + attributesWithoutIgnored +
                '}';
    }
}
