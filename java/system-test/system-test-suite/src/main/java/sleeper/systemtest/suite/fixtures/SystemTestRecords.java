/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.suite.fixtures;

import sleeper.core.record.Record;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class SystemTestRecords {
    private SystemTestRecords() {
    }

    public static List<Record> recordsForRange(long startInclusive, long endExclusive) {
        return LongStream.range(startInclusive, endExclusive)
                .mapToObj(i -> new Record(Map.of(
                        "key", "record-" + i,
                        "timestamp", Instant.parse("2023-07-25T13:52:00Z")
                                .plus(Duration.ofSeconds(i))
                                .getEpochSecond(),
                        "value", "Value " + i)))
                .collect(Collectors.toUnmodifiableList());
    }
}
