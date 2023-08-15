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

package sleeper.systemtest.datageneration;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class RecordNumbers {
    private final List<Long> records;

    private RecordNumbers(List<Long> records) {
        this.records = records;
    }

    public static RecordNumbers scrambleNumberedRecords(LongStream longStream) {
        List<Long> records = longStream.boxed().collect(Collectors.toList());
        Collections.shuffle(records, new Random(0L));
        return new RecordNumbers(records);
    }

    public List<Long> asList() {
        return records;
    }

    public LongStream range(int from, int to) {
        return records.subList(from, to).stream().mapToLong(Long::longValue);
    }
}
