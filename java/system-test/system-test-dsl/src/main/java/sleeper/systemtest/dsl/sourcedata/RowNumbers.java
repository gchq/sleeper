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

package sleeper.systemtest.dsl.sourcedata;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class RowNumbers {
    private final List<Long> numbers;

    private RowNumbers(List<Long> numbers) {
        this.numbers = numbers;
    }

    public static RowNumbers scrambleNumberedRows(LongStream longStream) {
        List<Long> numbers = longStream.boxed().collect(Collectors.toList());
        Collections.shuffle(numbers, new Random(0L));
        return new RowNumbers(numbers);
    }

    public int numRows() {
        return numbers.size();
    }

    public LongStream stream() {
        return numbers.stream().mapToLong(Long::longValue);
    }

    public LongStream range(int from, int to) {
        return numbers.subList(from, to).stream().mapToLong(Long::longValue);
    }
}
