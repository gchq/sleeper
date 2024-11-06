/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.ingest.batcher.core.testutil;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class IngestBatcherTestHelper {

    private IngestBatcherTestHelper() {
    }

    public static Supplier<String> jobIdSupplier(List<String> jobIds) {
        return Stream.concat(jobIds.stream(), infiniteIdsForUnexpectedJobs())
                .iterator()::next;
    }

    public static Supplier<Instant> timeSupplier(Instant... times) {
        return timeSupplier(List.of(times));
    }

    public static Supplier<Instant> timeSupplier(List<Instant> times) {
        return times.iterator()::next;
    }

    private static Stream<String> infiniteIdsForUnexpectedJobs() {
        return IntStream.iterate(1, n -> n + 1)
                .mapToObj(num -> "unexpected-job-" + num);
    }
}
