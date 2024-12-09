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
package sleeper.core.testutils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Helper functions to create Supplier objects.
 */
public class SupplierTestHelper {

    private SupplierTestHelper() {
    }

    /**
     * Creates a supplier of IDs that would usually be generated with UUID.randomUUID. The IDs will be supplied one at
     * a time from the list, and then throw an exception when there are no more.
     *
     * @param  ids the IDs
     * @return     the supplier
     */
    public static Supplier<String> fixIds(String... ids) {
        return List.of(ids).iterator()::next;
    }

    /**
     * Creates a supplier that would usually be defined as Instant::now. The supplied times will start at the given
     * time, then each subsequent time will be a minute later than the last.
     *
     * @param  startTime the start time
     * @return           the supplier
     */
    public static Supplier<Instant> timePassesAMinuteAtATimeFrom(Instant startTime) {
        return Stream.iterate(startTime, time -> time.plus(Duration.ofMinutes(1)))
                .iterator()::next;
    }

}
