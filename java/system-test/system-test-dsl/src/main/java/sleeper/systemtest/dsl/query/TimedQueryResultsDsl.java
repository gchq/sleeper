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
package sleeper.systemtest.dsl.query;

import sleeper.core.row.Row;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public record TimedQueryResultsDsl(Instant startTime, Instant endTime, List<Row> rows) {

    public Duration duration() {
        return Duration.between(startTime, endTime);
    }

}
