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

package sleeper.core.util;

import java.time.Duration;
import java.time.Instant;

public class LoggedDuration {
    private final Duration duration;

    private LoggedDuration(Duration duration) {
        this.duration = duration;
    }

    public static LoggedDuration between(Instant start, Instant end) {
        return new LoggedDuration(Duration.between(start, end));
    }

    @Override
    public String toString() {
        if (duration.getNano() == 0) {
            return String.format("%d", duration.getSeconds());
        } else {
            return String.format("%.3f", duration.getSeconds() + (double) duration.getNano() / 1_000_000_000);
        }
    }
}
