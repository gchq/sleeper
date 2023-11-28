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

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;

public class LoggedDuration {
    private static final DecimalFormat FORMATTER = new DecimalFormat("0.###");
    private final Duration duration;
    private final boolean shortOutput;

    private LoggedDuration(Duration duration, boolean shortOutput) {
        this.duration = duration;
        this.shortOutput = shortOutput;
    }

    public static LoggedDuration withFullOutput(Instant start, Instant end) {
        return new LoggedDuration(Duration.between(start, end), false);
    }

    public static LoggedDuration withShortOutput(Instant start, Instant end) {
        return new LoggedDuration(Duration.between(start, end), true);
    }

    public long getSeconds() {
        return duration.getSeconds();
    }

    @Override
    public String toString() {
        if (shortOutput) {
            return getShortString();
        } else {
            return getLongString();
        }
    }

    private String getLongString() {
        String output = "";
        long seconds = duration.getSeconds();
        if (seconds >= 3600) {
            output += (seconds / 3600) + " hour" + ((seconds / 3600) > 1 ? "s " : " ");
            seconds %= 3600;
        }
        if (seconds >= 60) {
            output += (seconds / 60) + " minute" + ((seconds / 60) > 1 ? "s " : " ");
            seconds %= 60;
        }
        output += FORMATTER.format(seconds + (double) duration.getNano() / 1_000_000_000) + " second"
                + (seconds > 1 || seconds == 0 ? "s" : "");
        return output;
    }

    private String getShortString() {
        String output = "";
        long seconds = duration.getSeconds();
        if (seconds >= 3600) {
            output += (seconds / 3600) + "h ";
            seconds %= 3600;
        }
        if (seconds >= 60) {
            output += (seconds / 60) + "m ";
            seconds %= 60;
        }
        output += FORMATTER.format(seconds + (double) duration.getNano() / 1_000_000_000) + "s";
        return output;
    }
}
