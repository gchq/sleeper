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

package sleeper.core.util;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;

/**
 * A wrapper around a Duration object for inclusion in log messages. This implements <code>toString</code> to produce
 * human-readable output, for lazy evaluation by a logger. If we pass an instance of this class to a logger, instead of
 * a string, we avoid building a string for a log message with a level that is not configured to be logged.
 */
public class LoggedDuration {
    private static final DecimalFormat FORMATTER = new DecimalFormat("0.###");
    private final Duration duration;
    private final boolean shortOutput;

    private LoggedDuration(Duration duration, boolean shortOutput) {
        this.duration = duration;
        this.shortOutput = shortOutput;
    }

    /**
     * Returns an instance of this class which formats a duration as a string with full output.
     * E.g "1 hour 2 minutes 3 seconds"
     *
     * @param  start the start time to calculate the duration from
     * @param  end   the end time to calculate the duration from
     * @return       an instance of this class which formats the duration with full output
     */
    public static LoggedDuration withFullOutput(Instant start, Instant end) {
        return withFullOutput(Duration.between(start, end));
    }

    /**
     * Returns an instance of this class which formats a duration as a string with short output.
     * E.g "1h 2m 3s"
     *
     * @param  start the start time to calculate the duration from
     * @param  end   the end time to calculate the duration from
     * @return       an instance of this class which formats the duration with short output
     */
    public static LoggedDuration withShortOutput(Instant start, Instant end) {
        return withShortOutput(Duration.between(start, end));
    }

    /**
     * Returns an instance of this class which formats a duration as a string with full output.
     * E.g "1 hour 2 minutes 3 seconds"
     *
     * @param  duration the duration
     * @return          an instance of this class which formats the duration with full output
     */
    public static LoggedDuration withFullOutput(Duration duration) {
        return new LoggedDuration(duration, false);
    }

    /**
     * Returns an instance of this class which formats a duration as a string with short output.
     * E.g "1h 2m 3s"
     *
     * @param  duration the duration
     * @return          an instance of this class which formats the duration with short output
     */
    public static LoggedDuration withShortOutput(Duration duration) {
        return new LoggedDuration(duration, true);
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
        boolean negative = duration.getSeconds() < 0;
        if (negative) {
            output += "-";
        }
        long seconds = Math.abs(duration.getSeconds());
        if (seconds >= 3600) {
            output += (seconds / 3600) + " hour" + ((seconds / 3600) > 1 ? "s " : " ");
            seconds %= 3600;
        }
        if (seconds >= 60) {
            output += (seconds / 60) + " minute" + ((seconds / 60) > 1 ? "s " : " ");
            seconds %= 60;
        }
        double fraction = duration.getNano() / 1_000_000_000.0;
        if (negative) {
            fraction = -fraction;
        }
        double secondsWithFraction = seconds + fraction;
        output += FORMATTER.format(secondsWithFraction) + " second"
                + (secondsWithFraction == 1 ? "" : "s");
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
