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
package sleeper.clients.report.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResultField;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses individual log messages from the state store comitter.
 */
public class ReadStateStoreCommitterLogs {
    public static final Logger LOGGER = LoggerFactory.getLogger(ReadStateStoreCommitterLogs.class);

    private ReadStateStoreCommitterLogs() {
    }

    public static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS");

    private static final Pattern MESSAGE_PATTERN = Pattern.compile("" +
            "State store committer process started at ([^\\s]+)|" + // Lambda started message type
            "State store committer process finished at ([^\\s]+) |" + // Lambda finished message type
            "Applied request to table ID ([^\\s]+) with type ([^\\s]+) at time ([^\\s]+)"); // Commit applied message type

    /**
     * Constants to refer to capture groups in the regular expression above.
     */

    private static class CapturingGroups {
        private static final int START_TIME = 1;
        private static final int FINISH_TIME = 2;
        private static final int TABLE_ID = 3;
        private static final int TYPE = 4;
        private static final int COMMIT_TIME = 5;

        private CapturingGroups() {
        }
    }

    private static StateStoreCommitterLogEntry readMessage(String logStream, Instant timestamp, String message) {
        Matcher matcher = MESSAGE_PATTERN.matcher(message);
        if (!matcher.find()) {
            LOGGER.info("Couldn't match: {}", message);
            return null;
        }
        // The pattern can only match one type of log message at a time.
        // Each capturing group will be null unless its message type was matched.
        // We determine which type of message was found based on which capturing group is set.
        String startTime = matcher.group(CapturingGroups.START_TIME);
        if (startTime != null) {
            return new StateStoreCommitterRunStarted(logStream, timestamp, Instant.parse(startTime));
        }
        String finishTime = matcher.group(CapturingGroups.FINISH_TIME);
        if (finishTime != null) {
            return new StateStoreCommitterRunFinished(logStream, timestamp, Instant.parse(finishTime));
        }
        String tableId = matcher.group(CapturingGroups.TABLE_ID);
        if (tableId != null) {
            String type = matcher.group(CapturingGroups.TYPE);
            String commitTime = matcher.group(CapturingGroups.COMMIT_TIME);
            return new StateStoreCommitSummary(logStream, timestamp, tableId, type, Instant.parse(commitTime));
        }
        LOGGER.info("Match found but no group: {}", message);
        return null;
    }

    /**
     * Reads an entry from the state store committer log that matches one of the expected types of message. This can be
     * a {@link StateStoreCommitterRunStarted}, a {@link StateStoreCommitterRunFinished} or
     * a {@link StateStoreCommitSummary}.
     *
     * @param  entry the log entry as returned from Amazon CloudWatch
     * @return       the parsed entry, or null if it was not one of the expected message types
     */
    public static StateStoreCommitterLogEntry read(List<ResultField> entry) {
        String logStream = null;
        String timestamp = null;
        String message = null;
        for (ResultField field : entry) {
            switch (field.field()) {
                case "@logStream":
                    logStream = field.value();
                    break;
                case "@timestamp":
                    timestamp = field.value();
                    break;
                case "@message":
                    message = field.value();
                    break;
                default:
                    break;
            }
        }
        return readMessage(
                Objects.requireNonNull(logStream, "Log stream not found"),
                LocalDateTime.parse(timestamp, TIMESTAMP_FORMATTER).toInstant(ZoneOffset.UTC),
                Objects.requireNonNull(message, "Log message not found"));
    }
}
