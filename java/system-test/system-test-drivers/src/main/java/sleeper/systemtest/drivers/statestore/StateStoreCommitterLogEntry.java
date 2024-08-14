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
package sleeper.systemtest.drivers.statestore;

import sleeper.systemtest.dsl.statestore.StateStoreCommitSummary;

import java.time.Instant;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StateStoreCommitterLogEntry {

    private static final Pattern MESSAGE_PATTERN = Pattern.compile("" +
            "Lambda started at (?<startTime>.+)|" + // Lambda started message type
            "Lambda finished at (?<finishTime>[^ ]+) |" + // Lambda finished message type
            "Applied request to table ID (?<tableId>[^ ]+) with type (?<type>[^ ]+) at time (?<commitTime>[^ ]+)"); // Commit applied message type

    private StateStoreCommitterLogEntry() {
    }

    public static Object readEvent(String message) {
        Matcher matcher = MESSAGE_PATTERN.matcher(message);
        if (!matcher.find()) {
            return null;
        }
        // The pattern can only match one type of log message at a time.
        // Each capture group will be null unless its message type was matched.
        // We determine which type of message was found based on which capture group is set.
        String startTime = matcher.group("startTime");
        if (startTime != null) {
            return new LambdaStarted(Instant.parse(startTime));
        }
        String finishTime = matcher.group("finishTime");
        if (finishTime != null) {
            return new LambdaFinished(Instant.parse(finishTime));
        }
        String tableId = matcher.group("tableId");
        if (tableId != null) {
            String type = matcher.group("type");
            String commitTime = matcher.group("commitTime");
            return new StateStoreCommitSummary(tableId, type, Instant.parse(commitTime));
        }
        return null;
    }

    public static class LambdaStarted {
        private final Instant startTime;

        public LambdaStarted(Instant startTime) {
            this.startTime = startTime;
        }

        public Instant getStartTime() {
            return startTime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(startTime);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof LambdaStarted)) {
                return false;
            }
            LambdaStarted other = (LambdaStarted) obj;
            return Objects.equals(startTime, other.startTime);
        }

        @Override
        public String toString() {
            return "LambdaStarted{startTime=" + startTime + "}";
        }
    }

    public static class LambdaFinished {
        private final Instant finishTime;

        public LambdaFinished(Instant finishTime) {
            this.finishTime = finishTime;
        }

        public Instant getFinishTime() {
            return finishTime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(finishTime);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof LambdaFinished)) {
                return false;
            }
            LambdaFinished other = (LambdaFinished) obj;
            return Objects.equals(finishTime, other.finishTime);
        }

        @Override
        public String toString() {
            return "LambdaFinished{finishTime=" + finishTime + "}";
        }
    }

}
