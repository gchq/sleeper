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

import software.amazon.awssdk.services.cloudwatchlogs.model.ResultField;

import sleeper.systemtest.drivers.statestore.ReadStateStoreCommitterLogs.LambdaFinished;
import sleeper.systemtest.drivers.statestore.ReadStateStoreCommitterLogs.LambdaStarted;
import sleeper.systemtest.dsl.statestore.StateStoreCommitSummary;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterRun;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

public class StateStoreCommitterRunsBuilder {

    private final Map<String, LogStream> logStreamByName = new LinkedHashMap<>();

    public void add(List<ResultField> entry) {
        String logStream = null;
        String message = null;
        for (ResultField field : entry) {
            switch (field.field()) {
                case "@logStream":
                    logStream = field.value();
                    break;
                case "@message":
                    message = field.value();
                    break;
                default:
                    break;
            }
        }
        Objects.requireNonNull(logStream, "Log stream not found");
        Objects.requireNonNull(message, "Log message not found");
        logStreamByName.computeIfAbsent(logStream, name -> new LogStream())
                .add(ReadStateStoreCommitterLogs.readEvent(logStream, message));
    }

    public List<StateStoreCommitterRun> buildRuns() {
        return logStreamByName.entrySet().stream()
                .flatMap(entry -> entry.getValue().runs()
                        .map(run -> run.build(entry.getKey())))
                .collect(toUnmodifiableList());
    }

    private static class LogStream {

        private final List<LambdaRun> runs = new ArrayList<>();
        private LambdaRun lastRun;

        void add(Object event) {
            if (event instanceof LambdaStarted) {
                lastRun = new LambdaRun((LambdaStarted) event);
                runs.add(lastRun);
            } else if (event instanceof LambdaFinished) {
                lastRunOrUnknownStartTime().finished((LambdaFinished) event);
            } else if (event instanceof StateStoreCommitSummary) {
                lastRunOrUnknownStartTime().committed((StateStoreCommitSummary) event);
            }
        }

        private LambdaRun lastRunOrUnknownStartTime() {
            if (lastRun == null) {
                lastRun = new LambdaRun();
                runs.add(lastRun);
            }
            return lastRun;
        }

        Stream<LambdaRun> runs() {
            return runs.stream();
        }
    }

    private static class LambdaRun {
        private final Instant startTime;
        private Instant finishTime;
        private List<StateStoreCommitSummary> commits = new ArrayList<>();

        LambdaRun() {
            startTime = null;
        }

        LambdaRun(LambdaStarted event) {
            this.startTime = event.getStartTime();
        }

        void finished(LambdaFinished event) {
            finishTime = event.getFinishTime();
        }

        void committed(StateStoreCommitSummary commit) {
            commits.add(commit);
        }

        StateStoreCommitterRun build(String logStream) {
            return new StateStoreCommitterRun(logStream, startTime, finishTime, commits);
        }
    }

}
