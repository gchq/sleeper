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
package sleeper.systemtest.dsl.statestore;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;

public interface StateStoreCommitterLogs {

    Map<String, Integer> getNumCommitsByTableId();

    static StateStoreCommitterLogs from(List<StateStoreCommitterLogEntry> logs) {
        return () -> logs.stream()
                .filter(entry -> entry instanceof StateStoreCommitSummary)
                .map(entry -> (StateStoreCommitSummary) entry)
                .collect(groupingBy(StateStoreCommitSummary::getTableId, summingInt(commit -> 1)));
    }

}