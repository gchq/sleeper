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

import java.time.Instant;

/**
 * A log entry recording that a state store committer batch finished processing.
 * A state store committer run can have multiple batch finishes but only one run finished.
 * Instances of this class are compared with each other to calculate which of the multi threaded
 * state store batch finishes was the last in the run.
 */
public class StateStoreCommitterThreadRunFinished extends StateStoreCommitterRunFinished {

    public StateStoreCommitterThreadRunFinished(String logStream, Instant timestamp, Instant finishTime) {
        super(logStream, timestamp, finishTime);
    }
}
