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
package sleeper.core.statestore.commit;

/**
 * Represents the different types of state store commit requests that can be performed.
 */
public enum CommitRequestType {
    STORED_IN_S3,
    COMPACTION_FINISHED,
    INGEST_ADD_FILES,
    COMPACTION_JOB_ID_ASSIGNMENT,
    SPLIT_PARTITION,
    GARBAGE_COLLECTED_FILES
}
