/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest.status.store.task;

public class DynamoDBIngestTaskStatusFormat {

    private DynamoDBIngestTaskStatusFormat() {
    }

    public static final String TASK_ID = "TaskId";
    public static final String UPDATE_TIME = "UpdateTime";
    public static final String EXPIRY_DATE = "ExpiryDate";
}
