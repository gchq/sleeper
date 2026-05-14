/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.bulkimport.core.statemachine;

import sleeper.bulkimport.core.job.BulkImportJob;

public class DeriveJobExecutionName {

    private DeriveJobExecutionName() {
    }

    public static String jobExecutionName(BulkImportJob job) {
        String tableId = job.getTableId();
        String jobId = job.getId();
        // See maximum length restriction in AWS documentation:
        // https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html#API_StartExecution_RequestParameters
        int spaceForTableId = 80 - jobId.length() - 1;
        if (tableId.length() > spaceForTableId) {
            tableId = tableId.substring(0, spaceForTableId);
        }
        return String.join("-", tableId, jobId);
    }

}
