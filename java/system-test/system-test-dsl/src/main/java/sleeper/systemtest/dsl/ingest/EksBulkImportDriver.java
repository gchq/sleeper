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

package sleeper.systemtest.dsl.ingest;

import java.util.List;

/**
 * Runs checks on bulk import jobs that are specific to running in EKS.
 */
public interface EksBulkImportDriver {

    /**
     * Retrieves the execution status of each bulk import job submitted in this system test.
     *
     * @return a list of execution statuses (e.g. "SUCCEEDED", "FAILED", "RUNNING")
     */
    List<String> getExecutionStatuses();

    /**
     * Retrieves a list of running Kubernetes pods in the Spark namespace.
     *
     * @return a list of descriptions of running pods
     */
    List<String> getRunningPods();
}
