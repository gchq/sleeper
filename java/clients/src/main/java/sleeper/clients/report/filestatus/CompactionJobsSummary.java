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
package sleeper.clients.report.filestatus;

public class CompactionJobsSummary {

    private final int numJobsAssignedTo;
    private final int onNumPartitions;
    private final int numReferencesAssigned;

    public CompactionJobsSummary(int numJobsAssignedTo, int onNumPartitions, int numReferencesAssigned) {
        this.numJobsAssignedTo = numJobsAssignedTo;
        this.onNumPartitions = onNumPartitions;
        this.numReferencesAssigned = numReferencesAssigned;
    }

    public int getNumJobsAssignedTo() {
        return numJobsAssignedTo;
    }

    public int getOnNumPartitions() {
        return onNumPartitions;
    }

    public int getNumReferencesAssigned() {
        return numReferencesAssigned;
    }

    public double getPercentOfFileReferencesAssigned(long totalFileReferences) {
        return 100.0 * (double) numReferencesAssigned / (double) totalFileReferences;
    }

}
