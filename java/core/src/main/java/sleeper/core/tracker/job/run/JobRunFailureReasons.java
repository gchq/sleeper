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
package sleeper.core.tracker.job.run;

import java.util.List;

/**
 * Formats reasons a run of a job failed, e.g. ingest or compaction.
 */
public class JobRunFailureReasons {

    private JobRunFailureReasons() {
    }

    /**
     * Generates a string list detailing all the reasons provided why a job run failed. The length of the string
     * created will be restricted to the given length.
     *
     * @param  maxLength the maximum length of the resulting string before it is concatenated
     * @param  reasons   the list of reasons the run failed
     * @return           combined list of reasons for failure
     */
    public static String getFailureReasonsDisplay(int maxLength, List<String> reasons) {
        if (reasons == null || reasons.isEmpty()) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (String reason : reasons) {
            String delimiter = builder.isEmpty() ? "" : ". ";
            int delimiterLength = delimiter.length();
            int ellipsisLength = "...".length();
            if (builder.length() + delimiterLength + ellipsisLength >= maxLength) {
                return builder.append("...").toString();
            } else if (builder.length() + delimiterLength + reason.length() + 1 >= maxLength) {
                return builder.append(delimiter).append(reason).substring(0, maxLength - ellipsisLength) + "...";
            } else {
                builder.append(delimiter).append(reason);
            }
        }
        return builder.toString() + ".";
    }

}
