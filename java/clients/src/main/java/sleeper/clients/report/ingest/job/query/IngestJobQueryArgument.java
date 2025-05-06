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

package sleeper.clients.report.ingest.job.query;

import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.report.job.query.JobQueryArgument;

import static sleeper.clients.util.ClientUtils.optionalArgument;

public class IngestJobQueryArgument {
    private static final String REJECTED_ARG = "-n";

    private IngestJobQueryArgument() {
    }

    public static JobQuery.Type readTypeArgument(String[] args, int index) {
        return optionalArgument(args, index)
                .map(IngestJobQueryArgument::readType)
                .orElse(JobQuery.Type.PROMPT);
    }

    private static JobQuery.Type readType(String queryTypeStr) {
        try {
            return JobQueryArgument.readType(queryTypeStr);
        } catch (IllegalArgumentException e) {
            if (REJECTED_ARG.equals(queryTypeStr)) {
                return JobQuery.Type.REJECTED;
            } else {
                throw new IllegalArgumentException("Invalid query type " + queryTypeStr + ". " +
                        "Valid query types are -d (Detailed), -r (Range), -u (Unfinished), -n (Rejected)");
            }
        }
    }
}
