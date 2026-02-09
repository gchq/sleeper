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
package sleeper.clients.report.job.query;

import java.util.HashMap;
import java.util.Map;

import static sleeper.clients.util.ClientUtils.optionalArgument;

/**
 * Reads arguments from the command line when creating a query to generate a report from a job tracker.
 */
public class JobQueryArgument {

    private JobQueryArgument() {
    }

    private static final Map<String, JobQuery.Type> QUERY_TYPES = new HashMap<>();

    static {
        QUERY_TYPES.put("-a", JobQuery.Type.ALL);
        QUERY_TYPES.put("-d", JobQuery.Type.DETAILED);
        QUERY_TYPES.put("-r", JobQuery.Type.RANGE);
        QUERY_TYPES.put("-u", JobQuery.Type.UNFINISHED);
    }

    /**
     * Reads the type of job tracker query from a command line argument. Defaults to prompting from the command line if
     * the query type is not specified. There's a specific query type for that.
     *
     * @param  args  the command line arguments
     * @param  index the index of the query type argument
     * @return       the job tracker query type
     */
    public static JobQuery.Type readTypeArgument(String[] args, int index) {
        return optionalArgument(args, index)
                .map(JobQueryArgument::readType)
                .orElse(JobQuery.Type.PROMPT);
    }

    /**
     * Reads the type of job tracker query from a command line argument.
     *
     * @param  queryTypeStr the query type argument as specified on the command line
     * @return              the job tracker query type
     */
    public static JobQuery.Type readType(String queryTypeStr) {
        if (!QUERY_TYPES.containsKey(queryTypeStr)) {
            throw new IllegalArgumentException("Invalid query type " + queryTypeStr + ". Valid query types are -d (Detailed), -r (Range), -u (Unfinished)");
        }
        return QUERY_TYPES.get(queryTypeStr);
    }
}
