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

package sleeper.clients.report.query;

import sleeper.clients.util.console.ConsoleInput;

/**
 * Prompts the user on the command line to create a query to generate a report from a query tracker.
 */
public class TrackerQueryPrompt {
    private TrackerQueryPrompt() {
    }

    /**
     * Creates a tracker query by prompting the user. This can be used to generate a report from a query tracker.
     *
     * @param  in the console to prompt the user
     * @return    the query
     */
    public static TrackerQuery from(ConsoleInput in) {
        String type = in.promptLine("Query types are:\n" +
                "a (All queries)\n" +
                "q (Queued queries)\n" +
                "i (In progress queries)\n" +
                "c (Completed queries)\n" +
                "f (Failed queries)\n\n" +
                "Enter query type: ");
        if ("a".equalsIgnoreCase(type)) {
            return TrackerQuery.ALL;
        } else if ("q".equalsIgnoreCase(type)) {
            return TrackerQuery.QUEUED;
        } else if ("i".equalsIgnoreCase(type)) {
            return TrackerQuery.IN_PROGRESS;
        } else if ("c".equalsIgnoreCase(type)) {
            return TrackerQuery.COMPLETED;
        } else if ("f".equalsIgnoreCase(type)) {
            return TrackerQuery.FAILED;
        } else {
            return from(in);
        }
    }
}
