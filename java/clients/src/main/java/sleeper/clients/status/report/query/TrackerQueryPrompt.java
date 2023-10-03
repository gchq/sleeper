/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.clients.status.report.query;

import sleeper.clients.util.console.ConsoleInput;

public class TrackerQueryPrompt {
    private TrackerQueryPrompt() {
    }

    public static TrackerQuery from(ConsoleInput in) {
        String type = in.promptLine("Query types are:\n" +
                "-a (All queries)\n" +
                "-q (Queued queries)\n" +
                "-i (In progress queries)\n" +
                "-c (Completed queries)\n" +
                "-f (Failed queries)\n\n" +
                "Enter query type:");
        if ("".equals(type)) {
            return null;
        } else if (type.equalsIgnoreCase("a")) {
            return TrackerQuery.ALL;
        } else if (type.equalsIgnoreCase("q")) {
            return TrackerQuery.QUEUED;
        } else if (type.equalsIgnoreCase("i")) {
            return TrackerQuery.IN_PROGRESS;
        } else if (type.equalsIgnoreCase("c")) {
            return TrackerQuery.COMPLETED;
        } else if (type.equalsIgnoreCase("f")) {
            return TrackerQuery.FAILED;
        } else {
            return from(in);
        }
    }
}
