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

package sleeper.clients.admin;

import sleeper.clients.util.console.ConsoleInput;

public class JobStatusScreenHelper {
    private JobStatusScreenHelper() {
    }

    static String promptForJobId(ConsoleInput in) {
        return in.promptLine("Enter the ID of the job you want to view the details for: ");
    }

    static String promptForRange(ConsoleInput in) {
        String startRange = in.promptLine("Enter the start range in the format yyyyMMddhhmmss: ");
        String endRange = in.promptLine("Enter the end range in the format yyyyMMddhhmmss: ");
        return startRange + "," + endRange;
    }
}
