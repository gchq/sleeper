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

package sleeper.clients.status.report;

import java.util.List;
import java.util.function.Function;

import static sleeper.clients.testutil.ClientTestUtils.exampleUUID;

public class StatusReporterTestHelper {
    private StatusReporterTestHelper() {
    }

    public static String job(int number) {
        return exampleUUID("job", number);
    }

    public static String task(int number) {
        return exampleUUID("task", number);
    }

    public static String replaceStandardJobIds(List<String> jobIds, String example) {
        return replaceJobIds(jobIds, StatusReporterTestHelper::job, example);
    }

    public static String replaceBracketedJobIds(List<String> jobIds, String example) {
        return replaceJobIds(jobIds, number -> "$(jobId" + number + ")", example);
    }

    protected static String replaceJobIds(
            List<String> jobIds, Function<Integer, String> getTemplateId, String example) {
        String replaced = example;
        for (int i = 0; i < jobIds.size(); i++) {
            replaced = replaced.replace(getTemplateId.apply(jobIds.size() - i), jobIds.get(i));
        }
        return replaced;
    }

}
