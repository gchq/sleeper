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

package sleeper.systemtest.nightly.output;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

public class NightlyTestOutputTestHelper {
    private NightlyTestOutputTestHelper() {
    }

    public static NightlyTestOutput emptyOutput() {
        return new NightlyTestOutput(Collections.emptyList());
    }

    public static NightlyTestOutput outputWithStatusCodeByTest(Map<String, Integer> statusCodeByTest) {
        return new NightlyTestOutput(statusCodeByTest.entrySet().stream()
                .map(entry -> TestResult.builder().testName(entry.getKey()).exitCode(entry.getValue()).build())
                .sorted(Comparator.comparing(TestResult::getTestName))
                .collect(Collectors.toList()));
    }
}
