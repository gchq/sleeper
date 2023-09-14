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

package sleeper.systemtest.suite.testutil;

import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;

import sleeper.systemtest.drivers.util.TestContext;

public class TestContextFactory {

    private TestContextFactory() {
    }

    public static TestContext testContext(TestInfo info) {
        return TestContext.builder()
                .displayName(info.getDisplayName())
                .tags(info.getTags())
                .testClass(info.getTestClass().orElse(null))
                .testMethod(info.getTestMethod().orElse(null))
                .build();
    }

    public static TestContext testContext(ExtensionContext context) {
        return TestContext.builder()
                .displayName(context.getDisplayName())
                .tags(context.getTags())
                .testClass(context.getTestClass().orElse(null))
                .testMethod(context.getTestMethod().orElse(null))
                .build();
    }
}
