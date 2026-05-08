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
package sleeper.cdk.artefacts;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;
import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;

import sleeper.cdk.testutil.StackPrinter;

public class SleeperArtefactRepositoriesIT {

    StackPrinter printer = StackPrinter.raw();

    @Test
    void shouldCreateRepositories() {
        // Given
        Stack stack = createRootStack("test-deployment");

        // When
        SleeperArtefactRepositories.Builder.create(stack, "test-deployment").build();

        // Then
        Approvals.verify(printer.toJson(stack), new Options()
                .forFile().withName("repositories", ".json"));
    }

    private Stack createRootStack(String deploymentId) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());
        Environment environment = Environment.builder()
                .account("test-account")
                .region("test-region")
                .build();
        StackProps stackProps = StackProps.builder()
                .stackName(deploymentId)
                .env(environment)
                .build();
        return new Stack(app, "TestArtefacts", stackProps);
    }
}
