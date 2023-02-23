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

package sleeper.clients.admin.deploy;

import org.junit.jupiter.api.Test;

import sleeper.util.RunCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class CdkDeployInstanceTest {
    @Test
    void shouldRunCdkSuccessfully() throws IOException, InterruptedException {
        // Given
        CdkDeployInstance cdk = CdkDeployInstance.builder()
                .instancePropertiesFile(Path.of("instance.properties"))
                .cdkJarFile(Path.of("cdk.jar"))
                .cdkAppClassName("sleeper.cdk.SleeperCdkApp")
                .ensureNewInstance(false).build();

        // Then
        assertThat(commandRunOnDeployOf(cdk))
                .containsExactly("cdk",
                        "-a",
                        "java -cp \"cdk.jar\" sleeper.cdk.SleeperCdkApp",
                        "deploy",
                        "--require-approval",
                        "never",
                        "-c",
                        "propertiesfile=instance.properties",
                        "-c",
                        "newinstance=false",
                        "*");
    }

    private String[] commandRunOnDeployOf(CdkDeployInstance cdk) throws IOException, InterruptedException {
        AtomicReference<String[]> reference = new AtomicReference<>();
        RunCommand runCommand = (args) -> {
            reference.set(args);
            return 0;
        };
        cdk.deploy(runCommand);
        return reference.get();
    }
}
