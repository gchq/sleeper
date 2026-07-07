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
package sleeper.clients.deploy;

import org.junit.jupiter.api.Test;

import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.container.images.ContainerRegistryCredentials;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArguments;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SetDeployConfigurationTest {

    private final ToStringConsoleOutput out = new ToStringConsoleOutput();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    private final Map<String, String> pathToFileContent = new HashMap<>();

    @Test
    void shouldConfigureToBuildImagesLocally() {
        // When
        setDeployConfig("--image-location", "LOCAL_BUILD");

        // Then
        assertThat(getDeployConfig()).isEqualTo(DeployConfiguration.fromLocalBuild());
    }

    @Test
    void shouldConfigureToRetrieveRemoteImages() {
        // When
        setDeployConfig("--image-repository-prefix", "ghcr.io/gchq");

        // Then
        assertThat(getDeployConfig()).isEqualTo(DeployConfiguration.fromDockerRepository("ghcr.io/gchq"));
    }

    @Test
    void shouldConfigureCredentialsToRetrieveRemoteImages() {
        // When
        in.enterNextPrompt("my-password");
        setDeployConfig("--image-repository-prefix", "ghcr.io/gchq", "--image-username", "my-user");

        // Then
        assertThat(getDeployConfig()).isEqualTo(DeployConfiguration.fromDockerRepository(
                "ghcr.io/gchq", new ContainerRegistryCredentials("my-user", "my-password")));
    }

    @Test
    void shouldOverrideBaseImage() {
        // When
        setDeployConfig("--image-location", "LOCAL_BUILD", "--override-base-image-dir", "./custom/base");

        // Then
        assertThat(getDeployConfig()).isEqualTo(DeployConfiguration.fromLocalBuild()
                .withOverrideBaseImageDir("./custom/base"));
    }

    @Test
    void shouldOverrideBaseForSpecificImage() {
        // When
        setDeployConfig("--image-location", "LOCAL_BUILD",
                "--override-base-image-dir-by-image", "bulk-import-runner,./custom/spark-base");

        // Then
        assertThat(getDeployConfig()).isEqualTo(DeployConfiguration.fromLocalBuild()
                .withImageToOverrideBaseDir(Map.of("bulk-import-runner", "./custom/spark-base")));
    }

    private DeployConfiguration getDeployConfig() {
        String json = pathToFileContent.get("./scripts/templates/deployConfig.json");
        return new DeployConfigurationSerDe().fromJson(json);
    }

    private void setDeployConfig(String... args) {
        List<String> allArgs = new ArrayList<>();
        allArgs.add("./scripts");
        allArgs.addAll(List.of(args));
        CommandArguments arguments = CommandArgumentReader.parse(SetDeployConfiguration.USAGE, allArgs.toArray(String[]::new));
        try {
            SetDeployConfiguration.writeConfigurationFile(
                    (path, string) -> pathToFileContent.put(path.toString(), string),
                    SetDeployConfiguration.readArguments(in.consoleIn(), arguments));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
