/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.systemtest.drivers.cdk;

import sleeper.clients.teardown.TearDownInstance;
import sleeper.core.properties.instance.InstanceProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;

public class TearDownTestInstance {

    private TearDownTestInstance() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Usage: <scripts directory> <optional instance id>");
        }
        builder().scriptsDir(Path.of(args[0]))
                .instanceId(optionalArgument(args, 1).orElse(null))
                .tearDownWithDefaultClients();
    }

    public static TearDownInstance.Builder builder() {
        return TearDownInstance.builder()
                .getExtraEcsClusters(properties -> getSystemTestEcsClusters(properties))
                .getExtraEcrRepositories(properties -> getSystemTestEcrRepositories(properties));
    }

    public static List<String> getSystemTestEcsClusters(InstanceProperties properties) {
        return Optional.ofNullable(properties.get(SYSTEM_TEST_CLUSTER_NAME))
                .stream().collect(Collectors.toUnmodifiableList());
    }

    public static List<String> getSystemTestEcrRepositories(InstanceProperties properties) {
        return List.of(Optional.ofNullable(properties.get(SYSTEM_TEST_REPO))
                .orElseGet(() -> properties.get(ID) + "/system-test"));
    }
}
