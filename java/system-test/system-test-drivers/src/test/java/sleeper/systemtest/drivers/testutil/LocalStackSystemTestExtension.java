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
package sleeper.systemtest.drivers.testutil;

import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;

import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.extension.SleeperSystemTestExtension;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.testutil.SystemTestParametersTestHelper;

public class LocalStackSystemTestExtension extends SleeperSystemTestExtension {

    private static final SystemTestDeploymentContext CONTEXT = createContext();

    private LocalStackSystemTestExtension() {
        super(CONTEXT);
    }

    private static SystemTestDeploymentContext createContext() {
        LocalStackSystemTestDrivers drivers = LocalStackSystemTestDrivers.fromContainer();
        SystemTestClients clients = drivers.clients();
        Region region = clients.getRegion();
        PartitionMetadata partitionMetadata = PartitionMetadata.of(region);
        return new SystemTestDeploymentContext(
                SystemTestParametersTestHelper.parametersBuilder()
                        .account(clients.getSts().getCallerIdentity().account())
                        .shortTestId("localstack")
                        .region(region.id())
                        .dnsSuffix(partitionMetadata.dnsSuffix())
                        .build(),
                drivers);
    }

}
