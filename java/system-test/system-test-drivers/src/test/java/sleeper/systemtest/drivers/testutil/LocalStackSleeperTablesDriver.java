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

import sleeper.clients.table.AddTableClient;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.statestore.StateStoreFactory;
import sleeper.systemtest.drivers.instance.AwsSleeperTablesDriver;
import sleeper.systemtest.drivers.util.SystemTestClients;

import java.io.IOException;
import java.io.UncheckedIOException;

public class LocalStackSleeperTablesDriver extends AwsSleeperTablesDriver {
    private final SystemTestClients clients;

    public LocalStackSleeperTablesDriver(SystemTestClients clients) {
        super(clients);
        this.clients = clients;
    }

    @Override
    public void addTable(InstanceProperties instanceProperties, TableProperties properties) {
        try {
            new AddTableClient(properties,
                    S3TableProperties.createStore(instanceProperties, clients.getS3(), clients.getDynamo()),
                    StateStoreFactory.createProvider(instanceProperties, clients.getS3(), clients.getDynamo()))
                    .run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
